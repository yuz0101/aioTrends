# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import pickle
import random
import sys
import time
from datetime import datetime, date, timedelta
from pandas import DataFrame, to_datetime

import aiohttp
import aiofiles
import aiofiles.os
import numpy as np

from aioTrends.Settings import Settings
from aioTrends.processData import formQueries, collectRawData, delAllFiles, scaledDailyData
from aioTrends.HandleExceptions import InputError
import colorama
colorama.init()


#if 'win' in sys.platform:
    #policy = asyncio.WindowsSelectorEventLoopPolicy()
    #asyncio.set_event_loop_policy(policy)

def freqReso(freq):
    '''The function `freqReso` takes a frequency input and returns the corresponding time resolution.
    
    Parameters
    ----------
    freq
        The freq parameter is a string that represents the frequency of a certain event or action.
    
    Returns
    -------
        The function `freqReso` returns the frequency resolution based on the input `freq`. The possible
    return values are 'DAY', 'MONTH', 'WEEK', or 'YEAR'.
    
    '''
    if freq.lower() in ['d', 'day', 'daily']:
        return 'DAY'
    elif freq.lower() in ['m', 'month', 'monthly']:
        return 'MONTH'
    elif freq.lower() in ['w', 'week', 'weekly']:
        return 'WEEK'
    elif freq.lower() in ['y', 'year', 'yearly', 'annual', 'a']:
        return 'YEAR'

def readDatakeys(path):
    '''The function `readDatakeys` reads the keys from a given path by removing the file extension and
    converting them to integers.
    
    Parameters
    ----------
    path
        The path parameter is the directory path where the data files are located.
    
    Returns
    -------
        The function `readDatakeys` returns a list of integers.
    
    '''
    paths = os.listdir(path)
    ps = []
    for p in paths:
        ps.append(int(p.replace('.pkl', '')))
    return ps

async def readJson(path: str):
    '''The function reads a JSON file asynchronously and returns its contents as a Python dictionary.
    
    Parameters
    ----------
    path : str
        The `path` parameter is a string that represents the file path of the JSON file that you want to
    read.
    
    Returns
    -------
        a JSON object.
    
    '''
    async with aiofiles.open(path, 'r') as f:
        s = await f.read()
        s = json.loads(s)
    return s

async def readProxies(path: str)->list:
    '''The function `readProxies` reads a file containing proxy information and returns a list of formatted
    proxy URLs.
    
    Parameters
    ----------
    path : str
        The `path` parameter is a string that represents the file path of the file containing the proxies.
    
    Returns
    -------
        The function `readProxies` returns a list of formatted proxy URLs.
    
    '''
    async with aiofiles.open(path, 'r') as f:
        f = await f.read()
        f = f.split('\n')
    f = [x.split(':') for x in f]
    return [f'http://{x[2]}:{x[3]}@{x[0]}:{x[1]}' for x in f]

async def pkl(path: str, obj: any, lock)->None:
    '''The function `pkl` asynchronously writes a pickled object to a file, using a lock to ensure thread
    safety.
    
    Parameters
    ----------
    path : str
        The `path` parameter is a string that represents the file path where the object will be pickled and
    saved.
    obj : any
        The `obj` parameter is the object that you want to pickle and save to a file.
    lock
        The `lock` parameter is an asyncio lock object that is used to ensure that only one coroutine can
    access the file at a time. It is used to prevent concurrent writes to the file, which could lead to
    data corruption.
    
    '''
    async with lock:
        async with aiofiles.open(path, 'wb') as f:
            p = pickle.dumps(obj)
            await f.write(p)

async def unpkl(path: str, lock)->any:
    '''The function `unpkl` reads a pickled object from a file asynchronously, using a lock to ensure
    thread safety.
    
    Parameters
    ----------
    path : str
        The `path` parameter is a string that represents the file path of the file you want to unpickle.
    lock
        The `lock` parameter is a lock object that is used to synchronize access to the file. It ensures
    that only one coroutine can access the file at a time, preventing any potential race conditions.
    
    Returns
    -------
        the deserialized object that was loaded from the file.
    
    '''
    async with lock:
        async with aiofiles.open(path, 'rb') as f:
            p = await f.read()
        p = pickle.loads(p)
    return p

async def updateList(path:str, a: any, lock)->None:
    '''The function `updateList` asynchronously updates a list stored in a file by appending a new element
    to it.
    
    Parameters
    ----------
    path : str
        The `path` parameter is a string that represents the file path where the list will be stored or
    updated.
    a : any
        The parameter `a` can be any object or value that you want to append to the list stored in the file
    at the given `path`.
    lock
        The `lock` parameter is a lock object that is used to synchronize access to the file. It ensures
    that only one coroutine can access the file at a time, preventing any potential race conditions or
    conflicts when multiple coroutines try to update the file simultaneously.
    
    '''
    async with lock:
        if await aiofiles.os.path.exists(path):
            async with aiofiles.open(path, mode='rb') as f:
                p = await f.read()
            p = pickle.loads(p)
        else:
            p = []
        p.append(a)
        async with aiofiles.open(path, mode='wb') as f:
            p = pickle.dumps(p)
            await f.write(p)    

async def updateDict(path: str, d: dict, lock)->None:
    '''The function `updateDict` updates a dictionary `d` and saves it to a file specified by `path` using
    a lock to ensure thread safety.
    
    Parameters
    ----------
    path : str
        The `path` parameter is a string that represents the file path where the dictionary will be stored
    or updated.
    d : dict
        The parameter `d` is a dictionary that contains the data to be updated in the file.
    lock
        The `lock` parameter is a lock object that is used to synchronize access to the shared resource (in
    this case, the file). It ensures that only one coroutine can access the file at a time, preventing
    any potential race conditions or conflicts.
    
    '''
    async with lock:
        if await aiofiles.os.path.exists(path):
            async with aiofiles.open(path, mode='rb') as f:
                p = await f.read()
            p = pickle.loads(p)
        else:
            p = {}
        p.update(d)
        async with aiofiles.open(path, mode='wb') as f:
            p = pickle.dumps(p)
            await f.write(p)

async def popDict(path: str, key, lock)-> None:
    '''The function `popDict` asynchronously removes a key from a dictionary stored in a file.
    
    Parameters
    ----------
    path : str
        The `path` parameter is a string that represents the file path where the dictionary is stored. This
    is the file that will be read from and written to.
    key
        The `key` parameter is the key of the dictionary element that you want to remove.
    lock
        The `lock` parameter is a lock object that is used to synchronize access to the file. It ensures
    that only one coroutine can access the file at a time, preventing concurrent writes that could lead
    to data corruption.
    
    '''
    async with lock:
        if await aiofiles.os.path.exists(path):
            async with aiofiles.open(path, 'rb') as f:
                p = await f.read()
                s = pickle.loads(p)
            s.pop(key)
            async with aiofiles.open(path, 'wb') as f:
                p = pickle.dumps(s)
                await f.write(p)

async def readCookies(path: str, lock)->dict:
    '''The function `readCookies` reads a pickled file at the specified path, acquires a lock, and returns
    a randomly chosen element from the unpickled data.
    
    Parameters
    ----------
    path : str
        The `path` parameter is a string that represents the file path where the cookies are stored.
    lock
        The "lock" parameter is a lock object that is used to synchronize access to the file being read. It
    ensures that only one coroutine can access the file at a time, preventing any potential race
    conditions.
    
    Returns
    -------
        a randomly chosen element from the dictionary `s`.
    
    '''
    s = await unpkl(path, lock)
    return random.choice(s)

class CookiesPool(Settings):
    def __init__(self, workerAmount: int):
        '''The above function is a constructor that initializes an object with a workerAmount attribute and a
        queueCookies attribute.
        
        Parameters
        ----------
        workerAmount : int
            The `workerAmount` parameter is an integer that represents the number of workers or threads that
        will be used in the program.
        
        '''
        super().__init__()
        self.workerAmount = workerAmount
        self.queueCookies = asyncio.Queue()
    
    async def getCookies(self, i: str or int, userAgent: str, proxy: str)->None:
        '''The `getCookies` function is an asynchronous function that retrieves cookies from a website using a
        given user agent and proxy.
        
        Parameters
        ----------
        i : str or int
            The parameter `i` is a string or integer that represents the index or identifier of the current
        request being made. It is used for logging purposes to track the progress of the `getCookies`
        function.
        userAgent : str
            The `userAgent` parameter is a string that represents the user agent header to be used in the HTTP
        request. The user agent header identifies the client making the request, typically a web browser or
        a web scraping tool.
        proxy : str
            The `proxy` parameter is a string, formatted as 
            f"http://{host}:{port}" or 
            f"http://{username}:{password}@{host}:{port}"..
        
        '''
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    f'https://trends.google.com/?geo={self.geo}', proxy=proxy, timeout=self.timeout
                    ) as res:
                    if await self.status(f'|{i}|Cookies Pool', res):
                        cookies = dict(filter(lambda i: i[0]=='NID', res.cookies.items()))
                        await updateList(self.pathCookies, cookies, self.lock)
                        self.cookiesNumb += 1
                        logging.critical(f'|{i}|Cookies Pool|Amount:{self.cookiesNumb}| Got')
                    else:
                        logging.error(f'|{i}|Cookies Pool|Amount:{self.cookiesNumb}| failed')
        except Exception as e:
            logging.error(f'|{i}|Cookies Pool|Amount:{self.cookiesNumb}|Unknown Error| {repr(e)}')

    async def worker(self, i:str or int)->None:
        '''The `worker` function is an asynchronous function that continuously retrieves cookies from a queue
        and uses them to make requests with a random user agent and proxy.
        
        Parameters
        ----------
        i : str or int
            The parameter `i` in the `worker` function can be either a string or an integer. It is used as an
        input to the `getCookies` method.
        
        '''
        while True:
            _ = await self.queueCookies.get()
            userAgent = random.choice(self.userAgents)
            proxy = random.choice(self.proxies)
            await self.getCookies(i, userAgent, proxy)
            self.queueCookies.task_done()
    
    async def filterCookies(self, cookiesQrys: int):
        '''The function "filterCookies" calculates the difference between the number of cookies specified in
        the "cookiesQrys" parameter and the number of cookies stored in a file.
        
        Parameters
        ----------
        cookiesQrys : int
            The parameter `cookiesQrys` represents the total number of queries for cookies.
        
        '''
        if os.path.exists(self.pathCookies):
            self.cookiesNumb = len(await unpkl(self.pathCookies, self.lock))
        else:
            self.cookiesNumb = 0
        self.cookiesQrys = cookiesQrys - self.cookiesNumb

    async def asyncGetCookies(self, cookiesQrys: int)->None:
        '''The `asyncGetCookies` function reads user agents and proxies, filters cookies, and then assigns
        tasks to workers to process the cookies.
        
        Parameters
        ----------
        cookiesQrys : int
            The parameter `cookiesQrys` represents the number of cookie queries to be processed.
        
        '''
        self.userAgents = await readJson(self.pathUserAgents)
        self.proxies = await readProxies(self.pathProxies)
        await self.filterCookies(cookiesQrys)
        while self.cookiesQrys > 0:
            if self.cookiesQrys < self.workerAmount:
                self.workerAmount = self.cookiesQrys
            for cid in range(self.cookiesQrys):
                self.queueCookies.put_nowait(cid)
            tasks = []
            for i in range(self.workerAmount):
                task = asyncio.create_task(self.worker(i))
                tasks.append(task)
            await self.queueCookies.join()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.filterCookies(cookiesQrys)

    def run(self, cookiesQrys: int):
        '''The `run` function starts the program for getting cookies by running the `asyncGetCookies` function
        with the target amount of cookies as an argument.
        
        Parameters
        ----------
        cookiesQrys : int
            The `cookiesQrys` parameter is an integer that represents the target amount of cookies that the
        program is trying to obtain.
        
        '''
        self.lock = asyncio.Lock()
        asyncio.run(self.asyncGetCookies(cookiesQrys))

class WidgetsPool(Settings):

    def __init__(self, workerAmount: int):
        '''The above function is a constructor that initializes the workerAmount and queueWgt variables.
        
        Parameters
        ----------
        workerAmount : int
            The `workerAmount` parameter is an integer that represents the number of workers or threads that
        will be used in the program.
        
        '''
        super().__init__()
        self.workerAmount = workerAmount
        self.queueWgt = asyncio.Queue()
    
    async def widgetInterestOverTime(self, i: int, tid, cookies: dict, proxy: str, userAgent: str)->None:
        '''The function `widgetInterestOverTime` is an asynchronous Python function that sends a request to a
        Google Trends API to retrieve widget data and stores it in a dictionary.
        
        Parameters
        ----------
        i : int
            The parameter `i` is an integer that represents the index or identifier of the current widget
        interest query. It is used for logging purposes.
        tid
            The `tid` parameter in the `widgetInterestOverTime` function is used to identify a specific query
        or request for the Google Trends API. It stands for "Trend ID" and is used to retrieve the interest
        over time data for a particular keyword or set of keywords.
        cookies : dict
            The `cookies` parameter is a dictionary that contains any cookies that need to be sent with the
        request. Cookies are typically used for session management or to store user preferences.
        proxy : str
            The `proxy` parameter is a string, formatted as 
            f"http://{host}:{port}" or 
            f"http://{username}:{password}@{host}:{port}".
        userAgent : str
            The `userAgent` parameter is a string that represents the user agent of the HTTP request. It is
        used to identify the client making the request to the server.
        
        '''
        payload = {
            'hl': self.hl, 
            'tz': self.tz,
            'req': {
                'comparisonItem': [
                    {'keyword': kw, 'time': self.qrys[tid]['periods'], 'geo': self.geo} for kw in self.qrys[tid]['keywords']
                    ],
                'category': self.cat,
                'property': self.gprop
                }
            }
        payload['req'] = json.dumps(payload['req'])
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    self.urls['token'], params=payload, cookies=cookies, proxy=proxy, timeout=self.timeout
                    ) as res:
                    if await self.status(f'|{i}|Widget Pool|TID:{tid}|Remained:{self.wgtQryN}', res):
                        res = await res.text()
                        res = json.loads(res[4:])
                        widgets = res['widgets']
                        widget = list(filter(lambda w: w['id']=='TIMESERIES', widgets))[0]
                        if widget['request']['resolution'] == freqReso(self.qrys[tid]['freq']):
                            await updateDict(self.pathWgt, {tid: widget}, self.lock)
                            self.wgtQryN -= 1
                            logging.critical(f'|{i}|Widget Pool|TID:{tid}|Remained:{self.wgtQryN}| Stored')
                        else:
                            logging.error(f'|{i}|Widget Pool|TID:{tid}|Remained:{self.wgtQryN}|Got Wrong Resolution|Re-try nextly')
        except Exception as e:
            logging.error(f'|{i}|Widget Pool|TID:{tid}|Remained:{self.wgtQryN}|Unknown Error| {repr(e)}')

    async def worker(self, i:int)->None:
        '''The `worker` function is an asynchronous function that continuously retrieves tasks from a queue and
        performs a specific task using cookies, proxy, and user agent.
        
        Parameters
        ----------
        i : int
            The parameter `i` is an integer that represents the index of the worker. It is used to identify the
        worker and can be used for any specific logic or tracking purposes within the `worker` function.
        
        '''
        while True:
            tid = await self.queueWgt.get()
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            cookies = await readCookies(self.pathCookies, self.lock)
            await self.widgetInterestOverTime(i, tid, cookies, proxy, userAgent)
            self.queueWgt.task_done()

    async def filterQrys(self)->list:
        '''The function `filterQrys` returns a list of keys from a dictionary, after filtering out keys that
        are present in two other dictionaries.
        
        Returns
        -------
            a list of keys after filtering out certain keys.
        
        '''
        k = list(self.qrys.keys())
        try:
            dkeys = await unpkl(self.pathWgt, self.lock)
            k = list(set(k)-set(dkeys))
        except:
            pass
        try:
            dkeys = await unpkl(self.pathWgtEptyRes, self.lock)
            k = list(set(k)-set(dkeys))
        except:
            pass
        return k

    async def asyncGetWidget(self, qrys)->None:
        '''The `asyncGetWidget` function reads proxies, user agents, and queries, filters the queries, and then
        assigns tasks to worker functions to process the queries asynchronously.
        
        Parameters
        ----------
        qrys
            The `qrys` parameter is a list of queries. It is used to filter the queries before processing them.
        If `qrys` is `None`, then the queries are read from a file specified by `self.pathQrys`. Otherwise,
        the provided `qrys` list is used.
        
        '''
        self.proxies = await readProxies(self.pathProxies)
        self.userAgents = await readJson(self.pathUserAgents)
        if qrys == None:
            self.qrys = await unpkl(self.pathQrys, self.lock)
        else:
            self.qrys = qrys
        k = await self.filterQrys()
        self.wgtQryN = len(k)
        random.shuffle(k)
        if self.wgtQryN < self.workerAmount:
            self.workerAmount = self.wgtQryN

        while self.wgtQryN > 0:
            if self.wgtQryN < self.workerAmount:
                self.workerAmount = self.wgtQryN
            for tid in k:
                self.queueWgt.put_nowait(tid)
            tasks = []
            for i in range(self.workerAmount):
                task = asyncio.create_task(self.worker(i))
                tasks.append(task)
            await self.queueWgt.join()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            k = await self.filterQrys()
            self.wgtQryN = len(k)
            random.shuffle(k)

    def run(self, qrys=None)->None:
        '''The `run` function is a method that takes an optional parameter `qrys` and runs the `asyncGetWidget`
        function using asyncio.
        
        Parameters
        ----------
        qrys
            The `qrys` parameter is an optional argument that represents a list of queries. It is used as an
        input to the `asyncGetWidget` method. If no queries are provided, the `qrys` parameter will default
        to `None`.
        
        '''
        self.lock = asyncio.Lock()
        asyncio.run(self.asyncGetWidget(qrys))

class DataInterestOverTime(Settings):
    def __init__(self, workerAmount: int):
        """This is a init function of the class DataInterestOverTime.

        Args:
            workerAmount (int): The amount of workers to run tasks cocurrently.
        """
        super().__init__()
        self.workerAmount = workerAmount
        self.queueData = asyncio.Queue()

    async def getData(
        self, i:int, tid: str or int, userAgent: str, proxy: str,
        )->None:
        '''The `getData` function is an asynchronous function that retrieves data from a specified URL using
        the `aiohttp` library and stores the data in a file.
        
        Parameters
        ----------
        i : int
            Worker id.
        tid : str or int
            The `tid` parameter stands for "Task ID" and it can be either a string or an integer. It is used to
        identify a specific task or request.
        userAgent : str
            The `userAgent` parameter is a string that represents the user agent header to be used in the HTTP
        request. The user agent header identifies the client making the request, typically a web browser or
        a web scraping tool. It helps the server understand how to handle the request and respond
        accordingly.
        proxy : str
            The `proxy` parameter is a string, formatted as 
            f"http://{host}:{port}" or 
            f"http://{username}:{password}@{host}:{port}".
        
        '''
        payload = {
            'req': json.dumps(self.widgets[tid]['request']),
            'token': self.widgets[tid]['token'],
            'tz': self.tz
        }
        kws = ','.join([i['complexKeywordsRestriction']['keyword'][0]['value'] for i in self.widgets[tid]['request']['comparisonItem']])
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    self.urls['multiline'], params=payload, proxy=proxy, timeout=self.timeout
                ) as res:
                    if await self.status(f'|{i}|Data|Remained:{self.qryN}|TID:{tid}', res):
                        res = await res.text()
                        res = json.loads(res[5:])['default']['timelineData']
                        self.qryN -= 1 
                        await popDict(self.pathWgt, tid, self.lock)
                        if len(res) > 0:
                            #await updateDict(self.pathDataIOT, {int(tid): res}, self.lock)
                            res = {'keywords': kws, 'freq': self.widgets[tid]['request']['resolution'], 'data': res}
                            await pkl(f'{self.pathDataIOT}{tid}.pkl', res, self.lock)
                            logging.critical(f'|{i}|Data|Remained:{self.qryN}|TID:{tid}| Stored **')
                        else:
                            await updateDict(self.pathWgtEptyRes, {int(tid): self.widgets[tid]}, self.lock)
                            logging.error(f'|{i}|Data|Remained:{self.qryN}|TID:{tid}| Empty Set')
        except Exception as e:
            logging.error(f'|{i}|Data|Remained:{self.qryN}|TID:{tid}|Unknown Error| {repr(e)}')

    async def worker(self, i: int)->None:
        '''The `worker` function is an asynchronous function that continuously retrieves tasks from a queue,
        selects a random user agent and proxy, and then calls the `getData` function with the provided
        arguments.
        
        Parameters
        ----------
        i : int
            The parameter `i` is an integer that represents the worker's identifier or index. It is used to
        differentiate between multiple worker instances if there are more than one.
        
        '''
        while True:
            tid = await self.queueData.get()
            userAgent = random.choice(self.userAgents)
            proxy = random.choice(self.proxies)
            #cookies = await readCookies(self.pathCookies, self.lock)
            await self.getData(i, tid, userAgent, proxy)
            self.queueData.task_done()

    async def filterWidgets(self)->list:
        """This is a function of loading and filtering 
            (1) the finished tasks and 
            (2) the tasks with empty responses from google.
        Returns:
            list: The key value of widgets that remained to be conducted.
        """
        k = list(self.widgets.keys())
        try:
            #dkeys = await unpkl(self.pathDataIOT, self.lock)
            dkeys = readDatakeys(self.pathDataIOT)
            k = list(set(k)-set(dkeys))
        except:
            pass
        try:
            dkeys = await unpkl(self.pathWgtEptyRes, self.lock)
            k = list(set(k)-set(dkeys))
        except:
            pass
        return k
    
    async def asynGetData(self)->None:
        '''The `asynGetData` function reads user agents, proxies, and widgets asynchronously, filters the
        widgets, and then processes them using multiple workers.
        
        '''
        self.userAgents = await readJson(self.pathUserAgents)
        self.proxies = await readProxies(self.pathProxies)
        self.widgets = await unpkl(self.pathWgt, self.lock)
        k = await self.filterWidgets()
        self.qryN = len(k)
        random.shuffle(k)
        while self.qryN > 0:
            if self.qryN < self.workerAmount:
                self.workerAmount = self.qryN
            for tid in k:
                self.queueData.put_nowait(tid)
            tasks = []
            for i in range(self.workerAmount):
                task = asyncio.create_task(self.worker(i))
                tasks.append(task)
            await self.queueData.join()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            k = await self.filterWidgets()
            self.qryN = len(k)
            random.shuffle(k)

    def run(self) -> None:
        '''The function `run` initializes a lock and runs an asynchronous function `asynGetData`.
        
        '''
        self.lock = asyncio.Lock()
        asyncio.run(self.asynGetData())

class Aio(Settings):
    def __init__(self, workerAmount: int):
        '''The above code defines a class with an initializer that takes an integer argument and assigns it to
        an instance variable.
        
        Parameters
        ----------
        workerAmount : int
            The `workerAmount` parameter is an integer that represents the number of workers. It is used to
        initialize the `workerAmount` attribute of the class.
        
        '''
        super().__init__()
        self.workerAmount = workerAmount

    def initFiles(self):
        '''The `initFiles` function removes specific files and calls another function to delete all files in a
        given directory.
        
        '''
        try:
            os.remove(self.pathCookies)
        except:
            pass
        try:
            os.remove(self.pathWgt)
        except:
            pass
        try:
            os.remove(self.pathWgtEptyRes)
        except:
            pass
        delAllFiles(self.pathDataIOT)

    def _oneStep(self, qrys):
        '''The function `_oneStep` performs three steps: processing cookies, processing widgets, and processing
        data.
        
        Parameters
        ----------
        qrys
            The parameter "qrys" is a list of queries. It is used as an input for the "WidgetsPool" class,
        which runs a pool of workers to process the queries and generate widgets.
        
        '''
        # 1 cookies
        if len(qrys) <= 3:
            ckis = 1
        else:
            ckis = int(len(qrys)/3)
        CookiesPool(self.workerAmount).run(ckis)
        # 2 widgets
        WidgetsPool(self.workerAmount).run(qrys=qrys)
        # 3 data
        DataInterestOverTime(self.workerAmount).run()

    def _getRawDailyData(self, keywords, start, end):
        '''The function `_getRawDailyData` takes in keywords, start and end dates, forms queries, performs a
        step, and returns collected raw data.
        
        Parameters
        ----------
        keywords
            The `keywords` parameter is a list of keywords or search terms that you want to use for querying
        the data.
        start
            The start parameter is the start date for collecting the daily data.
        end
            The "end" parameter is the end date for the data collection. It specifies the last date for which
        you want to collect data.
        
        Returns
        -------
            the result of the `collectRawData` function, which is being passed the `self.pathDataIOT` argument.
        
        '''
        qrys = formQueries(keywords, start, end, freq='D')
        self._oneStep(qrys)
        return collectRawData(self.pathDataIOT)

    def _getRawMonthlyData(self, keywords, start, end):
        '''The function `_getRawMonthlyData` takes in keywords, start and end dates, forms queries, performs a
        step, and returns collected raw data.
        
        Parameters
        ----------
        keywords
            The `keywords` parameter is a list of strings that represent the keywords or search terms for which
        you want to collect data.
        start
            The start parameter is the starting date for the data collection. It specifies the date from which
        the data collection should begin.
        end
            The "end" parameter is the end date for the data collection. It specifies the last date for which
        data should be collected.
        
        Returns
        -------
            the result of the `collectRawData` function, which is being passed the `self.pathDataIOT` argument.
        
        '''
        qrys = formQueries(keywords, start, end, freq='M')
        self._oneStep(qrys)
        return collectRawData(self.pathDataIOT)
    
    def getMonthlyData(self, keywords: list, start: str or date or datetime='2004-01-01', end: str or date or datetime=date.today()):
        '''The function `getMonthlyData` retrieves monthly data for a given list of keywords within a specified
        time range and returns it as a DataFrame.
        
        Parameters
        ----------
        keywords : list
            A list of keywords for which you want to retrieve monthly data.
        start : str or date or datetime, optional
            The start parameter is the starting date for the data retrieval. It can be specified as a string in
        the format 'YYYY-MM-DD', or as a date or datetime object. If no start date is provided, it defaults
        to '2004-01-01'.
        end : str or date or datetime
            The "end" parameter is the end date for the monthly data. It can be specified as a string in the
        format 'YYYY-MM-DD', or as a date object, or as a datetime object. If no end date is provided, it
        defaults to the current date.
        
        Returns
        -------
            a DataFrame that contains the monthly data for the specified keywords.
        
        '''
        _d = self._getRawMonthlyData(keywords, start, end)
        _dm = {}
        for key in _d.keys():
            _dm[key] = _d[key][0]
        _dm = DataFrame(_dm)
        _dm.index = to_datetime(_dm.index, unit='s')
        return _dm

    def getScaledDailyData(self, keywords: list, filename: str, start: str or date or datetime='2004-01-01', end: str or date or datetime=date.today())->DataFrame:
        '''The function `getScaledDailyData` takes a list of keywords, a filename, a start date, and an end
        date as input, retrieves raw daily and monthly data, scales the daily data using the monthly data,
        and saves the scaled data in the specified file format.
        
        Parameters
        ----------
        keywords : list
            A list of keywords or search terms. i.e., ['AMZN', 'MSFT', 'AAPL']
        filename : str
            The `filename` parameter is a string that represents the name of the file where the scaled daily
        data will be saved.
        start : str or date or datetime, optional
            The `start` parameter is the starting date for the data. It can be specified as a string in the
        format 'YYYY-MM-DD', or as a `date` or `datetime` object. If no value is provided, the default value
        is '2004-01-01'.
        end : str or date or datetime
            The `end` parameter is the end date for the data. It can be specified as a string in the format
        'YYYY-MM-DD', or as a `date` or `datetime` object. If no value is provided, it defaults to the
        current date.
        
        Returns
        -------
            a DataFrame.
        
        '''

        _filename, fileExt = filename.split('.')
        _filename = _filename + '.pkl'

        if os.path.exists(os.path.join(self.pathScaledDaily, filename)):
            raise InputError(
                f"File existed under the folder {self.pathScaledDaily}"
            )
        
        #if not os.path.exists(os.path.join(self.pathRawDaily, _filename)):
        self.initFiles()
        daily = self._getRawDailyData(keywords, start, end)
        with open(os.path.join(self.pathRawDaily, _filename), 'wb') as f:
            pickle.dump(daily, f)
        #else:
        daily = pickle.load(open(os.path.join(self.pathRawDaily, _filename), 'rb'))           
        
        #if not os.path.exists(os.path.join(self.pathRawMonthly, _filename)):
        self.initFiles()
        monthly = self._getRawMonthlyData(keywords, start, end)
        with open(os.path.join(self.pathRawMonthly, _filename), 'wb') as f:
            pickle.dump(monthly, f)
        #else:
            #monthly = pickle.load(open(os.path.join(self.pathRawMonthly, _filename), 'rb'))      

        samples = scaledDailyData(daily, monthly)

        if fileExt == 'json':
            with open(os.path.join(self.pathScaledDaily, filename), 'w') as f:
                json.dump(json.dumps(samples.to_dict()), f)
        samples = DataFrame(samples)
        samples.index = (to_datetime(samples.index, unit='s')).rename('date')
        samples.sort_index(inplace=True)
        if fileExt == 'pkl':
            samples.to_pickle(os.path.join(self.pathScaledDaily, filename))
        elif fileExt == 'csv':
            samples.to_csv(os.path.join(self.pathScaledDaily, filename))
        return samples