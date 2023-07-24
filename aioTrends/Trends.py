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



if 'win' in sys.platform:
    import colorama
    colorama.init()
    #policy = asyncio.WindowsSelectorEventLoopPolicy()
    #asyncio.set_event_loop_policy(policy)

def freqReso(freq):
    if freq.lower() in ['d', 'day', 'daily']:
        return 'DAY'
    elif freq.lower() in ['m', 'month', 'monthly']:
        return 'MONTH'
    elif freq.lower() in ['w', 'week', 'weekly']:
        return 'WEEK'
    elif freq.lower() in ['y', 'year', 'yearly', 'annual', 'a']:
        return 'YEAR'

def readDatakeys(path):
    paths = os.listdir(path)
    ps = []
    for p in paths:
        ps.append(int(p.replace('.pkl', '')))
    return ps

async def readJson(path: str):
    async with aiofiles.open(path, 'r') as f:
        s = await f.read()
        s = json.loads(s)
    return s

async def readProxies(path: str)->list:
    async with aiofiles.open(path, 'r') as f:
        f = await f.read()
        f = f.split('\n')
    f = [x.split(':') for x in f]
    return [f'http://{x[2]}:{x[3]}@{x[0]}:{x[1]}' for x in f]

async def pkl(path: str, obj: any, lock)->None:
    async with lock:
        async with aiofiles.open(path, 'wb') as f:
            p = pickle.dumps(obj)
            await f.write(p)

async def unpkl(path: str, lock)->any:
    async with lock:
        async with aiofiles.open(path, 'rb') as f:
            p = await f.read()
        p = pickle.loads(p)
    return p

async def updateList(path:str, a: any, lock)->None:
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
    s = await unpkl(path, lock)
    return random.choice(s)

class CookiesPool(Settings):
    def __init__(self, workerAmount: int):
        super().__init__()
        self.workerAmount = workerAmount
        self.queueCookies = asyncio.Queue()
    
    async def getCookies(self, i: str or int, userAgent: str, proxy: str)->None:
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
        while True:
            _ = await self.queueCookies.get()
            userAgent = random.choice(self.userAgents)
            proxy = random.choice(self.proxies)
            await self.getCookies(i, userAgent, proxy)
            self.queueCookies.task_done()
    
    async def filterCookies(self, cookiesQrys: int):
        if os.path.exists(self.pathCookies):
            self.cookiesNumb = len(await unpkl(self.pathCookies, self.lock))
        else:
            self.cookiesNumb = 0
        self.cookiesQrys = cookiesQrys - self.cookiesNumb

    async def asyncGetCookies(self, cookiesQrys: int)->None:
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
        """This is a function of starting the programme for getting cookies

        Args:
            cookiesQrys (int): the target amount of cookies
        """
        self.lock = asyncio.Lock()
        asyncio.run(self.asyncGetCookies(cookiesQrys))

class WidgetsPool(Settings):

    def __init__(self, workerAmount: int):
        super().__init__()
        self.workerAmount = workerAmount
        self.queueWgt = asyncio.Queue()
    
    async def widgetInterestOverTime(
        self, i: int, tid, cookies: dict, proxy: str, userAgent: str
        )->None:
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
        while True:
            tid = await self.queueWgt.get()
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            cookies = await readCookies(self.pathCookies, self.lock)
            await self.widgetInterestOverTime(i, tid, cookies, proxy, userAgent)
            self.queueWgt.task_done()

    async def filterQrys(self)->list:
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
        """This is a function of starting the programme

        Args:
            qrys(dict)
        """
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
        """This is the main function of getting data of interest over time.

        Args:
            i (int): Worker id.
            tid (str or int): Task id.
            userAgent (str): The user agent loaded on headers for GET.
            proxy (str): A proxy in a format of f"http://{username}:{password}@{ip}:{port}" or f"http://{ip}:{port}".
        """
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
        """This is a function of starting the programme
        """
        self.lock = asyncio.Lock()
        asyncio.run(self.asynGetData())

class Aio(Settings):
    def __init__(self, workerAmount: int):
        super().__init__()
        self.workerAmount = workerAmount

    def initFiles(self):
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
        qrys = formQueries(keywords, start, end, freq='D')
        self._oneStep(qrys)
        return collectRawData(self.pathDataIOT)

    def _getRawMonthlyData(self, keywords, start, end):
        qrys = formQueries(keywords, start, end, freq='M')
        self._oneStep(qrys)
        return collectRawData(self.pathDataIOT)
    
    def getMonthlyData(self, keywords: list, start: str or date or datetime='2004-01-01', end: str or date or datetime=date.today()):
        _d = self._getRawMonthlyData(keywords, start, end)
        _dm = {}
        for key in _d.keys():
            _dm[key] = _d[key][0]
        _dm = DataFrame(_dm)
        _dm.index = to_datetime(_dm, unit='s')
        return _dm

    def getScaledDailyData(self, keywords: list, filename: str, start: str or date or datetime='2004-01-01', end: str or date or datetime=date.today())->DataFrame:
        """This is a function of getting scaled daily data. 

        Args:
            keywords (list): A list of queries with keywords. E.g., ['AAPL', 'AMZN', 'TSLA', 'MSFT', 'GOOGL'].
            filename (str): A filename with extension for storing the file. E.g., 'scaledDailyData.pkl' stored as a format of pickle. Optional extensions are csv and json.
            start (str or date or datetime): The start date of searching periods.
            end (str or date or datetime): The end date of searching periods.

        Raises:
            InputError

        Returns:
            DataFrame: A dataframe of searching volume index with column lables of keywords and a datetime index
        """
        if os.path.exists(os.path.join(self.pathScaledDaily, filename)):
            raise InputError(
                f"File existed under the folder {self.pathScaledDaily}"
            )
        
        if not os.path.exists(os.path.join(self.pathRawDaily, filename)):
            self.initFiles()
            daily = self._getRawDailyData(keywords, start, end)
            with open(os.path.join(self.pathRawDaily, filename), 'wb') as f:
                pickle.dump(daily, f)
        else:
            daily = pickle.load(open(os.path.join(self.pathRawDaily, filename), 'rb'))           
        
        if not os.path.exists(os.path.join(self.pathRawMonthly, filename)):
            self.initFiles()
            monthly = self._getRawMonthlyData(keywords, start, end)
            with open(os.path.join(self.pathRawMonthly, filename), 'wb') as f:
                pickle.dump(monthly, f)
        else:
            monthly = pickle.load(open(os.path.join(self.pathRawMonthly, filename), 'rb'))      

        samples = scaledDailyData(daily, monthly)

        fileExt = filename.split('.')[1]
        if fileExt == 'json':
            with open(os.path.join(self.pathScaledDaily, filename), 'w') as f:
                json.dump(json.dumps(samples.to_dict()), f)
        samples = DataFrame(samples)
        samples.index = to_datetime(samples.index, unit='s')
        samples.sort_index(inplace=True)
        if fileExt == 'pkl':
            samples.to_pickle(os.path.join(self.pathScaledDaily, filename))
        elif fileExt == 'csv':
            samples.to_csv(os.path.join(self.pathScaledDaily, filename))
        return samples


