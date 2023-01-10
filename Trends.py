# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import pickle
import random
import sys
import time

import aiohttp
import aiofiles
import aiofiles.os
import numpy as np

from aioTrends.Settings import Settings

TIMEOUT = 10
LOCK = asyncio.Lock()

if 'win' in sys.platform:
    import colorama
    colorama.init()
    policy = asyncio.WindowsSelectorEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)

def readJson(path: str):
    with open(path, 'r') as f:
        s = json.load(f)
    return s

def readProxies(path: str):
    with open(path, 'r') as f:
        f = f.read().split('\n')
    f = [x.split(':') for x in f]
    return [f'http://{x[2]}:{x[3]}@{x[0]}:{x[1]}' for x in f]

async def pkl(path, obj):
    async with LOCK:
        async with aiofiles.open(path, 'wb') as f:
            p = pickle.dumps(obj)
            await f.write(p)

async def unpkl(path):
    async with LOCK:
        async with aiofiles.open(path, 'rb') as f:
            p = await f.read()
        p = pickle.loads(p)
    return p

async def updateDict(path, d: dict):
    async with LOCK:
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

def loadDict(path: str):
    with open(path, 'rb') as f:
        s = pickle.load(f)
    return s

async def popDict(path: str, key):
    async with LOCK:
        if await aiofiles.os.path.exists(path):
            async with aiofiles.open(path, 'rb') as f:
                p = await f.read()
                s = pickle.loads(p)
            #print(str(s)[:100])
            s.pop(key)
            async with aiofiles.open(path, 'wb') as f:
                p = pickle.dumps(s)
                await f.write(p)

def readCookies(path: str):
    s = loadDict(path)
    l = list(s.keys())
    return s[l[random.randint(0, len(l)-1)]]
"""
class CookiesPool(Settings):
    def __init__(self, asyncNumb):
        super().__init__()
        self.asyncNumb = asyncNumb
    
    async def getCookies(self, userAgent, proxy, timeout):
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    f'https://trends.google.com/?geo={self.geo}', proxy=proxy #,timeout=timeout
                    ) as res:
                    if await self.status('[Cookies Pool]', res):
                        cookies = dict(filter(lambda i: i[0]=='NID', res.cookies.items()))
                        await updateDict(self.pathCookies, {self.cookiesNumb: cookies})
                        self.cookiesNumb += 1
                        logging.critical(f'[Cookies Pool][Amount:{self.cookiesNumb}] {self.cookiesNumb} Got')
                    else:
                        logging.error(f'[Cookies Pool][Amount:{self.cookiesNumb}] failed')
        except Exception as e:
            logging.error(f'[Cookies Pool][Amount:{self.cookiesNumb}][Unknown Error] {repr(e)}')
    
    async def loopGetCookies(self, userAgent, proxy, timeout):
        while self.cookiesQrys > self.cookiesNumb:
            await self.getCookies(userAgent, proxy, timeout)

    async def asyncGetCookies(self, timeout: int):
        tasks = []
        for i in range(self.asyncNumb):
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            tasks.append(self.loopGetCookies(userAgent, proxy, timeout))
        await asyncio.gather(*tasks)

    def run(self, cookiesQrys: int, timeout=TIMEOUT):
        self.userAgents = readJson(self.pathUserAgents)
        self.proxies = readProxies(self.pathProxies)
        if os.path.exists(self.pathCookies):
            self.cookiesNumb = len(loadDict(self.pathCookies))
        else:
            self.cookiesNumb = 0
        self.cookiesQrys = cookiesQrys# - self.cookiesNumb
        if self.cookiesQrys < self.asyncNumb:
            self.asyncNumb = self.cookiesQrys
        asyncio.run(self.asyncGetCookies(timeout))
"""

class CookiesPool(Settings):
    def __init__(self, asyncNumb):
        super().__init__()
        self.asyncNumb = asyncNumb
    
    async def getCookies(self, i, userAgent, proxy, timeout):
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    f'https://trends.google.com/?geo={self.geo}', proxy=proxy #,timeout=timeout
                    ) as res:
                    if await self.status(f'[{i}][Cookies Pool]', res):
                        cookies = dict(filter(lambda i: i[0]=='NID', res.cookies.items()))
                        await updateDict(self.pathCookies, {self.cookiesNumb: cookies})
                        self.cookiesNumb += 1
                        logging.critical(f'[{i}][Cookies Pool][Amount:{self.cookiesNumb}] {self.cookiesNumb} Got')
                    else:
                        logging.error(f'[{i}][Cookies Pool][Amount:{self.cookiesNumb}] failed')
        except Exception as e:
            logging.error(f'[{i}][Cookies Pool][Amount:{self.cookiesNumb}][Unknown Error] {repr(e)}')

    async def worker(self, i, queue, timeout):
        while True:
            # Get a "work item" out of the queue.
            _ = await queue.get()
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            
            # Get cookies
            await self.getCookies(i, userAgent, proxy, timeout)

            # Notify the queue that the "work item" has been processed.
            queue.task_done()

    async def asyncGetCookies(self, timeout):
        self.userAgents = readJson(self.pathUserAgents)
        self.proxies = readProxies(self.pathProxies)

        # Create a queue that we will use to store our "workload".
        queue = asyncio.Queue()

        # Put args: cid (cookies id) into the queue
        for cid in range(self.cookiesQrys):
            queue.put_nowait(cid)
        
        # Create worker tasks to process the queue concurrently.
        tasks = []
        for i in range(self.asyncNumb):
            task = asyncio.create_task(self.worker(i, queue, timeout))
            tasks.append(task)
        
        # Wait until the queue is fully processed.
        await queue.join()
        for task in tasks:
            task.cancel()
        
        # Wait until all worker tasks are cancelled.
        await asyncio.gather(*tasks, return_exceptions=True)

    def run(self, cookiesQrys: int, timeout=TIMEOUT):
        self.userAgents = readJson(self.pathUserAgents)
        self.proxies = readProxies(self.pathProxies)
        if os.path.exists(self.pathCookies):
            self.cookiesNumb = len(loadDict(self.pathCookies))
        else:
            self.cookiesNumb = 0
        self.cookiesQrys = cookiesQrys - self.cookiesNumb
        if self.cookiesQrys < self.asyncNumb:
            self.asyncNumb = self.cookiesQrys
        asyncio.run(self.asyncGetCookies(timeout))

class WidgetsPool(Settings):

    def __init__(self, asyncNumb):
        super().__init__()
        self.asyncNumb = asyncNumb
    
    async def widgetInterestOverTime(self, i, tid, cookies, proxy, userAgent, timeout):
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
                    self.urls['token'], params=payload, cookies=cookies, proxy=proxy#, timeout=timeout
                    ) as res:
                    if await self.status(f'[{i}][Widget Pool][TID:{tid}][Remained Widgets:{self.wgtQryN}]', res):
                        res = await res.text()
                        res = json.loads(res[4:])
                        widgets = res['widgets']
                        widget = list(filter(lambda w: w['id']=='TIMESERIES', widgets))[0]
                        await updateDict(self.pathWgt, {tid: widget})
                        self.wgtQryN -= 1
                        logging.critical(f'[{i}][Widget Pool][TID:{tid}][Remained:{self.wgtQryN}] Stored')
        except Exception as e:
            logging.error(f'[{i}][Widget Pool][TID:{tid}][Remained:{self.wgtQryN}][Unknown Error] {repr(e)}')

    async def worker(self, i, queue, timeout):
        while True:
            # Get a "work item" out of the queue.
            tid = await queue.get()
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            cookies = readCookies(self.pathCookies)
            
            # Get cookies
            await self.widgetInterestOverTime(i, tid, cookies, proxy, userAgent, timeout)

            # Notify the queue that the "work item" has been processed.
            queue.task_done()

    def filterQrys(self):
        k = list(self.qrys.keys())
        try:
            dkeys = loadDict(self.pathWgt)
            k = list(set(k)-set(dkeys))
        except:
            pass
        try:
            dkeys = loadDict(self.pathWgtEptyRes)
            k = list(set(k)-set(dkeys))
        except:
            pass
        return k

    async def asyncGetWidget(self, timeout):
        self.proxies = readProxies(self.pathProxies)
        self.userAgents = readJson(self.pathUserAgents)
        self.qrys = loadDict(self.pathQrys)
        k = self.filterQrys()
        self.wgtQryN = len(k)
        random.shuffle(k)
        if self.wgtQryN < self.asyncNumb:
            self.asyncNumb = self.wgtQryN
        self.qryN = len(k)
        random.shuffle(k)

        while self.qryN > 0:
            if self.qryN < self.asyncNumb:
                self.asyncNumb = self.qryN
            
            # Create a queue that we will use to store our "workload".
            queue = asyncio.Queue()

            # Put tid (task id) into the queue
            for tid in k:
                queue.put_nowait(tid)
            
            # Create worker tasks to process the queue concurrently.
            tasks = []
            for i in range(self.asyncNumb):
                task = asyncio.create_task(self.worker(i, queue, timeout))
                tasks.append(task)
            
            # Wait until the queue is fully processed.
            await queue.join()
            for task in tasks:
                task.cancel()
            
            # Wait until all worker tasks are cancelled.
            await asyncio.gather(*tasks, return_exceptions=True)

            k = self.filterWidgets()
            self.qryN = len(k)
            random.shuffle(k)

            print('another loop', self.qryN,self.qryN,self.qryN,self.qryN,self.qryN,self.qryN,self.qryN,self.qryN)

    def run(self, timeout=TIMEOUT):
        asyncio.run(self.asyncGetWidget(timeout))
        
"""
class WidgetsPool(Settings):

    def __init__(self, asyncNumb):
        super().__init__()
        self.asyncNumb = asyncNumb
    
    async def widgetInterestOverTime(self, tid, cookies, proxy, userAgent, timeout):
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
                    self.urls['token'], params=payload, cookies=cookies, proxy=proxy#, timeout=timeout
                    ) as res:
                    if await self.status(f'[Widget Pool][TID:{tid}][Remained Widgets:{self.wgtQryN}]', res):
                        res = await res.text()
                        res = json.loads(res[4:])
                        widgets = res['widgets']
                        widget = list(filter(lambda w: w['id']=='TIMESERIES', widgets))[0]
                        await updateDict(self.pathWgt, {tid: widget})
                        self.wgtQryN -= 1
                        logging.critical(f'[Widget Pool][TID:{tid}][Remained:{self.wgtQryN}] Stored')
        except Exception as e:
            logging.error(f'[Widget Pool][TID:{tid}][Remained:{self.wgtQryN}][Unknown Error] {repr(e)}')

    async def loopWidgets(self, qryks, timeout):
        for tid in qryks:
            cookies = readCookies(self.pathCookies)
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            await self.widgetInterestOverTime(tid, cookies, proxy, userAgent, timeout)
    
    async def asyncGetWidget(self, qrykss, timeout):
        tasks = []
        for qryks in qrykss:
            tasks.append(self.loopWidgets(qryks, timeout))
        await asyncio.gather(*tasks)

    def filterQrys(self):
        k = list(self.qrys.keys())
        try:
            dkeys = loadDict(self.pathWgt)
            k = list(set(k)-set(dkeys))
        except:
            pass
        try:
            dkeys = loadDict(self.pathWgtEptyRes)
            k = list(set(k)-set(dkeys))
        except:
            pass
        return k

    def run(self, timeout=TIMEOUT):
        self.proxies = readProxies(self.pathProxies)
        self.userAgents = readJson(self.pathUserAgents)
        self.qrys = loadDict(self.pathQrys)
        k = self.filterQrys()
        self.wgtQryN = len(k)
        random.shuffle(k)
        while self.wgtQryN > 0:
            if self.wgtQryN < self.asyncNumb:
                self.asyncNumb = self.wgtQryN
            qrykss = np.array_split(list(k), self.asyncNumb)
            asyncio.run(self.asyncGetWidget(qrykss, timeout))
            k = self.filterQrys()
            self.wgtQryN = len(k)
            random.shuffle(k)


class DataInterestOverTime(Settings):
    def __init__(self, asyncNumb: int):
        super().__init__()
        self.asyncNumb = asyncNumb

    async def getData(self, tid, userAgent: str, proxy: str, cookies: dict, timeout: int):
        payload = {
            'req': json.dumps(self.widgets[tid]['request']),
            'token': self.widgets[tid]['token'],
            'tz': self.tz
        }
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    self.urls['multiline'], params=payload, proxy=proxy#, cookies=cookies, timeout=timeout
                ) as res:
                    if await self.status(f'[Data][Remained:{self.qryN}][TID:{tid}]', res):
                        res = await res.text()
                        res = json.loads(res[5:])['default']['timelineData']
                        self.qryN -= 1 
                        await popDict(self.pathWgt, tid)
                        if len(res) > 0:
                            await updateDict(self.pathDataIOT, {int(tid): res})
                            logging.critical(f'[Data][Remained:{self.qryN}][TID:{tid}] Stored **')
                        else:
                            await updateDict(self.pathWgtEptyRes, {int(tid): self.widgets[tid]})
                            logging.error(f'[Data][Remained:{self.qryN}][TID:{tid}] Empty Set')
        except Exception as e:
            logging.error(f'[Data][Remained:{self.qryN}][TID:{tid}][Unknown Error] {repr(e)}')

    async def loopGetData(self, qryks, timeout):
        for tid in qryks:
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            cookies = readCookies(self.pathCookies)
            await self.getData(tid, userAgent, proxy, cookies, timeout)

    async def asyncGetData(self, qrykss, timeout):
        tasks = []
        for qryks in qrykss:
            tasks.append(self.loopGetData(qryks, timeout))
        await asyncio.gather(*tasks)

    def filterWidgets(self):
        k = list(self.widgets.keys())
        try:
            # load & filter the finished tasks
            dkeys = loadDict(self.pathDataIOT)
            k = list(set(k)-set(dkeys))
        except:
            pass
        try:
            # load & filter the tasks with empty responses from gooogle. There is not enough searching data for google to form a SVI.
            dkeys = loadDict(self.pathWgtEptyRes)
            k = list(set(k)-set(dkeys))
        except:
            pass
        return k

    def run(self, timeout=TIMEOUT):
        self.userAgents = readJson(self.pathUserAgents)
        self.proxies = readProxies(self.pathProxies)
        self.widgets = loadDict(self.pathWgt)
        k = self.filterWidgets()
        self.qryN = len(k)
        random.shuffle(k)
        while self.qryN > 0:
            if self.qryN < self.asyncNumb:
                self.asyncNumb = self.qryN
            qrykss = np.array_split(list(k), self.asyncNumb)
            asyncio.run(self.asyncGetData(qrykss, timeout))
            k = self.filterWidgets()
            self.qryN = len(k)
            random.shuffle(k)

"""

class DataInterestOverTime(Settings):
    def __init__(self, asyncNumb: int):
        super().__init__()
        self.asyncNumb = asyncNumb

    async def getData(self, i, tid, userAgent: str, proxy: str, cookies: dict, timeout: int):
        payload = {
            'req': json.dumps(self.widgets[tid]['request']),
            'token': self.widgets[tid]['token'],
            'tz': self.tz
        }
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    self.urls['multiline'], params=payload, proxy=proxy#, cookies=cookies, timeout=timeout
                ) as res:
                    if await self.status(f'[{i}][Data][Remained:{self.qryN}][TID:{tid}]', res):
                        res = await res.text()
                        res = json.loads(res[5:])['default']['timelineData']
                        self.qryN -= 1 
                        await popDict(self.pathWgt, tid)
                        if len(res) > 0:
                            await updateDict(self.pathDataIOT, {int(tid): res})
                            logging.critical(f'[{i}][Data][Remained:{self.qryN}][TID:{tid}] Stored **')
                        else:
                            await updateDict(self.pathWgtEptyRes, {int(tid): self.widgets[tid]})
                            logging.error(f'[{i}][Data][Remained:{self.qryN}][TID:{tid}] Empty Set')
        except Exception as e:
            logging.error(f'[{i}][Data][Remained:{self.qryN}][TID:{tid}][Unknown Error] {repr(e)}')

    async def worker(self, i, queue, timeout):
        while True:
            # Get a "work item" out of the queue.
            tid = await queue.get()
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            cookies = readCookies(self.pathCookies)
            
            # Get data
            await self.getData(i, tid, userAgent, proxy, cookies, timeout)

            # Notify the queue that the "work item" has been processed.
            queue.task_done()

    def filterWidgets(self):
        k = list(self.widgets.keys())
        try:
            # load & filter the finished tasks
            dkeys = loadDict(self.pathDataIOT)
            k = list(set(k)-set(dkeys))
        except:
            pass
        try:
            # load & filter the tasks with empty responses from gooogle. There is not enough searching data for google to form a SVI.
            dkeys = loadDict(self.pathWgtEptyRes)
            k = list(set(k)-set(dkeys))
        except:
            pass
        return k

    async def asynGetData(self, timeout):
        self.userAgents = readJson(self.pathUserAgents)
        self.proxies = readProxies(self.pathProxies)
        self.widgets = loadDict(self.pathWgt)
        k = self.filterWidgets()
        self.qryN = len(k)
        random.shuffle(k)
        while self.qryN > 0:
            if self.qryN < self.asyncNumb:
                self.asyncNumb = self.qryN
            
            # Create a queue that we will use to store our "workload".
            queue = asyncio.Queue()

            # Put tid (task id) into the queue
            for tid in k:
                queue.put_nowait(tid)
            
            # Create worker tasks to process the queue concurrently.
            tasks = []
            for i in range(self.asyncNumb):
                task = asyncio.create_task(self.worker(i, queue, timeout))
                tasks.append(task)
            
            # Wait until the queue is fully processed.
            await queue.join()
            for task in tasks:
                task.cancel()
            
            # Wait until all worker tasks are cancelled.
            await asyncio.gather(*tasks, return_exceptions=True)

            k = self.filterWidgets()
            self.qryN = len(k)
            random.shuffle(k)

            print('another loop', self.qryN,self.qryN,self.qryN,self.qryN,self.qryN,self.qryN,self.qryN,self.qryN)

    def run(self, timeout=TIMEOUT):
        asyncio.run(self.asynGetData(timeout))