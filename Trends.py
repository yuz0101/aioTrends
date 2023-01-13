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

TIMEOUT = 30
LOCK = asyncio.Lock()

if 'win' in sys.platform:
    import colorama
    colorama.init()
    #policy = asyncio.WindowsSelectorEventLoopPolicy()
    #asyncio.set_event_loop_policy(policy)

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

async def pkl(path: str, obj: any)->None:
    async with LOCK:
        async with aiofiles.open(path, 'wb') as f:
            p = pickle.dumps(obj)
            await f.write(p)

async def unpkl(path: str)->any:
    async with LOCK:
        async with aiofiles.open(path, 'rb') as f:
            p = await f.read()
        p = pickle.loads(p)
    return p

async def updateList(path:str, a: any)->None:
    async with LOCK:
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

async def updateDict(path: str, d: dict)->None:
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

async def popDict(path: str, key)-> None:
    async with LOCK:
        if await aiofiles.os.path.exists(path):
            async with aiofiles.open(path, 'rb') as f:
                p = await f.read()
                s = pickle.loads(p)
            s.pop(key)
            async with aiofiles.open(path, 'wb') as f:
                p = pickle.dumps(s)
                await f.write(p)

async def readCookies(path: str)->dict:
    s = await unpkl(path)
    return random.choice(s)

class CookiesPool(Settings):
    def __init__(self, workerAmount: int):
        super().__init__()
        self.workerAmount = workerAmount
        self.queueCookies = asyncio.Queue()
    
    async def getCookies(self, i: str or int, userAgent: str, proxy: str, timeout: int)->None:
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    f'https://trends.google.com/?geo={self.geo}', proxy=proxy #,timeout=timeout
                    ) as res:
                    if await self.status(f'|{i}|Cookies Pool', res):
                        cookies = dict(filter(lambda i: i[0]=='NID', res.cookies.items()))
                        await updateList(self.pathCookies, cookies)
                        self.cookiesNumb += 1
                        logging.critical(f'|{i}|Cookies Pool|Amount:{self.cookiesNumb}| Got')
                    else:
                        logging.error(f'|{i}|Cookies Pool|Amount:{self.cookiesNumb}| failed')
        except Exception as e:
            logging.error(f'|{i}|Cookies Pool|Amount:{self.cookiesNumb}|Unknown Error| {repr(e)}')

    async def worker(self, i:str or int, timeout: int)->None:
        while True:
            _ = await self.queueCookies.get()
            userAgent = random.choice(self.userAgents)
            proxy = random.choice(self.proxies)
            await self.getCookies(i, userAgent, proxy, timeout)
            self.queueCookies.task_done()
    
    async def filterCookies(self, cookiesQrys: int):
        if os.path.exists(self.pathCookies):
            self.cookiesNumb = len(await unpkl(self.pathCookies))
        else:
            self.cookiesNumb = 0
        self.cookiesQrys = cookiesQrys - self.cookiesNumb

    async def asyncGetCookies(self, cookiesQrys: int, timeout: int)->None:
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
                task = asyncio.create_task(self.worker(i, timeout))
                tasks.append(task)
            await self.queueCookies.join()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.filterCookies(cookiesQrys)

    def run(self, cookiesQrys: int, timeout=TIMEOUT):
        asyncio.run(self.asyncGetCookies(cookiesQrys, timeout))

class WidgetsPool(Settings):

    def __init__(self, workerAmount: int):
        super().__init__()
        self.workerAmount = workerAmount
        self.queueWgt = asyncio.Queue()
    
    async def widgetInterestOverTime(
        self, i: int, tid, cookies: dict, proxy: str, userAgent: str, timeout: int
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
                    self.urls['token'], params=payload, cookies=cookies, proxy=proxy, timeout=timeout
                    ) as res:
                    if await self.status(f'|{i}|Widget Pool|TID:{tid}|Remained:{self.wgtQryN}', res):
                        res = await res.text()
                        res = json.loads(res[4:])
                        widgets = res['widgets']
                        widget = list(filter(lambda w: w['id']=='TIMESERIES', widgets))[0]
                        await updateDict(self.pathWgt, {tid: widget})
                        self.wgtQryN -= 1
                        logging.critical(f'|{i}|Widget Pool|TID:{tid}|Remained:{self.wgtQryN}| Stored')
        except Exception as e:
            logging.error(f'|{i}|Widget Pool|TID:{tid}|Remained:{self.wgtQryN}|Unknown Error| {repr(e)}')

    async def worker(self, i:int, timeout:int)->None:
        while True:
            tid = await self.queueWgt.get()
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            cookies = await readCookies(self.pathCookies)
            await self.widgetInterestOverTime(i, tid, cookies, proxy, userAgent, timeout)
            self.queueWgt.task_done()

    async def filterQrys(self)->list:
        k = list(self.qrys.keys())
        try:
            dkeys = await unpkl(self.pathWgt)
            k = list(set(k)-set(dkeys))
        except:
            pass
        try:
            dkeys = await unpkl(self.pathWgtEptyRes)
            k = list(set(k)-set(dkeys))
        except:
            pass
        return k

    async def asyncGetWidget(self, timeout: int)->None:
        self.proxies = await readProxies(self.pathProxies)
        self.userAgents = await readJson(self.pathUserAgents)
        self.qrys = await unpkl(self.pathQrys)
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
                task = asyncio.create_task(self.worker(i, timeout))
                tasks.append(task)
            await self.queueWgt.join()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            k = await self.filterQrys()
            self.wgtQryN = len(k)
            random.shuffle(k)

    def run(self, timeout: int=TIMEOUT)->None:
        asyncio.run(self.asyncGetWidget(timeout))

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
        self, i:int, tid: str or int, userAgent: str, proxy: str, cookies: dict, timeout: int
        )->None:
        """This is the main function of getting data of interest over time.

        Args:
            i (int): Worker id.
            tid (strorint): Task id.
            userAgent (str): The user agent loaded on headers for GET.
            proxy (str): A proxy in a format of f"http://{username}:{password}@{ip}:{port}" or f"http://{ip}:{port}".
            cookies (dict): Cookies requested from google.
            timeout (int): A timeout setting for aiohttp.ClientTimeout.
        """
        payload = {
            'req': json.dumps(self.widgets[tid]['request']),
            'token': self.widgets[tid]['token'],
            'tz': self.tz
        }
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    self.urls['multiline'], params=payload, proxy=proxy, timeout=timeout #cookies=cookies,
                ) as res:
                    if await self.status(f'|{i}|Data|Remained:{self.qryN}|TID:{tid}', res):
                        res = await res.text()
                        res = json.loads(res[5:])['default']['timelineData']
                        self.qryN -= 1 
                        await popDict(self.pathWgt, tid)
                        if len(res) > 0:
                            #await updateDict(self.pathDataIOT, {int(tid): res})
                            await pkl(f'{self.pathDataIOT}{tid}.pkl', res)
                            logging.critical(f'|{i}|Data|Remained:{self.qryN}|TID:{tid}| Stored **')
                        else:
                            await updateDict(self.pathWgtEptyRes, {int(tid): self.widgets[tid]})
                            logging.error(f'|{i}|Data|Remained:{self.qryN}|TID:{tid}| Empty Set')
        except Exception as e:
            logging.error(f'|{i}|Data|Remained:{self.qryN}|TID:{tid}|Unknown Error| {repr(e)}')

    async def worker(self, i: int, timeout: int)->None:
        while True:
            tid = await self.queueData.get()
            userAgent = random.choice(self.userAgents)
            proxy = random.choice(self.proxies)
            cookies = await readCookies(self.pathCookies)
            await self.getData(i, tid, userAgent, proxy, cookies, timeout)
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
            #dkeys = await unpkl(self.pathDataIOT)
            dkeys = readDatakeys(self.pathDataIOT)
            k = list(set(k)-set(dkeys))
        except:
            pass
        try:
            dkeys = await unpkl(self.pathWgtEptyRes)
            k = list(set(k)-set(dkeys))
        except:
            pass
        return k
    
    async def asynGetData(self, timeout: int)->None:
        self.userAgents = await readJson(self.pathUserAgents)
        self.proxies = await readProxies(self.pathProxies)
        self.widgets = await unpkl(self.pathWgt)
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
                task = asyncio.create_task(self.worker(i, timeout))
                tasks.append(task)
            await self.queueData.join()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            k = await self.filterWidgets()
            self.qryN = len(k)
            random.shuffle(k)

    def run(self, timeout: int=TIMEOUT) -> None:
        """This is a function of starting the programme

        Args:
            timeout (int, optional): aiohttp.ClientTimeout. Defaults to TIMEOUT.
        """
        asyncio.run(self.asynGetData(timeout))
