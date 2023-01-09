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
        while self.cookiesQueries > self.cookiesNumb:
            await self.getCookies(userAgent, proxy, timeout)

    async def asyncGetCookies(self, timeout: int):
        tasks = []
        for i in range(self.asyncNumb):
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            tasks.append(self.loopGetCookies(userAgent, proxy, timeout))
        await asyncio.gather(*tasks)

    def run(self, cookiesQueries: int, timeout=TIMEOUT):
        self.userAgents = readJson(self.pathUserAgents)
        self.proxies = readProxies(self.pathProxies)
        if os.path.exists(self.pathCookies):
            self.cookiesNumb = len(loadDict(self.pathCookies))
        else:
            self.cookiesNumb = 0
        self.cookiesQueries = cookiesQueries# - self.cookiesNumb
        if self.cookiesQueries < self.asyncNumb:
            self.asyncNumb = self.cookiesQueries
        asyncio.run(self.asyncGetCookies(timeout))
            
class WidgetsPool(Settings):

    def __init__(self, asyncNumb):
        super().__init__()
        self.asyncNumb = asyncNumb
    
    async def widgetInterestOverTime(self, tid, cookies, proxy, userAgent, timeout):
        tokenPayload = {
            'hl': self.hl, 
            'tz': self.tz, 
            'req': {
                'comparisonItem': [
                    {'keyword': kw, 'time': self.queries[tid]['periods'], 'geo': self.geo} for kw in self.queries[tid]['keywords']
                    ], 
                'category': self.cat, 
                'property': self.gprop
                }
            }
        tokenPayload['req'] = json.dumps(tokenPayload['req'])
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    self.urls['token'], params=tokenPayload, cookies=cookies, proxy=proxy#, timeout=timeout
                    ) as res:
                    if await self.status(f'[Widget Pool][TID:{tid}][Remained Widgets:{self.widgetQueryAmount}]', res):
                        res = await res.text()
                        res = json.loads(res[4:])
                        widgets = res['widgets']
                        widget = list(filter(lambda w: w['id']=='TIMESERIES', widgets))[0]
                        await updateDict(self.pathWidget, {tid: widget})
                        self.widgetQueryAmount -= 1
                        logging.critical(f'[Widget Pool][TID:{tid}][Remained:{self.widgetQueryAmount}] Stored')
        except Exception as e:
            logging.error(f'[Widget Pool][TID:{tid}][Remained:{self.widgetQueryAmount}][Unknown Error] {repr(e)}')

    async def loopWidgets(self, queryKeys, timeout):
        for tid in queryKeys:
            cookies = readCookies(self.pathCookies)
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            await self.widgetInterestOverTime(tid, cookies, proxy, userAgent, timeout)
    
    async def asyncGetWidget(self, querykeyss, timeout):
        tasks = []
        for queryKeys in querykeyss:
            tasks.append(self.loopWidgets(queryKeys, timeout))
        await asyncio.gather(*tasks)

    def filterQueries(self):
        skeys = list(self.queries.keys())
        try:
            dkeys = loadDict(self.pathWidget)
            skeys = list(set(skeys)-set(dkeys))
        except:
            pass
        try:
            dkeys = loadDict(self.pathWidgetReceiveEmptyReponse)
            skeys = list(set(skeys)-set(dkeys))
        except:
            pass
        return skeys

    def run(self, timeout=TIMEOUT):
        self.proxies = readProxies(self.pathProxies)
        self.userAgents = readJson(self.pathUserAgents)
        self.queries = loadDict(self.pathQueries)
        skeys = self.filterQueries()
        self.widgetQueryAmount = len(skeys)
        random.shuffle(skeys)
        while self.widgetQueryAmount > 0:
            if self.widgetQueryAmount < self.asyncNumb:
                self.asyncNumb = self.widgetQueryAmount
            querykeyss = np.array_split(list(skeys), self.asyncNumb)
            asyncio.run(self.asyncGetWidget(querykeyss, timeout))
            skeys = self.filterQueries()
            self.widgetQueryAmount = len(skeys)
            random.shuffle(skeys)

class DataInterestOverTime(Settings):
    def __init__(self, asyncNumb: int):
        super().__init__()
        self.asyncNumb = asyncNumb

    async def getData(self, tid, userAgent: str, proxy: str, cookies: dict, timeout: int):
        queryPayload = {
            'req': json.dumps(self.widgets[tid]['request']),
            'token': self.widgets[tid]['token'],
            'tz': self.tz
        }
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    self.urls['multiline'], params=queryPayload, proxy=proxy#, cookies=cookies, timeout=timeout
                ) as res:
                    if await self.status(f'[Data][Remained:{self.queryDataAmount}][TID:{tid}]', res):
                        res = await res.text()
                        res = json.loads(res[5:])['default']['timelineData']
                        self.queryDataAmount -= 1 
                        await popDict(self.pathWidget, tid)
                        #key = widget['request']['comparisonItem'][0]['complexKeywordsRestriction']['keyword'][0]['value']
                        if len(res) > 0:
                            await updateDict(self.pathDataInterstOverTime, {int(tid): res})
                            logging.critical(f'[Data][Remained:{self.queryDataAmount}][TID:{tid}] Stored **')
                        else:
                            await updateDict(self.pathWidgetReceiveEmptyReponse, {int(tid): self.widgets[tid]})
                            logging.error(f'[Data][Remained:{self.queryDataAmount}][TID:{tid}] Empty Set')
        except Exception as e:
            logging.error(f'[Data][Remained:{self.queryDataAmount}][TID:{tid}][Unknown Error] {repr(e)}')

    async def loopGetData(self, querykeys, timeout):
        for tid in querykeys:
            userAgent = self.userAgents[random.randint(0, len(self.userAgents)-1)]
            proxy = self.proxies[random.randint(0, len(self.proxies)-1)]
            cookies = readCookies(self.pathCookies)
            await self.getData(tid, userAgent, proxy, cookies, timeout)

    async def asyncGetData(self, querykeyss, timeout):
        tasks = []
        for querykeys in querykeyss:
            tasks.append(self.loopGetData(querykeys, timeout))
        await asyncio.gather(*tasks)

    def filterWidgets(self):
        skeys = list(self.widgets.keys())
        try:
            # load & filter the finished tasks
            dkeys = loadDict(self.pathDataInterstOverTime)
            skeys = list(set(skeys)-set(dkeys))
        except:
            pass
        try:
            # load & filter the tasks with empty responses from gooogle. There is not enough searching data for google to form a SVI.
            dkeys = loadDict(self.pathWidgetReceiveEmptyReponse)
            skeys = list(set(skeys)-set(dkeys))
        except:
            pass
        return skeys

    def run(self, timeout=TIMEOUT):
        self.userAgents = readJson(self.pathUserAgents)
        self.proxies = readProxies(self.pathProxies)
        self.widgets = loadDict(self.pathWidget)
        skeys = self.filterWidgets()
        self.queryDataAmount = len(skeys)
        random.shuffle(skeys)
        while self.queryDataAmount > 0:
            if self.queryDataAmount < self.asyncNumb:
                self.asyncNumb = self.queryDataAmount
            querykeyss = np.array_split(list(skeys), self.asyncNumb)
            asyncio.run(self.asyncGetData(querykeyss, timeout))
            skeys = self.filterWidgets()
            self.queryDataAmount = len(skeys)
            random.shuffle(skeys)
