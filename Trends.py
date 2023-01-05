# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import pickle
import random
import sys
import time

import aiohttp
import numpy as np

from aioTrends.Settings import Settings

TIMEOUT = 10

if 'win' in sys.platform:
    import colorama
    colorama.init()
    policy = asyncio.WindowsSelectorEventLoopPolicy()
    asyncio.set_event_loop_policy(policy)

def readProxies(path: str):
    with open(path) as f:
        f = f.read().split('\n')
    f = [x.split(':') for x in f]
    return [f'http://{x[2]}:{x[3]}@{x[0]}:{x[1]}' for x in f]

def storeCookies(path: str, cookies: dict):
    cid = int(time.time())
    cookies = {cid: cookies}
    storeDict(path, cookies)

def readCookies(path: str):
    with open(path, 'rb') as f:
        s = pickle.load(f)
        l = list(s.keys())
    return s[l[random.randint(0, len(l)-1)]]

def storeDict(path: str, data):
    try:
        with open(path, 'rb') as f:
            s = pickle.load(f)
    except:
        s = {}
    s.update(data)
    with open(path, 'wb') as f:
        pickle.dump(s, f)

def popDict(path: str, key):
    try:
        with open(path, 'rb') as f:
            s = pickle.load(f)
        s.pop(key)
        with open(path, 'wb') as f:
            pickle.dump(s, f)
    except:
        pass

class CookiesPool(Settings):
    def __init__(self, asyncNumb):
        super().__init__()
        self.asyncNumb = asyncNumb
     
    async def getCookies(self, userAgent, proxy, timeout):
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    f'https://trends.google.com/?geo={self.geo}', proxy=proxy, timeout=timeout
                    ) as res:
                    if await self.status('[Cookies Pool]', res):
                        cookies = dict(filter(lambda i: i[0]=='NID', res.cookies.items()))
                        self.cookiesNumb += 1
                        logging.critical(f'[Cookies Pool][Amount:{self.cookiesNumb}] Got')
                        storeCookies(self.pathCookies, cookies)
                    else:
                        logging.error(f'[Cookies Pool][Amount:{self.cookiesNumb}] failed')
        except Exception as e:
            logging.error(f'[Cookies Pool][Amount:{self.cookiesNumb}][Unknown Error] {e}')

    def asyncGetCookies(self, userAgents, proxies, timeout: int):
        tasks = []
        for i in range(self.asyncNumb):
            proxy = proxies[random.randint(0, len(proxies)-1)]
            userAgent = userAgents[random.randint(0, len(userAgents)-1)]
            tasks.append(self.getCookies(userAgent, proxy, timeout))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
    
    def run(self, cookiesQueries: int, timeout=TIMEOUT):
        userAgents = json.load(open(self.pathUserAgents, 'r'))
        proxies = readProxies(self.pathProxies)
        self.cookiesNumb = len(pickle.load(open(self.pathCookies, 'rb')))
        for i in range(int(cookiesQueries/self.asyncNumb)):
            self.asyncGetCookies(userAgents, proxies, timeout)
    
class WidgetsPool(Settings):

    def __init__(self, asyncNumb):
        super().__init__()
        self.asyncNumb = asyncNumb
    
    async def widgetInterestOverTime(self, tid, keywords, timeframe, cookies, proxy, userAgent, timeout):
        tokenPayload = {'hl': self.hl, 'tz': self.tz, 'req': {'comparisonItem': [{'keyword': kw, 'time': timeframe, 'geo': self.geo} for kw in keywords], 'category': self.cat, 'property': self.gprop}}
        tokenPayload['req'] = json.dumps(tokenPayload['req'])
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(self.urls['token'], params=tokenPayload, cookies=cookies, proxy=proxy, timeout=timeout) as res:
                    if await self.status(f'[Widget Pool][TID:{tid}][Remained Widgets:{self.widgetQueryAmount}]', res):
                        res = await res.text()
                        res = json.loads(res[4:])
                        widgets = res['widgets']
                        widget = list(filter(lambda w: w['id']=='TIMESERIES', widgets))[0]
                        storeDict(self.pathWidget, {tid: widget})
                        self.widgetQueryAmount -= 1
                        logging.critical(f'[Widget Pool][TID:{tid}][Remained:{self.widgetQueryAmount}] Stored')
                    else:
                        logging.error(f'[Widget Pool][TID:{tid}][Remained:{self.widgetQueryAmount}] Failed')
        except Exception as e:
            logging.error(f'[Widget Pool][TID:{tid}][Remained:{self.widgetQueryAmount}][Unknown Error] {e}')

    def asyncGetWidget(self, queryKeys, queries, proxies, userAgents, timeout):
        tasks = []
        for tid in queryKeys:
            query = queries[tid]
            keywords = query['tickers']
            timeframe = str(query['periods'][0].date()) + ' ' + str(query['periods'][1].date())
            if '2003' not in timeframe:
                cookies = readCookies(self.pathCookies)
                proxy = proxies[random.randint(0, len(proxies)-1)]
                userAgent = userAgents[random.randint(0, len(userAgents)-1)]
                tasks.append(self.widgetInterestOverTime(tid, keywords, timeframe, cookies, proxy, userAgent, timeout))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))

    def run(self, timeout=TIMEOUT):
        proxies = readProxies(self.pathProxies)
        userAgents = json.load(open(self.pathUserAgents, 'r'))
        queries = pickle.load(open(self.pathQueries, 'rb'))
        skeys = list(queries.keys())
        try:
            dkeys = pickle.load(open(self.pathWidget, 'rb'))
            skeys = list(set(skeys)-set(dkeys))
        except:
            pass
        try:
            dkeys = pickle.load(open(self.pathWidgetReceiveEmptyReponse, 'rb'))
            skeys = list(set(skeys)-set(dkeys))
        except:
            pass
        random.shuffle(skeys)
        self.widgetQueryAmount = len(skeys)
        seqs = int(self.widgetQueryAmount/self.asyncNumb)
        if self.widgetQueryAmount > self.asyncNumb:
            querykeyss = np.array_split(list(skeys), seqs)
        else:
            querykeyss = [skeys]
        for queryKeys in querykeyss:
            self.asyncGetWidget(queryKeys, queries, proxies, userAgents, timeout)

class DataInterestOverTime(Settings):
    def __init__(self, asyncNumb: int):
        super().__init__()
        self.asyncNumb = asyncNumb

    async def getData(self, tid, widget, userAgent: str, proxy: str, cookies: dict, timeout: int):
        queryPayload = {
            'req': json.dumps(widget['request']),
            'token': widget['token'],
            'tz': self.tz
        }
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                session.headers.update({'accept-language': self.hl, 'user-agent': userAgent})
                async with session.get(
                    self.urls['multiline'], params=queryPayload, timeout=timeout, proxy=proxy
                ) as res:
                    if await self.status(f'[Data][TID:{tid}][Remained:{self.queryDataAmount}]', res):
                        res = await res.text()
                        res = json.loads(res[5:])['default']['timelineData']
                        self.queryDataAmount -= 1 
                        popDict(self.pathWidget, tid)
                        key = widget['request']['comparisonItem'][0]['complexKeywordsRestriction']['keyword'][0]['value']
                        if len(res) > 0:
                            storeDict(self.pathDataInterstOverTime, {int(tid): {key: res}})
                            logging.critical(f'[Data][TID:{tid}][Remained:{self.queryDataAmount}] Stored **')
                        else:
                            storeDict(self.pathWidgetReceiveEmptyReponse, {int(tid): widget})
                            logging.error(f'[Data][TID:{tid}][Remained:{self.queryDataAmount}] Empty Set')
        except Exception as e:
            logging.error(f'[Data][TID:{tid}][Remained:{self.queryDataAmount}][Unknown Error] {e}')

    def asyncGetData(self, querykeys, widgets, userAgents, proxies, timeout):
        tasks = []
        for tid in querykeys:
            widget = widgets[tid]
            userAgent = userAgents[random.randint(0, len(userAgents)-1)]
            proxy = proxies[random.randint(0, len(proxies)-1)]
            cookies =readCookies(self.pathCookies)
            tasks.append(self.getData(tid, widget, userAgent, proxy, cookies, timeout))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))

    def run(self, timeout=TIMEOUT):
        userAgents = json.load(open(self.pathUserAgents, 'r'))
        widgets = pickle.load(open(self.pathWidget, 'rb'))
        proxies = readProxies(self.pathProxies)
        skeys = list(widgets.keys())
        try:
            # load & filter the finished tasks
            dkeys = pickle.load(open(self.pathDataInterstOverTime , 'rb'))
            skeys = list(set(skeys)-set(dkeys))
        except:
            pass
        try:
            # load & filter the tasks with empty responses from gooogle. There is not enough searching data for google to form a SVI.
            dkeys = pickle.load(open(self.pathWidgetReceiveEmptyReponse, 'rb'))
            skeys = list(set(skeys)-set(dkeys))
        except:
            pass
        random.shuffle(skeys)
        self.queryDataAmount = len(skeys)
        if self.queryDataAmount > self.asyncNumb:
            seqs = int(self.queryDataAmount/self.asyncNumb)
            querykeyss = np.array_split(list(skeys), seqs)
        else:
            querykeyss = [skeys]
        for querykeys in querykeyss:
            self.asyncGetData(querykeys, widgets, userAgents, proxies, timeout)
