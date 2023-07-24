
import os
import pickle
from datetime import date, datetime, timedelta

from pandas.tseries.offsets import DateOffset
from pandas import to_datetime

from aioTrends.HandleExceptions import InputError


def _formQueries(tid: str, keyword: list, start='2004-01-01', end='2022-12-31', freq: str='D') -> dict:
    if type(start) == str:
        try:
            start = date.fromisoformat(start)
        except:
            raise InputError(
                "arg of start should be a string, date or datetime, e.g., '2012-01-01'"
            )
    
    if type(end) == str:
        try:
            end = date.fromisoformat(end)
        except:
            raise InputError(
                "arg of end should be a string, date or datetime, e.g., '2022-12-28'"
            )
    
    return {tid: {'keywords': [keyword], 'periods': f'{start} {end}', 'freq': freq}}

def _handlePeriods(start: date, end: date, freq: str) -> list:
    periods = []
    if freq.lower() == 'd':
        if (end - start).days > 3*30:
            for _ in range((end - start).days // 180):
                periods.append([start, start+timedelta(days=180)])
                start += timedelta(days=181)
            rs = (end - start).days % 180
            periods.append([start, start+timedelta(days=rs)])
            return periods
        else:
            return [[start, end]]
    
    elif freq.lower() == 'm':
        return [[date.fromisoformat('2004-01-01'), date.today()]]

    else:
        raise InputError(
            "The arg of freq should be either 'D' for daily, or 'M' for 'monthly' frequency."
        )

def formQueries(keywords: list, start='2004-01-01', end=date.today(), freq: str='D') -> dict:
    """This is a function for forming queries.

    Args:
        keywords (list): The searching keywords in a list. E.g., ['MSFT', 'TSLA', 'DJIA', 'SPX']
        start (str, optional): The start date of searching periods. Defaults to '2004-01-01'.
            E.g., '2012-01-01'. Defaults to '2004-01-01', the start date of google trends data lib..
        end (_type_, optional): The end date of searching periods. Defaults to date.today()'.
            E.g., '2022-12-31'. Defaults to date.today().
        freq (str, optional): The required frequency of data. Defaults to 'D'.
            E.g., 'D' for daily, 'M' for monthly.

    Raises:
        InputError: Input errors of args of start, end or freq.

    Returns:
        dict: A dict of query tasks with task id, searching keywords and periods.
    """

    if type(start) == str:
        try:
            start = date.fromisoformat(start)
        except:
            raise InputError(
                "arg of start should be a string, date or datetime, e.g., '2012-01-01'"
            )
    
    if type(end) == str:
        try:
            end = date.fromisoformat(end)
        except:
            raise InputError(
                "arg of end should be a string, date or datetime, e.g., '2022-12-28'"
            )
    
    if freq.lower() in ['d', 'day', 'daily']:
        freq = 'd'
    elif freq.lower() in ['m', 'month', 'monthly']:
        freq = 'm'
    else:
        raise InputError(
            "The arg of freq should be daily or monthly"
        )

    periods = _handlePeriods(start, end, freq)
    queries = {}
    i = 0
    for keyword in keywords:
        for period in periods:
            queries.update(_formQueries(i, keyword, period[0], period[1], freq))
            i += 1
    return queries

def collectRawData(path: str='./data/IOT/') -> dict:
    ps = os.listdir(path)
    dii = {}
    for p in ps:
        with open(os.path.join(path, p), 'rb') as f:
            d = pickle.load(f)
        di = {}
        for i in d['data']:
            di.update({i['time']: i['value'][0]})
        
        if d['keywords'] in dii.keys():
            dii[d['keywords']].append(di)
        else:
            dii.update({d['keywords']: [di]})
    return dii

def delAllFiles(path):
    for f in os.listdir(path):
        os.remove(os.path.join(path, f))

def scaledDailyData(daily, monthly):
    samples = {}
    for keyword in list(daily.keys()):
        sample = {}
        for samp in daily[keyword]:
            dt = max(samp, key=samp.get)
            dt = to_datetime(dt, unit='s').date()
            dt += DateOffset(day=1)
            dt = int(dt.timestamp())
            factor = monthly[keyword][0][str(dt)]
            samp.update((key, value/100*factor) for key, value in samp.items())
            sample |= samp
        samples.update({keyword: sample})
    return samples


