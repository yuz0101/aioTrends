
import os
import pickle
from datetime import date, datetime, timedelta

from pandas.tseries.offsets import DateOffset
from pandas import to_datetime

from aioTrends.HandleExceptions import InputError


def _formQueries(tid: str, keyword: list, start='2004-01-01', end='2022-12-31', freq: str='D') -> dict:
    '''The function `_formQueries` takes in a task ID, a list of keywords, a start date, an end date, and a
    frequency, and returns a dictionary containing the task ID, keywords, periods, and frequency.
    
    Parameters
    ----------
    tid : str
        The `tid` parameter is a string that represents the unique identifier for the query.
    keyword : list
        The `keyword` parameter is a list of keywords. It represents the keywords that you want to use for
    querying data.
    start, optional
        The `start` parameter is the start date of the period for which you want to form queries. It is a
    string representing a date in the format 'YYYY-MM-DD'. The default value is '2004-01-01'.
    end, optional
        The `end` parameter is a string representing the end date of the period for which the queries will
    be formed. It has a default value of '2022-12-31'.
    freq : str, optional
        The `freq` parameter is a string that specifies the frequency of the data. It has a default value
    of 'D', which stands for daily frequency. Other possible values for `freq` could be 'W' for weekly
    frequency, 'M' for monthly frequency, 'Q' for quarterly frequency
    
    Returns
    -------
        a dictionary with the following structure:
    
    '''
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
    '''The `_handlePeriods` function takes in a start date, end date, and frequency, and returns a list of
    periods based on the given frequency.
    
    Parameters
    ----------
    start : date
        The start date of the period. It should be a date object.
    end : date
        The `end` parameter represents the end date of the period for which you want to generate periods.
    freq : str
        The `freq` parameter is a string that represents the frequency of the periods. It can be either 'D'
    for daily or 'M' for monthly frequency.
    
    Returns
    -------
        a list of periods. Each period is represented as a list containing two elements: the start date and
    the end date.
    
    '''
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
    '''The function `collectRawData` collects raw data from files in a specified directory and organizes it
    into a dictionary based on keywords.
    
    Parameters
    ----------
    path : str, optional
        The `path` parameter is a string that represents the directory path where the raw data files are
    located. By default, it is set to `'./data/IOT/'`, which means that the function will look for the
    raw data files in the `./data/IOT/` directory.
    
    Returns
    -------
        The function `collectRawData` returns a dictionary `dii` which contains the collected raw data.
    
    '''
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
    '''The function `delAllFiles` deletes all files in a given directory.
    
    Parameters
    ----------
    path
        The path parameter is the directory path where the files are located.
    
    '''
    for f in os.listdir(path):
        os.remove(os.path.join(path, f))

def scaledDailyData(daily, monthly):
    '''The function `scaledDailyData` takes in two dictionaries, `daily` and `monthly`, and returns a new
    dictionary where the values in `daily` are scaled based on the corresponding values in `monthly`.
    
    Parameters
    ----------
    daily
        The "daily" parameter is a dictionary where the keys are keywords and the values are lists of
    dictionaries. Each dictionary in the list represents a daily sample and contains data for different
    variables. The keys in each dictionary represent the variables and the values represent the
    corresponding values for that variable on that day.
    monthly
        The "monthly" parameter is a dictionary that contains monthly data for each keyword. Each keyword
    is a key in the dictionary, and the corresponding value is a list of dictionaries. Each dictionary
    in the list represents a specific month, with the keys being the timestamps (in seconds) and the
    values being the
    
    Returns
    -------
        a dictionary of scaled daily data. Each keyword in the input dictionary `daily` is used as a key in
    the output dictionary `samples`. The corresponding value for each keyword is a dictionary of scaled
    samples.
    
    '''
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
