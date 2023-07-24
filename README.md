# aioTrends

## Intro.

This is a project for asynchronously obtaining data from google trends in an efficient way. Inspired by [pytrends](https://github.com/GeneralMills/pytrends), I am developing this project based on a asynchronous framework, asyncio, and a related module, [aiohttp](https://github.com/aio-libs/aiohttp).

The logic behind this project is to firstly build a cookies pool, then obtain and store the tokenized queries (wrapped inside the widgets) in another pool, and lastly retreive the data with widgets from the widget pool.

Only data of interest over time is tested and avaiable now.

## Pros & Cons
### Pros
- **Saving time** ~ By employing the asynchronous framework, the programme will deal with other requests while waiting for responses from Google Trends, saving the waiting time.
* **Saving repeated requests** ~ Suffering from broken connections and being tired of restarting the requests process? This programme separates the whole process into (1) building a cookies pool, (2) building a widgets pool and (3) retrieving data. The programme can be started from either sub-stage, avoiding sending repeated requests.
+ **Unlimited tasks amount** ~ Tons of queries? The programme will handle that for you automatically.

### Cons
- **Heavily relying on proxies** ~ When running on a large amount of queries, proxies would be required for successfully catching responses. In this context, a small amount of rotating proxies or a large amount of static proxies would be required.
+ **Only timeseries interest data is avaiable now** ~ Will test others in the future.

## Requirements
- python >= 3.10
* aiohttp
* aiofiles
* numpy
+ pandas

## Files
Settings can be customized by amending the settings.json under the foler settings.

An example input of queries is given under the data folder.

An example of proxies file is given under the proxies folder.

The file userAgents.json is from [Said-Ait-Driss](https://github.com/Said-Ait-Driss/user-agents).

## Before Start
### I. Initial stage
1. Install [Python version at least 3.10](https://www.python.org/downloads/) if you don't have one, I use python 3.11 in this example
2. Install package via pip command line. (On macOS's terminal or WindowsOS's CMD)
```consol
pip install aioTrends
pip install virtualenv
```
3. Create a virtual environment, named as atenv, for running python3.11 without affecting your other setups.
```consol
where python3.11
```
copy the path to python 3.11 and replace below path
```consol
virtualenv -p /path/to/python3.11 atenv
```
4. Activate the virtual environment
On Windows:
```consol
atenv\Scripts\activate
```
On macOS and Linux:
```consol
source atenv/bin/activate
```
5. Install aioTrends********
the package must be installed under the environment of python 3.10+  
```consol
pip install aioTrends
```

6. Checking if installed properly. The programme will creat folders, please follow the instructions given by the programme.
```consol
cd path/to/your/working/path
python
import aioTrends as at
```
7. Amend the settings.json under the folder settings.
8. Paste proxies to the proxies.txt under the folder proxies.
9. Get userAgents.json file from [Said-Ait-Driss](https://github.com/Said-Ait-Driss/user-agents) and past it under

## Getting Started
### II. Setup a queries file

```python
import pickle
qrys = {
    0: {'keywords': ['AAPL'], 'periods': '2007-01-01 2007-08-31', 'freq': 'D'},
    1: {'keywords': ['AMZN'], 'periods': 'all', 'freq': 'M'},
    2: {'keywords': ['AAPL', 'AMZN'], 'periods': 'all', 'freq': 'M'},
    .
    .
    .
    10000: {'keywords': ['MSFT'], 'periods': '2004-01-01 2022-12-31', 'freq': 'M'}
    }

pickle.dump(qrys, open('./data/qrys.pkl', 'wb'))
```

Alternatively, function ```formQueries``` would form the query dataset based on the list of keywords you give.
```python
from aioTrends import formQueries

qrys = formQueries(keywords: ['AMZN', 'MSFN'], start='2004-01-01', end=date.today(), freq: str='D')
pickle.dump(qrys, open('./data/qrys.pkl', 'wb'))
```

### III. Create a py script named as example.py

```python
import aioTrends as at

#Step 0: Set the log file. Other settings can be customized by amending the settings.json under the folder settings.
at.setLog('./data/hello.log')

#Step 1: collect 1000 cookies with 100 cocurrent tasks. Cocurrent tasks amount can be customized.
at.CookeisPool(100).run(1000)

#Step 2: get widgets with 100 cocurrent tasks. Cocurrent tasks can be customized.
at.WidgetsPool(100).run()

#Step 3: get data with 100 cocurrent tasks. Cocurrent tasks can be customized.
at.DataInterestOverTime(100).run()
```

Alternatively, you can use below one line for forming queries and getting daily scaled data or monthly data.
```python

import aioTrends as at
from datetime import date

qry_list = ['AMZN', 'MSFN']

# running 100 cocurrent tasks
df = at.Aio(100).getScaledDailyData(
    keywords=qry_list, # the query keyword list
    filename='output.csv', # json and pickle are both supported
    start='2004-01-01', # both datetime and str are supported
    end=date.today()
    )
print(df)

df_m = at.Aio(50).getMonthlyData(keywords=qry_list, start='2004-01-01', end='2022-12-31')
print(df_m)
```

### IV. Run the above example.py file on your terminal or cmd (The code need to be running under the python 3.10+ environment)

```consol
python example.py
```

