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
+ numpy

## Files
Settings can be customized by amending the settings.json under the foler settings.

An example input of queries is given under the data folder.

An example of proxies file is given under the proxies folder.

The file userAgents.json is from [Said-Ait-Driss](https://github.com/Said-Ait-Driss/user-agents).

## Getting started
### I. Initial stage
1. Download the zip file.
2. Amend the settings.json under the folder settings.
3. Paste proxies to the proxies.txt under the folder proxies.
4. Install requirements: run below command line on your terminal or cmd.

```
cd <the path where you unzip and store the files>
pip install -r requirements.txt
```

### II. Setup a queries file

```python
import pickle

queries = {
    0: {'keywords': ['AAPL'], 'periods': '2007-01-01 2007-08-31'},
    1: {'keywords': ['AMZN'], 'periods': 'all'},
    2: {'keywords': ['AAPL', 'AMZN'], 'periods': 'all'},
    .
    .
    .
    10000: {'keywords': ['MSFT'], 'periods': '2004-01-01 2022-12-31'}
    }

pickle.dump(open('./data/queries.pkl', 'wb'))
```

### III. Create a py script named as example.py

```python
import aioTrends as at

#Step 0: Set the log file. Other settings can be customized by amending the settings.json under the folder settings.
at.setLog('./data/hello.log')

#Step 1: collect 1000 cookies with 100 cocurrent tasks. Cocurrent tasks amount can be customized.
at.CookeisPool(100).run(1000)

#Step 2: get widgets with 200 cocurrent tasks. Cocurrent tasks can be customized.
at.WidgetsPool(200).run()

#Step 3: get data with 100 cocurrent tasks. Cocurrent tasks can be customized.
at.DataInterestOverTime(100).run() 
```

### IV. Run the above example.py file on your terminal or cmd

```
python example.py
```