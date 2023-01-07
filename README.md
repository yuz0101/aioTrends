# aioTrends

### Intro.

This is a project for asynchronously obtaining data from google trends in an efficient way. Inspired by [pytrends](https://github.com/GeneralMills/pytrends), I am developing this project with a asynchronous framework, [aiohttp](https://github.com/aio-libs/aiohttp).

The logic behind this project is to firstly build a cookies pool, then obtain and store the tokenized queries (wrapped inside the widgets) in another pool, and lastly retreive the data with widgets from the widget pool.s

Only data of interest over time is tested and avaiable now.

### Pros

- **Saving time** ~ By employing the asynchronous framework of aiohttp, the programme will deal with other requests while waiting for responses from Google Trends, saving the waiting time.
* **Saving repeated requests** ~ Suffering from broken connections and being tired of restarting the requests process? This programme separates the whole process into (1) building a cookies pool, (2) building a widgets pool and (3) retrieving data. The programme can be started from either sub-stage, avoiding sending repeated requests.
+ **Unlimited tasks amount** ~ Tons of queries? The programme will handle that for you automatically.

### Cons

- **Heavily relying on proxies** ~ When running on a large amount of queries, proxies would be required for successfully catching responses. In this context, a small amount of rotating proxies or a large amount of static proxies would be required.
### Setting

Settings can be customized inside the `Settings` Class.

An example input of queries is given under the data folder.

An example of proxies file is given under the proxies folder.

The file userAgents.json is from [Said-Ait-Driss](https://github.com/Said-Ait-Driss/user-agents).

### Getting started

#### Fetch the timeseries data of your keywords

```python
import pickle
import aioTrends as at

#Step 0: Setup the queries file and the log file. Other settings can be customized by amending the init inside the Settings class
queries = {
    0: {'keywords': ['AAPL'], 'periods': '2007-01-01 2007-08-31'},
    1: {'keywords': ['AMZN'], 'periods': 'all'},
    2: {'keywords': ['AAPL', 'AMZN'], 'periods': 'all'}
    }

pickle.dump(open('./data/queries.pkl', 'wb'))
at.setLog('./data/hello.log') #Setp 0: set log file path

#Step 1: collect 10 cookies with two async. threads. Threads can be customized.
at.CookeisPool(2).run(10)

#Step 2: get widgets with three async. threads. Threads can be customized.
at.WidgetsPool(3).run()

#Step 3: get data with five async. threads. Threads can be customized.
at.DataInterestOverTime(5).run() 
```
