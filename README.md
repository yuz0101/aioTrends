# aioTrends

## Intro.

This is a project for asynchronously obtaining data from google trends in an efficient way. Inspired by [pytrends](https://github.com/GeneralMills/pytrends), I am developing this project with a asynchronous framework, [aiohttp](https://github.com/aio-libs/aiohttp).

The logic behind this project is to firstly build a cookies pool, then obtain and store the tokenized queries (wrapped inside the widgets) in another pool, and lastly retreive the data with widgets from the widget pool.

Only data of interest over time is tested and avaiable now.

## Setting

Settings can be customized inside the `Settings` Class.

An example input of queries is given under the data folder.

An example of proxies file is given under the proxies folder.

The file userAgents.json is from [Said-Ait-Driss](https://github.com/Said-Ait-Driss/user-agents).

## Start to fetch the timeseries data of your keywords

```
import pickle
import aioTrends as at

#Step 0: Setup the queries file and the log file. Other settings can be customized by amending the init inside the Settings class
queries = {
    'keywords': ['AAPL'],
    'periods': '2007-01-01 2007-08-31'
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

