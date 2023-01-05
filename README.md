# aioTrends

## Intro.

This is a project for asynchronously obtaining data from google trends in an efficient way. Inspired by pytrends, I am developing this project with a asynchronous framework, aiohttp. 

The logic behind this project is to firstly build a cookies pool, then obtain the tokenized queries (wrapped inside the widget) and lastly retreive the data with widgets. 


## Setting

Settings can be customized inside the `Settings` Class

## Coding Example

```
import aioTrends as at

at.setLog('./data/hello.log') #Setp 0: set log file path
at.CookeisPool(2).run(100) #Step 1: collect 100 cookies with two async. threads
at.WidgetsPool(3).run() #Step 2: get widgets with three async. threads
at.DataInterestOverTime(5).run() #Step 3: get data with five async. threads
```