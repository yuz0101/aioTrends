import aioTrends as at
from datetime import date

qry_list = ['AMZN', 'AAPL', 'MSFT']

# running 50 cocurrent tasks
df = at.Aio(50).getScaledDailyData(
    keywords=qry_list, # the query keyword list
    filename='test.csv', # json and pickle are both supported
    start='2004-01-01', # both datetime and str are supported
    end=date.today()
    )

fig = df.plot(figsize=(16,8), title='TEST_SCALED_DAILY_DATA').get_figure()
fig.savefig('test_scaled_daily_data.png')

df_m = at.Aio(50).getMonthlyData(
    keywords=qry_list, 
    start='2004-01-01', 
    end='2022-12-31'
    )
fig = df_m.plot(figsize=(16,8), title='TEST_MONTHLY_DATA').get_figure()
fig.savefig('test_monthly_data.png')