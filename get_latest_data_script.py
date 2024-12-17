# for loading .env file
from dotenv import load_dotenv
import os
# for API call
import requests
# for dealing with polygon api get results
import pandas as pd
import csv
import json
# for getting opening dates for stock markets
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
from interested_markets import markets
from pytz import timezone
# for writing to database
from import_script import import_to_postgresql

# function to load values from .env file
def configure():
    load_dotenv()

# getting the latest market closed date for NYSE
def getDate(market):
    if type(market) == str:
        try:
            market_calendar = mcal.get_calendar(market)
            start = datetime.now(timezone('UTC')) - timedelta(days=14)
            end = datetime.now(timezone('UTC')) - timedelta(days=1)
            dates_df = market_calendar.schedule(start_date=start, end_date=end)
            dates_df_sorted = dates_df.sort_values(by=['market_open'], ascending=False)
            latest_timestamp = dates_df_sorted['market_open'].iloc[1]
            latest_date = latest_timestamp.date()
            return latest_date
        except:
            return print(f'{market} is not a valid stock ticker')


# getting data for NYSE stocks on latest date
def getDayData(market, datevalue):
    configure()
    try:
        baseurl = 'https://api.polygon.io/'
        groupedDaily = 'v2/aggs/grouped/locale/us/market/stocks/' + str(datevalue)
        apiKey = os.getenv('polygonApiKey')
        url = f'{baseurl}{groupedDaily}?apiKey={apiKey}'
        try:
            response = requests.get(url)
            if response.status_code == 200:
                print(f'API call success, code: {response.status_code}')
            latest_data = response.json()
            latest_data_results = latest_data['results'][0:-1]
            latest_data_results_df = pd.DataFrame(latest_data_results).rename(columns={
                "":"number",
                'T':"ticker",
                "v":"volume",
                "vw":"volume_weighted_avg_price",
                "o":"open_price",
                "c":"close_price",
                "h":"highest_price",
                "l":"lowest_price",
                "t":"um_timestamp",
                "n":"transactions"})
                # creating column in day_data to track creation date
            latest_data_results_df['market_close'] = datevalue
            latest_data_results_df['market'] = market
            print(f'pandas dataframe for {datevalue} created')
            return latest_data_results_df
        except:
            return print(f'API call failed, error code {response.status_code}')
    except:
        return print(f'{datevalue} is not valid, please input value of type "datetime"')

# make new directory in stock_market_ELT to store created CSVs
os.makedirs('csvs', exist_ok=True)

# loop through interested markets and fetch data for each market
for market in markets:
    print(market)
    latest_date = getDate(market.upper())
    print(latest_date)
    day_data = getDayData(market, latest_date)
    
    #importing data to postgres
    import_to_postgresql(market, day_data)
    
    #creating a csv (optional)
    day_data.to_csv(f'csvs/{market.lower()}_latest_{latest_date}.csv', index=False)
    print(f'{market.lower()}_latest_{latest_date}.csv created')
