from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import yaml
import os
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import json
import dateutil
import numpy as np

crypto = {
    'yahoo' : 'https://finance.yahoo.com/crypto/',
}

def scrape_yahoo():
    data = requests.get(crypto['yahoo']).text
    soup = BeautifulSoup(data,'html.parser')

    # find tables on the wiki page
    for table in soup.find_all('table'):
        print(table.get('class'))

    tables = soup.find_all('table')
    table = soup.find('table', class_='W(100%)')

    col_to_scrape=[
        'Symbol', 
        'Name', 
        'Price (Intraday)', 
        'Change', 
        '% Change', 
        'Market Cap', 
        'Volume in Currency (Since 0:00 UTC)', 
        'Volume in Currency (24Hr)',
        'Total Volume All Currencies (24Hr))',
        'Circulating Supply',
        'created_at'
    ]

    # create empty dataframe with column names
    df = pd.DataFrame(columns=col_to_scrape)

    #scrape data from site
    for row in table.tbody.find_all('tr'):
        # Find all data for each column
        columns = row.find_all('td')
        
        if(columns != []):
            sym = columns[0].text.strip()
            name = columns[1].text.strip()
            price = columns[2].text.strip()
            chge = columns[3].text.strip()
            chge_percent = columns[4].text.strip()
            mkt_cap = columns[5].text.strip()
            vol_utc = columns[6].text.strip()
            vol_24hr = columns[7].text.strip()
            total_vol = columns[8].text.strip()
            cir_supply = columns[9].text.strip()

            scraped_values = [
                sym,
                name, 
                price, 
                chge, 
                chge_percent,
                mkt_cap,
                vol_utc,
                vol_24hr,
                total_vol,
                cir_supply,
                datetime.now()
                ]

            df = df.append({item[0]: item[1] for item in zip(col_to_scrape,scraped_values)}, ignore_index=True)

    df.set_index('Symbol', inplace=True)
    
    #if os.path.isfile('./data/yahoo.csv'):  
        #df.to_csv('./data/raw_yahoo.csv', header=False, mode='a')
    #else:
    df.to_csv('./data/raw_yahoo.csv', header=True)


def symbol_clean(col):
    col_ls = col.split("-")
    sym = col_ls[0].strip()
    return sym

def open_raw_yahoo_transform():
    raw_y_df = pd.read_csv('./data/raw_yahoo.csv', header=0)

    raw_old_names = [
        'Symbol', 
        'Name', 
        'Price (Intraday)', 
        'Change', 
        '% Change', 
        'Market Cap', 
        'Volume in Currency (Since 0:00 UTC)', 
        'Volume in Currency (24Hr)',
        'Total Volume All Currencies (24Hr))',
        'Circulating Supply',
        'created_at'
    ]

    raw_new_names = [
        'symbol',
        'name',
        'open_price',
        'change',
        'change_percent',
        'market_cap',
        'volume_utc',
        'volume_24h',
        'total_volume',
        'circulating_supply',
        'open_created_at'
    ]

    raw_rename_dict = {name[0]:name[1] for name in zip(raw_old_names,raw_new_names)}

    raw_y_df = raw_y_df.rename(columns=raw_rename_dict)

    raw_y_df = raw_y_df[['symbol','open_price','volume_24h','open_created_at']][raw_y_df['symbol'] == 'BTC-USD']

    raw_y_df['date'] = raw_y_df['open_created_at'].map(lambda d: datetime.date(pd.to_datetime(d)))
    raw_y_df['open_price'] = raw_y_df['open_price'].map(lambda v: round(float(v.replace(',','')),2))
    raw_y_df['close_price'] = pd.NA
    raw_y_df['symbol'] = raw_y_df['symbol'].map(symbol_clean)
    raw_y_df['volume_24h'] = pd.NA
    raw_y_df['open_created_at'] = raw_y_df['open_created_at'].map(lambda d: pd.to_datetime(d))
    raw_y_df['close_created_at'] = pd.to_datetime(pd.NA)

    raw_y_df = raw_y_df[['date','symbol', 'open_price', 'close_price','volume_24h', 'open_created_at', 'close_created_at']]

    raw_y_df.to_csv('./data/stg_data.csv', header=True)


def raw_clean_vol(row):
    if 'K' in row.volume_24h:
        return (float(row.volume_24h.replace('.','').replace('K',''))*1000*float(row.close_price))
    elif 'M' in row.volume_24h:
        return (float(row.volume_24h.replace('.','').replace('M',''))*1000000*float(row.close_price))
    elif 'B' in row.volume_24h:
        return (float(row.volume_24h.replace('.','').replace('B',''))*1000000000*float(row.close_price))


def close_raw_yahoo_transform():
    raw_y_df = pd.read_csv('./data/raw_yahoo.csv', header=0)

    raw_old_names = [
        'Symbol', 
        'Name', 
        'Price (Intraday)', 
        'Change', 
        '% Change', 
        'Market Cap', 
        'Volume in Currency (Since 0:00 UTC)', 
        'Volume in Currency (24Hr)',
        'Total Volume All Currencies (24Hr))',
        'Circulating Supply',
        'created_at'
    ]

    raw_new_names = [
        'symbol',
        'name',
        'close_price',
        'change',
        'change_percent',
        'market_cap',
        'volume_utc',
        'volume_24h',
        'total_volume',
        'circulating_supply',
        'close_created_at'
    ]

    raw_rename_dict = {name[0]:name[1] for name in zip(raw_old_names,raw_new_names)}

    raw_y_df = raw_y_df.rename(columns=raw_rename_dict)

    raw_y_df = raw_y_df[['symbol','close_price','volume_24h','close_created_at']][raw_y_df['symbol'] == 'BTC-USD']

    raw_y_df['date'] = raw_y_df['close_created_at'].map(lambda d: datetime.date(pd.to_datetime(d)))
    raw_y_df['close_price'] = raw_y_df['close_price'].map(lambda v: round(float(v.replace(',','')),2))
    raw_y_df['open_price'] = pd.NA
    raw_y_df['symbol'] = raw_y_df['symbol'].map(symbol_clean)
    raw_y_df['volume_24h'] = raw_y_df.apply(raw_clean_vol, axis=1)
    raw_y_df['close_created_at'] = raw_y_df['close_created_at'].map(lambda d: pd.to_datetime(d))
    raw_y_df['open_created_at'] = pd.to_datetime(pd.NA)

    raw_y_df = raw_y_df[['date','symbol', 'open_price', 'close_price','volume_24h', 'open_created_at', 'close_created_at']]

    raw_y_df.to_csv('./data/stg_data.csv', header=False, mode='a')


def stg_file_setup():
    df = pd.read_csv('./data/stg_data.csv', header=0)

    df = df.replace('NAN',np.nan) #If necessary
    df = df.groupby(['date','symbol'], as_index=False).first()
    
    df.set_index(['date'], inplace=True)

    df = df[['symbol', 'open_price', 'close_price', 'volume_24h']]

    old_names = ['symbol', 'open_price', 'close_price', 'volume_24h']
    new_names = ['symbol', 'open', 'close', 'volume']

    rename_dict = {name[0]:name[1] for name in zip(old_names,new_names)}

    df = df.rename(columns=rename_dict)

    # need to finish up appending this to the historical table
    return df.head()