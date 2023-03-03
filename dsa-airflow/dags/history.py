from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import yaml
import os
from datetime import datetime
import json
import dateutil
import numpy as np


# functions to be used for transformations
#---------------------------------------------------
def inv_string_to_date(col):
    return datetime.strptime(col, '%b %d, %Y')

def old_string_to_date(col):
    return datetime.strptime(col, '%Y-%m-%d')

def inv_clean_price(col):
    return round(float(col.replace(',','')),2)

def old_clean_price(col):
    return round(col,2)

def inv_clean_vol(row):
    if 'K' in row.volume:
        return (float(row.volume.replace('.','').replace('K',''))*1000*float(row.close))
    elif 'M' in row.volume:
        return (float(row.volume.replace('.','').replace('M',''))*1000000*float(row.close))
    elif 'B' in row.volume:
        return (float(row.volume.replace('.','').replace('B',''))*1000000000*float(row.close))


def hist_transf():
    # cleaning and transformations for historical data from kaggle
    #---------------------------------------------------
    old_price_df = pd.read_csv('./data/BTC_4_13_2021.csv', header=0)

    old_old_names = ['Date','Open','High','Low','Close','Adj Close','Volume']
    old_new_names = ['date','open','high','low','close','adj_close','volume']

    old_rename_dict = {name[0]:name[1] for name in zip(old_old_names,old_new_names)}

    old_price_df = old_price_df.rename(columns=old_rename_dict)

    old_price_df = old_price_df[['date', 'open', 'close', 'volume']]

    old_price_df['date'] = old_price_df['date'].map(old_string_to_date)
    old_price_df.insert(1, 'symbol', 'BTC')
    old_price_df['open'] = old_price_df['open'].map(old_clean_price)
    old_price_df['close'] = old_price_df['close'].map(old_clean_price)
    old_price_df['volume'] = old_price_df['volume'].map(lambda v: float(v))

    # cleaning and transformations for historical data from investing.com
    #---------------------------------------------------

    invest_df = pd.read_csv('./data/Bitcoin Historical Data - Investing.com.csv', header=0)

    inv_old_names = ["Date","Price","Open","High","Low","Vol.","Change %"]
    inv_new_names = ['date', 'close', 'open','high','low','volume', 'chge_percent']

    inv_rename_dict = {name[0]:name[1] for name in zip(inv_old_names,inv_new_names)}

    invest_df = invest_df.rename(columns=inv_rename_dict)

    invest_df = invest_df[['date', 'open', 'close', 'volume']]

    invest_df['date'] = invest_df['date'].map(inv_string_to_date)
    invest_df.insert(1, 'symbol', 'BTC')
    invest_df['open'] = invest_df['open'].map(inv_clean_price)
    invest_df['close'] = invest_df['close'].map(inv_clean_price)
    invest_df['volume'] = invest_df.apply(inv_clean_vol, axis=1)

    # consolidate historical data into one csv
    #---------------------------------------------------
    hist_df = pd.concat([old_price_df,invest_df])

    hist_df.set_index('date', inplace=True)

    file_path = './data/combined_BTC_hist_pricing.csv'

    if os.path.exists(file_path):
        pass
    else:
        hist_df.to_csv(file_path, header=True)