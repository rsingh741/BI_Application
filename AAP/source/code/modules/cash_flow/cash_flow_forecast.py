import os
import calendar
import json
import pickle
import logging
import numpy as np
import pandas as pd
from fbprophet import Prophet
from datetime import datetime
from dateutil import relativedelta
from typing import List, Dict
from functools import lru_cache
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from models.cash_flow_orm import BankTransaction
from modules.cash_flow.view_360  import load_process_data
from mongo_utils import (
                does_collection_exist, 
                drop_collection, 
                create_collection_and_insert_datafrme, 
                get_dataframe_collection)

PATH = os.path.join(os.path.dirname( __file__ ), 'pickle_data/')

def weekly_agg_cash_blance(client_id):

    df = load_process_data(client_id)
    start_date, end_date = df.date.min(), df.date.max()
    weekly_df = df.groupby(['year','week'], as_index=False).agg({'debit':sum, 'credit':sum}). \
            rename(columns={'debit':'inflow', 'credit':'outflow'})
    weekly_df['cum_inflow'] = weekly_df['inflow'].cumsum()
    weekly_df['cum_outflow'] = weekly_df['outflow'].cumsum()
    weekly_df['cash_balance'] = weekly_df['cum_inflow'] - weekly_df['cum_outflow']
    weekly_df = weekly_df.sort_values(['year', 'week'])
    weekly_df['date'] = pd.date_range(start_date, end_date, freq='W-SUN')
    
    return weekly_df[['date', 'cash_balance']]


def weekly_cash_blance_data(client_id):
    
    df = load_process_data(client_id)
    df = df[['date', 'debit', 'credit']].rename(columns={'debit':'inflow', 'credit':'outflow'})
    df['cum_inflow'] = df['inflow'].cumsum()
    df['cum_outflow'] = df['outflow'].cumsum()
    df['cash_balance'] = df['cum_inflow'] - df['cum_outflow']
    logic = {"cash_balance":'mean'}
    offset = pd.offsets.timedelta(days=0)
    weekly_balance = df[['date', 'cash_balance']]. \
                            set_index('date'). \
                            resample('W', loffset=offset).apply(logic).reset_index()
    return weekly_balance


def make_forecast(client_id, periods=52):
    
    #weekly_balance = weekly_cash_blance_data(client_id)    
    weekly_balance = weekly_agg_cash_blance(client_id)
    pkl_path = PATH + f'{client_id}_cash_flow_propeht_model.plk'
    #for prophet requirement date column name ds and data column name y
    weekly_ts = weekly_balance.rename(columns={'date':'ds', 'cash_balance':'y'})
    #print(daily_ts.head(4))
    
    m = Prophet(daily_seasonality=False, weekly_seasonality=True, yearly_seasonality=True)
    m.add_country_holidays(country_name='IN')
    m.fit(weekly_ts)
    future = m.make_future_dataframe(periods=periods, freq='W')    
    forecast_df = m.predict(future)
    forecast_df = forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    #print(forecast.head())
    
    data_name = 'cash_flow_weekly_forecast_data'
    if does_collection_exist(data_name, client_id):
        drop_collection(data_name, client_id) 
        
    create_collection_and_insert_datafrme(data_name, forecast_df, client_id)           
    
    with open(pkl_path, "wb") as f:    
        pickle.dump(m, f)
    logging.info(f'timeseries sales prediction model and data Mongo collection for \
                 {client_id} in sales_forcast created')
    
    
def load_propet_forecast_data_n_model(client_id):
    
    pkl_path = PATH + f'{client_id}_cash_flow_propeht_model.plk'
    data_name = 'cash_flow_weekly_forecast_data'
    if not os.path.exists(pkl_path) or not does_collection_exist(data_name, client_id):
        make_forecast(client_id, periods=52)  
            
    forecast_df = get_dataframe_collection(data_name, client_id)    
    with open(pkl_path, 'rb') as f:
        model = pickle.load(f)

    return  forecast_df, model


def get_monthly_prediction(client_id:str)->List[dict]:
    
    """
    input: client_id
    output: [{'month': 'April-2019', 'prediction': 122706089.0},
             {'month': 'August-2019', 'prediction': 142604681.0},..]
    """
    
    fcast, m = load_propet_forecast_data_n_model(client_id)
    
    last_date = m.history['ds'].values[-1].astype(str)[:10]
    fcast = (fcast[fcast['ds'] > last_date][['ds', 'yhat', 'yhat_lower', 'yhat_upper']]).copy() 
    
    monthly_fcast = fcast.groupby(fcast['ds'].dt.strftime('%B-%Y'))['yhat'].sum().reset_index(). \
                    rename(columns={'ds':'month', 'yhat':'prediction'}). \
                    sort_values('month')
    
    return monthly_fcast.round().to_dict(orient='records')


def get_hist_and_pred_data(client_id:str)->List[dict]:
    """
    Input:client_id
    Output:  [{
    'history': [{"ds": "02-04-2017", "y": 4235864.0}, {"ds": "09-04-2017", "y": 7148271.0}, ...]                  
    'prediction':[{"ds": "07-04-2019", "yhat": 16096249.7, "yhat_lower": -29488728.8, "yhat_upper": 62031373.1}, 
            {"ds": "14-04-2019", "yhat": 22947831.91, "yhat_lower": -22338663.8, "yhat_upper": 73050198.39}, ..]
        }]
    """
    
    fcast, m = load_propet_forecast_data_n_model(client_id)
    
    last_date = m.history['ds'].values[-1].astype(str)[:10]
    fcast = (fcast[fcast['ds'] > last_date][['ds', 'yhat', 'yhat_lower', 'yhat_upper']]).copy() 
        
    hist = m.history[['ds', 'y']].copy()    
    fcast['ds'] = fcast['ds'].map(lambda x: x.strftime('%d-%m-%Y'))
    hist['ds'] = hist['ds'].map(lambda x: x.strftime('%d-%m-%Y'))
    
    fcast, hist = fcast.round(2), hist.round(2)
    #return fcast, hist
    return [{"prediction": fcast.to_dict(orient='records') , 
             "history": hist.to_dict(orient='records')}]
    
    
def process_forecast(client_id):
    try:
        load_propet_forecast_data_n_model(client_id)       
    except Exception as e:
        logging.error(f"while creating timeseries sales prediction model for {client_id} in sales_forcast{e}")        
    logging.info(f'timeseries sales prediction model and data Mongo collection for {client_id} in sales_forcast exists')
    pkl_path = PATH + f'{client_id}_cash_flow_propeht_model.plk'
    data_name = 'cash_flow_weekly_forecast_data'
    return (os.path.exists(pkl_path) and does_collection_exist(data_name, client_id))    
    
    
def test_fcast(client_id):
    
    print(get_monthly_prediction(client_id)[:2])
    print(get_hist_and_pred_data(client_id)['prediction'][:2])
    
    
if __name__=='__main__':
    
    test_fcast('1')