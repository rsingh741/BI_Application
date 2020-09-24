import numpy as np
import pandas as pd
import calendar
import time
import os
import pickle
import warnings
import logging

from fbprophet import Prophet
from datetime import datetime
from dateutil import relativedelta


from modules.retail.sales_visualization import get_daily_sales_data
from models.retail import ItemMaster as item_tabls
from models.retail import MSalesDetailTxns as sales_table
#from models.retail import download_columns_from_db

from mongo_utils import create_collection_and_insert_datafrme
from mongo_utils import get_dataframe_collection
from mongo_utils import does_collection_exist, drop_collection


#PATH = os.path.join(os.path.dirname(os.path.dirname( __file__ )), 'pickle_data/')
PATH = os.path.join(os.path.dirname( __file__ ), 'pickle_data/')
logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

def make_forecast(client_id, periods):
    
    df = get_daily_sales_data(client_id)    
   
    pkl_path = PATH + f'{client_id}_propeht_model.plk'
    #for prophet requirement date column name ds and data column name y
    daily_ts = df[['date', 'Total_sales']].rename(columns={'date':'ds', 'Total_sales':'y'})
    #print(daily_ts.head(4))
    
    m = Prophet(daily_seasonality=False)
    m.add_country_holidays(country_name='US')
    m.fit(daily_ts)
    future = m.make_future_dataframe(periods=periods)    
    forecast_df = m.predict(future)
    forecast_df = forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    #print(forecast.head())
    
    data_name = 'forecast_data'
    if does_collection_exist(data_name, client_id):
        drop_collection(data_name, client_id) 
        
    create_collection_and_insert_datafrme(data_name, forecast_df, client_id)           
    
    with open(pkl_path, "wb") as f:    
        pickle.dump(m, f)
    logging.info(f'timeseries sales prediction model and data Mongo collection for {client_id} in sales_forcast created') 
        
def load_propet_forecast_data_n_model(client_id):
    
    pkl_path = PATH + f'{client_id}_propeht_model.plk'
    data_name = 'forecast_data'
    if not os.path.exists(pkl_path) or not does_collection_exist(data_name, client_id):
        make_forecast(client_id, periods=360)  
            
    forecast_df = get_dataframe_collection(data_name, client_id)    
    with open(pkl_path, 'rb') as f:
        model = pickle.load(f)

    return  forecast_df, model

def get_next_sevedays_pred_data(client_id):
    """
    Input: fcast => dataframe from fbprophet forecast
    
    Output: seven days daily data in json
                [{'ds': '10-05-2019', 'prediction': 9208.75,  'lower': 6446.99,  'upper': 11969.76},
    """
    fcast, m = load_propet_forecast_data_n_model(client_id)
    
    cur_dt = datetime.today().strftime('%Y-%m-%d')   
    df = fcast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
    
    date_list = df['ds'].dt.strftime('%Y-%m-%d').tolist()
    #print(date_list[-8:], date_list[-8])
    if cur_dt not in date_list:
        #print('cur_dt is not in')
        cur_dt = date_list[-8]
    print(cur_dt)   
    df = df[df['ds'] > cur_dt].head(7)
    df['ds'] = df['ds'].map(lambda x: x.strftime('%d-%m-%Y'))
    df = df.rename(columns={'yhat_lower':'lower', 'yhat':'prediction', 'yhat_upper':'upper'})
    df = df.round()
    return df.to_dict(orient='record')

def get_current_next_month_pred_data(client_id):
    """
    Input: fcast => dataframe from fbprophet forecast
    
    Output: seven days daily data in json
            [{'lower': 5062.49, 'month_year': 'June-2019',  'prediction': 7922.49,  'upper': 10846.83},..]
    
    """    
    fcast, m = load_propet_forecast_data_n_model(client_id)
    curr_date = datetime.today()
    curr_month = curr_date.strftime('%B-%Y')
    next_month = (curr_date + relativedelta.relativedelta(months=1)).strftime('%B-%Y')
    
    df = fcast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()    
    df['month_year'] = df['ds'].apply(lambda x : x.strftime('%B-%Y'))     
    
    df = df.groupby('month_year', as_index=False). \
            agg({'yhat':np.sum, 'yhat_lower':np.sum, 'yhat_upper':np.sum})    
    df = df.rename(columns={'yhat_lower':'lower', 'yhat':'prediction', 'yhat_upper':'upper'})
    
    month_year_list = df['month_year'].tolist()
    month_year_list = sorted(month_year_list, key = lambda x: datetime.strptime(x, '%B-%Y'))
    # if curr_month not in data
    #print(month_year_list[-2:], month_year_list[-2])
    if curr_month  not in month_year_list:
        #print('curr_month in')
        curr_month, next_month = month_year_list[-2], month_year_list[-1]
    print(curr_month, next_month)
    
    df = df.round()
    curr_ = df[df['month_year'].isin([curr_month])].to_dict(orient='record')
    next_ = df[df['month_year'].isin([next_month])].to_dict(orient='record')
    
   
    return {'curr_month': curr_[0], 'next_month': next_[0]}
 
 
def get_hist_and_pred_data(client_id):
    """
    Input: fcast, dataframe from fbprophet forecast
    Output: json [{
                'history': [{'ds': '18-03-2019', 'y': 4180.79},
                              {'ds': '19-03-2019', 'y': 4398.41},..],                   
                'prediction':[{'ds': '07-05-2019','yhat': 4073.61,'yhat_lower': 1310.38,'yhat_upper': 7023.90},
                            {'ds': '08-05-2019','yhat': 4636.79,'yhat_lower': 1716.99,'yhat_upper': 7369.18},.]
                  }] 
    """
    
    fcast, m = load_propet_forecast_data_n_model(client_id)
    
    last_date = m.history['ds'].values[-1].astype(str)[:10]
    fcast = fcast[fcast['ds'] > last_date][['ds', 'yhat', 'yhat_lower', 'yhat_upper']]   
        
    hist = m.history[m.history['ds'] > '2017-12-31'][['ds', 'y']]    
    fcast['ds'] = fcast['ds'].map(lambda x: x.strftime('%d-%m-%Y'))
    hist['ds'] = hist['ds'].map(lambda x: x.strftime('%d-%m-%Y'))
    
    fcast, hist = fcast.round(2), hist.round(2)
    #return fcast, hist
    return [{"prediction": fcast.to_dict(orient='records') , 
             "history": hist.to_dict(orient='records')}]
    
    
    
def test_forecast(client_id):
    
    print(get_next_sevedays_pred_data(client_id))
    print('\n')
    print(get_current_next_month_pred_data(client_id))
    print('\n')
    res = get_hist_and_pred_data(client_id)
    print(res[0]["history"][:2])
    print('\n')
    print(res[0]["prediction"][:2])
    print('\n')
    
    
def process_forecast(client_id):
    try:
        load_propet_forecast_data_n_model(client_id)       
    except Exception as e:
        logging.error(f"while creating timeseries sales prediction model for {client_id} in sales_forcast{e}")        
    logging.info(f'timeseries sales prediction model and data Mongo collection for {client_id} in sales_forcast exists')
    pkl_path = PATH + f'{client_id}_propeht_model.plk'
    data_name = 'forecast_data'
    return (os.path.exists(pkl_path) and does_collection_exist(data_name, client_id))    
    

if __name__=='__main__':
    
    test_forecast('1')














