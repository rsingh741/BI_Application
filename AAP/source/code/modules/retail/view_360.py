import numpy as np
import pandas as pd
import datetime
import calendar
import os
from functools import lru_cache


from models.retail import ItemMaster as item_table
from models.retail import MSalesDetailTxns as sales_table
from models.retail import download_columns_from_db
from utils import read_dataframe_from_pickle

PATH = os.path.join(os.path.dirname( __file__ ), 'pickle_data/')

@lru_cache(maxsize=8)
def load_data_dataframe(client_id):    
  
    df = sales_table.fetch_all_by_client(client_id)  # from database
    #df = read_dataframe_from_pickle(client_id, data_name='sales')  # from pikcle
    
    return df

def get_basket_kpi_details(client_id:str):
    """
    input: client_id:str
    output: {'as_on_May': 
                {'total_revenue': 742710.0,
                'num_basket': 36178,
                 'avg_item_per_basket': 2.0,
                  'avg_profit_per_basket': 4.65}
            }    
    """   
    
    now = datetime.datetime.now()
    year = now.year     
    df = load_data_dataframe(client_id) # load the datafrome from db or pikcke
    year_list = df['Datetimestamp'].dt.year.value_counts().index.tolist()
    # print(year_list, max(year_list))
    if year not in year_list:
        year = max(year_list)
    df['Datetimestamp'] = pd.to_datetime(df['Datetimestamp'])    
    df = df[df['Datetimestamp'].dt.year == year].copy()
    
    #df.head(5)
    #print(df.columns)
    month_upto = df.Datetimestamp.dt.month_name().tolist()[-1]    
    df['profit'] = df['Unitsale_price'] - df['Unitcost_price']
    agg_on_sales_id = df.groupby(['Sales_id'], as_index=False). \
            agg({"Item_name": 'count', "Total_sales": np.sum , 'profit': np.sum})
    #agg_on_sales_id.head()

    res = {
        'total_revenue': df['Total_sales'].sum().round(),
        'num_basket': agg_on_sales_id.shape[0],
        'avg_item_per_basket' : agg_on_sales_id.Item_name.mean().round(),
        'avg_profit_per_basket': agg_on_sales_id.profit.mean().round(2)
    }
    
    return {f'as_on_{month_upto}':res}
    

def get_total_items(client_id:str):
    """from database
    input=> str:client_id 
    output=> dict: {'total_items': 9765}    """    
    df = load_data_dataframe(client_id) # load the datafrome from db or pikcke
    
    return {'total_items': df.shape[0]}


def get_weekly_sales_profit(client_id:str):
    
    """
    input: client_id: str
    output: dict: 
    [{'week': 1.0, 'sales': 28709.0, 'profit': 6585.0, 'Sale_quantity': 3306.0},
     {'week': 2.0, 'sales': 37870.0, 'profit': 8640.0, 'Sale_quantity': 4543.0},...]
    """    
    now = datetime.datetime.now()
    year = now.year        
  
    df = load_data_dataframe(client_id) # load the datafrome from db or pikcke    
    df['Datetimestamp'] = pd.to_datetime(df['Datetimestamp']) 
    
    year_list = df['Datetimestamp'].dt.year.value_counts().index.tolist()
    # print(year_list, max(year_list))
    if year not in year_list:
        year = max(year_list)
        
    df = df[df['Datetimestamp'].dt.year == year].copy()
    
    df['profit'] = df['Unitsale_price'] - df['Unitcost_price']
    df['week'] = df.Datetimestamp.dt.week
    #print(df.head(10))
    df_week = df.groupby(['week'], as_index=False) \
        .agg({'Total_sales': np.sum, 'profit':np.sum, 'Sale_quantity':np.sum}) \
        .rename(columns={'Total_sales':'sales', 'profit':'profit', 'quantity':'weekly_qty'}) \
        .round()
            
    return df_week.to_dict(orient='records')        


def test_view_360(client_id:str):
    print(get_basket_kpi_details(client_id))
    print(get_total_items(client_id))
    print(get_weekly_sales_profit(client_id))   
    
if __name__=='__main__':
    client_id = '1'
    test_view_360(client_id)    