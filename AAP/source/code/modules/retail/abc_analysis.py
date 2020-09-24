import numpy as np
import pandas as pd
import datetime
import calendar
import os
import pickle
import warnings
import logging
from functools import lru_cache

from utils import read_dataframe_from_pickle
from models.retail import ItemMaster as item_table
from models.retail import MSalesDetailTxns as sales_table
from models.retail import download_columns_from_db
from modules.retail.market_basket_analysis import load_item_dict
from mongo_utils import create_collection_and_insert_datafrme
from mongo_utils import get_dataframe_collection
from mongo_utils import does_collection_exist, drop_collection

#PATH = os.path.join(os.path.dirname(os.path.dirname( __file__ )), 'pickle_data/')
PATH = os.path.join(os.path.dirname( __file__ ), 'pickle_data/')
logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

@lru_cache(maxsize=8)
def load_n_process_salesdetail_data(client_id):   
   
    sales_details = sales_table.fetch_all_by_client(client_id) #from database    
    # sales_details = read_dataframe_from_pickle(client_id, data_name='sales') #from pickle
    
    sales_details['Datetimestamp'] = pd.to_datetime(sales_details['Datetimestamp'])
    sales_details['item_len'] = sales_details['Item_code'].apply(lambda x : len(x))
    sales_details = sales_details[sales_details['item_len'] > 2].copy()    
    # itemcode2name = {}    
    # for code, name in zip(sales_details.Item_code, sales_details.Item_name):
    #     itemcode2name[code] = name
    #     #itemname2code[name] = code        
    #return sales_details, itemcode2name
    return sales_details

def get_saleswise_abc_data(client_id):  
    
    data_name='sales_abc'     
    if not does_collection_exist(data_name, client_id):
        #df, code2name = load_n_process_salesdetail_data(client_id)
        df = load_n_process_salesdetail_data(client_id)
        item2idx, idx2item, code2name = load_item_dict(client_id)        

        itemwise_sales = df.groupby(['Item_code'], as_index=False). \
                            agg({'Unitsale_price':np.average, 'Sale_quantity': np.sum})
        itemwise_sales['totalsales'] = itemwise_sales['Unitsale_price'] * itemwise_sales['Sale_quantity']
        itemwise_sales = itemwise_sales.sort_values(by='totalsales', ascending=False)
        itemwise_sales['cumulativesales'] = itemwise_sales['totalsales'].cumsum()
        itemwise_sales['%sales'] = (itemwise_sales['cumulativesales']/itemwise_sales['totalsales'].sum())

        itemwise_sales['ABC_category'] = itemwise_sales['%sales']. \
                                         apply(lambda x: 'A' if x < 0.71 
                                              else 'B' if (x >= 0.71 and x < 0.91)
                                              else 'C')
        itemwise_sales.insert(1, 'Item_name', itemwise_sales['Item_code'].apply(lambda x : code2name[x]))
        create_collection_and_insert_datafrme(data_name, itemwise_sales, client_id)
        logging.info(f'created sales_abc Mongo collection for {client_id}in abc analysis')
    else:
        itemwise_sales = get_dataframe_collection(data_name, client_id)
        logging.info(f'sales_abc Mongo collection for {client_id} in abc_analysis exists') 
        
    return itemwise_sales


def get_profitwise_abc_data(client_id):
    
    data_name='profit_abc' 
    
    if not does_collection_exist(data_name, client_id):
            
        #df, code2name = load_n_process_salesdetail_data(client_id)
        df = load_n_process_salesdetail_data(client_id)
        item2idx, idx2item, code2name = load_item_dict(client_id)        
        
        itemwise_profit = df.groupby(['Item_code'], as_index=False). \
                        agg({'Unitsale_price':np.average, 'Unitcost_price':np.average, 'Sale_quantity': np.sum})
        itemwise_profit['profit'] = ((itemwise_profit['Unitsale_price'] - itemwise_profit['Unitcost_price'])
                                 * itemwise_profit['Sale_quantity'])
        itemwise_profit = itemwise_profit.sort_values(by='profit', ascending=False)
        itemwise_profit['cumulative_profit'] = itemwise_profit['profit'].cumsum()
        total_profit = itemwise_profit['profit'].sum()
        itemwise_profit['%profit'] = (itemwise_profit['cumulative_profit'] / total_profit)
        itemwise_profit['ABC_category'] = itemwise_profit['%profit'].apply(lambda x: 'A' if x < 0.71 
                                                                     else 'B' if (x >= 0.71 and x < 0.91)
                                                                     else 'C')
        itemwise_profit.insert(1, 'Item_name', itemwise_profit['Item_code'].apply(lambda x : code2name[x]))
        create_collection_and_insert_datafrme(data_name, itemwise_profit, client_id)
        logging.info(f'created profit_abc Mongo collection for {client_id}in abc analysis')
    else:
        itemwise_profit = get_dataframe_collection(data_name, client_id)
    
    return itemwise_profit


def get_abc_cat_data(df):
    
    temp = df['ABC_category'].value_counts().reset_index()
    
    lst = []
    for cat, cnt in zip(temp['index'].tolist(), temp['ABC_category'].tolist()):
        lst.append({'cat':cat, 'counts':cnt})
        
    return lst   

def csv_to_json(df):    
    
    return df.to_dict(orient='records')


def get_sales_abc_cat_data(client_id):
    """
    input: df => pandas dataframe
    output: [{'cat': 'C', 'counts': 3756},....]
    """
    
    df = get_saleswise_abc_data(client_id)       
        
    return get_abc_cat_data(df)

def get_profit_abc_cat_data(client_id):
    
    """
    input: df => pandas dataframe
    output: [{'cat': 'C', 'counts': 3756},....]
    """
    df = get_profitwise_abc_data(client_id)
        
    return get_abc_cat_data(df)



def get_abc_a_cat_table(client_id, category='A'):    
    
    """
    input: client_id
    output: json [{'ABC_category_profit': 'C',  'ABC_category_sales': 'A',  'profit': 0.0,
    'profit_sales_ratio': 0.0,  'totalsales': 952.7199999999999,  'Item_code': '087236100612',
    'Item_name': 'MACALLAN 12 1.75LT'},......]
    """
        
    sales_abc  = get_saleswise_abc_data(client_id)
    profit_abc = get_profitwise_abc_data(client_id)
    
    with open(PATH+f'{client_id}_code2name.pkl', 'rb') as handle:
            code2name = pickle.load(handle)   
    
            
    join_abc = pd.merge(sales_abc[['Item_code', 'totalsales', 'ABC_category']],
         profit_abc[['Item_code', 'profit', 'ABC_category']],
        on = 'Item_code', how='left')
    join_abc.insert(1, 'Item_name', join_abc['Item_code'].apply(lambda x : code2name[x]))
    
    temp = join_abc[join_abc['ABC_category_x'] == category].copy()
    temp = temp.rename(columns={'ABC_category_x':'ABC_category_sales', 'ABC_category_y':'ABC_category_profit'})
    
    temp['totalsales'] = round(temp['totalsales'], 2)
    temp['profit'] = round(temp['profit'],1)
    temp['profit_sales_ratio'] = round((temp['profit']/temp['totalsales'])*100)    
    temp = temp.sort_values(by='profit_sales_ratio')    
   
    return csv_to_json(temp)

def get_sales_and_profit_abc_summary(client_id:str):
    
    """
    sales and profit summary table
    input: client_id
    output: dict 
    [{'Categories': 'A', 'Sales': 2429573.58, 'Profit': 463050.66, 'percet': 0.71},
     {'Categories': 'B', 'Sales': 685381.15, 'Profit': 130518.33, 'percet': 0.2},
     {'Categories': 'C', 'Sales': 308109.84, 'Profit': 58721.69, 'percet': 0.09}]

    """    
    categories = ['A', 'B', 'C']
    df = pd.DataFrame()
    df['Categories'] = categories
    sales, profit = [], []
    sales_abc  = get_saleswise_abc_data(client_id)
    profit_abc = get_profitwise_abc_data(client_id)
    
    for cat in categories:
        sales.append(sales_abc[sales_abc['ABC_category'] == cat]['totalsales'].sum())
        profit.append(profit_abc[profit_abc['ABC_category'] == cat]['profit'].sum())
        
    df['Sales'] = sales
    df['Profit'] = profit    
    df['percet'] = df['Sales']/sum(sales)
    #print(df.round(2))    
    return df.round(2).to_dict(orient='records')



def test_abc(client_id):
    print(get_sales_abc_cat_data(client_id))
    print(get_profit_abc_cat_data(client_id))
    print(get_abc_a_cat_table(client_id)[:2])
    print(get_sales_and_profit_abc_summary(client_id))
    
    
def process_abc_analysis(client_id):
    try:
        get_saleswise_abc_data(client_id)        
    except Exception as e:
        logging.error(f"while creating sales_abc_df for {client_id} in abc_analysis{e}")   
    
    try:
        get_profitwise_abc_data(client_id)       
    except Exception as e:
        logging.error(f"while creating profit_abc for {client_id} in abc_analysis{e}")      
       
    try: 
        _, _, _ = load_item_dict(client_id)       
    except Exception as e:
        logging.error(f'while creating pickle code2name, idx2code for {client_id}in abc_analysis {e}')
    
       
    return (does_collection_exist('sales_abc', client_id) and    
            does_collection_exist('profit_abc' , client_id))
    
if __name__=='__main__':
    CLIENT_ID = '1'
    test_abc(CLIENT_ID)
    
