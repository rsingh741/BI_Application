import numpy as np
import pandas as pd
import os
import datetime
import time
import pickle
import operator
import logging

from utils import read_dataframe_from_pickle
from models.retail import ItemMaster as item_table
from models.retail import MSalesDetailTxns as sales_table
from models.retail import download_columns_from_db

from db import nosqldb as db
from mongo_utils import create_collection_and_insert_many
from mongo_utils import does_collection_exist, drop_collection
from modules.retail.sales_visualization import top20_items_based_on_sales
from modules.retail.market_basket_analysis import load_item_dict, create_coocc_mat_mango_collecion
from modules.retail.sales_visualization import get_daily_sales_data

#PATH = os.path.join(os.path.dirname(os.path.dirname( __file__ )), 'pickle_data/')
PATH = os.path.join(os.path.dirname( __file__ ), 'pickle_data/')

logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

# def create_dic_item_idx(itemcodes):
    
#     item2idx = {}
#     idx2item = {}
#     for i, item in enumerate(itemcodes):
#         item2idx[item] = i
#         idx2item[i] = item
        
#     return item2idx, idx2item

def get_current_or_max_year(client_id):
    
    df = get_daily_sales_data(client_id)      
   
    now = datetime.datetime.now()
    year = now.year    
    year_list = df.year.value_counts().index.tolist()
    print(year_list)
    # current year not in data set
    if year not in year_list:
        year = max(year_list)  
        
    return year
    


def get_item_wise_sales_data(client_id, year):
    required_cols = ['Datetimestamp', 'Item_code', 'Item_name','Sale_quantity', 'Unitcost_price', 
                     'Unitsale_price','Total_sales']
   
    sales = sales_table.fetch_all_by_client(client_id)[required_cols] #from database
    # sales = read_dataframe_from_pickle(client_id, data_name='sales')[required_cols] #from pickle
    
    sales['Datetimestamp'] = pd.to_datetime(sales['Datetimestamp'])
    sales['month']= sales['Datetimestamp'].dt.month.astype(np.int8)
    sales['year'] = sales['Datetimestamp'].dt.year.astype(np.int16)
    sales['profit'] = round(((sales['Unitsale_price'] - sales['Unitcost_price'])
                             * sales['Sale_quantity']), 2)    
    sales = sales[sales['year'] >= year-1]    
    
    return sales

def create_collection_item_sales(data_name, df, item2idx, client_id):
    
    if does_collection_exist(data_name, client_id):
        drop_collection(data_name, client_id)
   
    rows = len(item2idx)
    cols = 13
    X = np.zeros((rows, cols), np.float32)
    for item, month, sales in zip(df['Item_code'], df['month'], df['Total_sales']):        
        if len(item) > 2:
            #print(item2idx[item])
            X[item2idx[item], month] += sales
            
    doc_list = []   
    for i in range(rows):
        doc = {'idx': i, 'total': float(X[i, :].sum()),'row':X[i, :].tolist()}
        doc_list.append(doc)
    create_collection_and_insert_many(data_name=data_name, doc_list=doc_list, client_id=client_id)
    logging.info(f'item_wise_sales Mongo collection for {client_id} in item_analysis created')


def create_collection_item_profit(data_name, df, item2idx, client_id):
    
    if does_collection_exist(data_name, client_id):
        drop_collection(data_name, client_id)
        
    rows = len(item2idx)
    cols = 13
    X = np.zeros((rows, cols), np.float32)
    for item, month,profit in zip(df['Item_code'], df['month'], df['profit']):
        if len(item) > 2:
            X[item2idx[item], month] += profit
        
    doc_list = []   
    for i in range(rows):
        doc = {'idx': i, 'total': float(X[i, :].sum()),'row':X[i, :].tolist()}
        doc_list.append(doc)
    create_collection_and_insert_many(data_name=data_name, doc_list=doc_list, client_id=client_id)
    logging.info(f'item_wise_profit Mongo collection for {client_id} in item_analysis created')    
    

def create_or_load_item_monthly_sales_profit_mat(client_id):
    
    # now = datetime.datetime.now()
    # year = now.year 
    year = get_current_or_max_year(client_id)
    cur_sale_name = f'{year}_sales_mat'
    prev_sale_name = f'{year-1}_sales_mat'
    cur_profit_name = f'{year}_profit_mat'
    prev_profit_name = f'{year-1}_profit_mat'
    
    if (not does_collection_exist(cur_sale_name, client_id)
         or not does_collection_exist(prev_sale_name, client_id)):       
       
        df = get_item_wise_sales_data(client_id, year)
        print("In item_analyis load data complete") 
        item2idx, idx2item, code2name = load_item_dict(client_id)
        curr_df, prev_df = df[df['year']== year], df[df['year']== year-1]
        
        create_collection_item_sales(cur_sale_name, curr_df, item2idx, client_id)   
        create_collection_item_sales(prev_sale_name, prev_df, item2idx, client_id)        
        print(f'{prev_sale_name}')
        create_collection_item_profit(cur_profit_name, curr_df, item2idx, client_id)        
        create_collection_item_profit(prev_profit_name, prev_df, item2idx, client_id)
        print(f'{prev_profit_name}')
    else:
        item2idx, idx2item, code2name = load_item_dict(client_id)
        
    return item2idx, idx2item, code2name


def get_itemwise_kpi(client_id:str, item_name:str):
    """
    input: client_id:str, item_name:str
    ouput: {'FIREBALL CINNAMON WHISKY 50ML_kpi': [{'total_sales': 2781},
                                                  {'total_profit': 1381},
                                                  {'total_basket': 625}]}
    """    
    # now = datetime.datetime.now()
    # year = now.year 
    year = get_current_or_max_year(client_id)
    cur_sale_name = f'{year}_sales_mat'  
    cur_profit_name = f'{year}_profit_mat'
    data_name='coocc_mat'
    
    _, _, _ = create_coocc_mat_mango_collecion(client_id)
    item2idx, idx2item, code2name = create_or_load_item_monthly_sales_profit_mat(client_id)
 
    name2code = {v : k for k, v in code2name.items()}      
    #print(name2code)
    idx = item2idx[name2code[item_name]]
    sale_coll = db[f'{client_id}_{cur_sale_name }']
    total_sales = sale_coll.find_one({'idx':idx})
   
    profit_coll = db[f'{client_id}_{cur_profit_name }']
    total_profit = profit_coll.find_one({'idx':idx})
    
    freq_coll = db[f'{client_id}_{data_name}']
    basket = freq_coll.find_one({'idx':idx})['bskts']
    #arr = np.nonzero(np.array(basket['row']) >=1)[0].tolist()
    #print(len(arr), basket['freq'])
    res = {
        f'{item_name}_kpi':[
            {'total_sales': round(total_sales['total'])},
            {'total_profit': round(total_profit['total'])},
            {'total_basket': basket}
            
    ]}
    
    return res

def get_montly_sales_of_item(client_id:str, item_name:str):
    """
    input: client_id:str, item_name:str
    ouput: {'FIREBALL CINNAMON WHISKY 50ML_sales': [
    {'2018': 295,'2019': 500,'month': 1},
  {'2018': 318, '2019': 538, 'month': 2},
  {'2018': 416, '2019': 774, 'month': 3},...]}
    """        
    # now = datetime.datetime.now()
    # year = now.year 
    year = get_current_or_max_year(client_id)
    cur_sale_name = f'{year}_sales_mat'  
    prev_sale_name = f'{year-1}_sales_mat'    
    print(cur_sale_name, prev_sale_name)
    item2idx, idx2item, code2name = create_or_load_item_monthly_sales_profit_mat(client_id)
    name2code = {v : k for k, v in code2name.items()} 
    idx = item2idx[name2code[item_name]]
    
    curr_year_coll= db[f'{client_id}_{cur_sale_name }']
    curr_year_sales = curr_year_coll.find_one({'idx':idx})
    cur_year_monthly_sales = curr_year_sales['row']    
    
    prev_year_coll= db[f'{client_id}_{prev_sale_name }']
    prev_year_sales = prev_year_coll.find_one({'idx':idx})
    prev_year_monthly_sales = prev_year_sales['row']    
    res = []
    for i in range(1, 13):
        res.append(
            {
                f'{year-1}' : round(prev_year_monthly_sales[i]), 
                f'{year}': round(cur_year_monthly_sales[i]),
                'month': i
            }
        )
        # f'{item_name}_kpi'
    
    return {f'{item_name}_sales' :res}

def get_montly_profit_of_item(client_id:str, item_name:str):
    """
    input: client_id:str, item_name:str
    ouput: {'FIREBALL CINNAMON WHISKY 50ML_sales': [
    {'2018': 146,'2019': 248, 'month': 1},
    {'2018': 157, '2019': 267, 'month': 2},
    {'2018': 206, '2019': 384, 'month': 3},,...]}
    """           
    
    # now = datetime.datetime.now()
    # year = now.year 
    year = get_current_or_max_year(client_id)
    cur_profit_name = f'{year}_profit_mat'  
    prev_proit_name = f'{year-1}_profit_mat'    
   
    item2idx, idx2item, code2name = create_or_load_item_monthly_sales_profit_mat(client_id)
    name2code = {v : k for k, v in code2name.items()} 
    idx = item2idx[name2code[item_name]]
    
    curr_year_coll= db[f'{client_id}_{cur_profit_name}']
    curr_year_profit = curr_year_coll.find_one({'idx':idx})
    cur_year_monthly_profit = curr_year_profit['row']   
    
    prev_year_coll= db[f'{client_id}_{prev_proit_name}']
    prev_year_profit = prev_year_coll.find_one({'idx':idx})
    prev_year_monthly_profit = prev_year_profit['row']
    
    res = []
    for i in range(1, 13):
        res.append(
            {
                f'{year-1}' : round(prev_year_monthly_profit[i]), 
                f'{year}': round(cur_year_monthly_profit[i]),
                'month': i
            }
        )
        # f'{item_name}_kpi'
    
    return {f'{item_name}_profit' :res}

def get_top20_items_on_sales(client_id):    
    """    
    input: json [{'totalsales': 34396.74, 'Item_name': 'TITO S HANDMADE REGSTORE VODKA 1.75LT'},
                 {'totalsales': 31957.07, 'Item_name': 'MOET  CHANDON IMP 750ML'}, ....]
    output: {1: 'TITO S HANDMADE REG VODKA 1.75LT', 2: 'COORS LIGHT 30PK CANS',..}
    """
    if not os.path.exists(PATH+f'{client_id}_top20_items_on_sales.pkl'):
        items = top20_items_based_on_sales(client_id)
        
        res = {}
        for it in items:
             res[it['item_id']] = it['Item_name']
        with open(PATH+f'{client_id}_top20_items_on_sales.pkl', 'wb') as handle:
             pickle.dump(res, handle, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(PATH+f'{client_id}_top20_items_on_sales.pkl', 'rb') as handle:
            res = pickle.load(handle)        
    
    return res    
     

def check_item_analysis(client_id):
     top20_item = get_top20_items_on_sales(client_id)
     print(top20_item)
     item_name = top20_item[1]
     print(get_itemwise_kpi(client_id, item_name))
     print(get_montly_sales_of_item(client_id, item_name))   
     print(get_montly_profit_of_item(client_id, item_name))
     
     
def process_item_analysis(client_id):
    try:
        create_or_load_item_monthly_sales_profit_mat(client_id)        
    except Exception as e:
        logging.error(f"while creating itemwise_sales_profit_mat for {client_id} in item_analysis{e}")        
   
    try: 
        _, _, _ = load_item_dict(client_id)       
    except Exception as e:
        logging.error(f'while creating pickle code2name, idx2code for {client_id}in item_analysis {e}')
    logging.info(f'pickle for code2name, idx2code for {client_id} in item_analysis exists')
    
    # now = datetime.datetime.now()
    # year = now.year 
    year = get_current_or_max_year(client_id)
    cur_sale_name = f'{year}_sales_mat'
    prev_sale_name = f'{year-1}_sales_mat'
    cur_profit_name = f'{year}_profit_mat'
    prev_profit_name = f'{year-1}_profit_mat'
    data_names =  [cur_sale_name, prev_sale_name, cur_profit_name, prev_profit_name]
    collection_exist = True
    for data_name in data_names:
        if does_collection_exist(data_name, client_id): 
            collection_exist = collection_exist and True
    
    return collection_exist
     
if __name__=='__main__':
    client_id = '1'
    check_item_analysis(client_id)
   
