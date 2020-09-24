import numpy as np
import pandas as pd
import pickle
import os
import logging
#from pymongo import MongoClient

from utils import read_dataframe_from_pickle
from models.retail import ItemMaster as item_table
from models.retail import MSalesDetailTxns as sales_table
from models.retail import download_columns_from_db

from db import nosqldb as db
from mongo_utils import create_collection_and_insert_many
from mongo_utils import does_collection_exist, drop_collection
#from modules.retail.item_analysis import get_top20_items_on_sales


#PATH = os.path.join(os.path.dirname(os.path.dirname( __file__ )), 'pickle_data/')
PATH = os.path.join(os.path.dirname( __file__ ), 'pickle_data/')
logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')


def create_itemcodes_list(client_id):    
    required_cols = ['Sales_id', 'Item_code', 'Item_name']    
    saleid_items = sales_table.fetch_all_by_client(client_id) #from database
    # saleid_items = read_dataframe_from_pickle(client_id, data_name='sales') #from pickle
    saleid_items = saleid_items[required_cols]
    saleid_items['vitem_len'] = saleid_items['Item_code'].apply(lambda x : len(x))
    saleid_items = saleid_items[saleid_items['vitem_len'] > 2]

    items_groups = saleid_items[['Sales_id', 'Item_code', 'Item_name']].groupby(['Sales_id'], as_index=False). \
                    agg(lambda x : ','.join(x))

    items_groups['Item_code'] = items_groups['Item_code'].apply(lambda x : x.split(','))   
    items_groups['Item_code'] = items_groups['Item_code'].apply(lambda x: list(set(x)))   

    Item_codes_list = items_groups.Item_code.tolist()    
    
    return  Item_codes_list

def get_code2name_dict(client_id):    
    
    saleid_items = sales_table.fetch_all_by_client(client_id)[['Item_code', 'Item_name']] #from database
    # saleid_items = read_dataframe_from_pickle(client_id, data_name='sales')[['Item_code', 'Item_name']] #from pickle
    saleid_items['vitem_len'] = saleid_items['Item_code'].apply(lambda x : len(x))
    saleid_items = saleid_items[saleid_items['vitem_len'] > 2]
    
    # mst_item = download_columns_from_db(['Item_code', 'Item_name'], item_table, client_id)    
    # mst_item = read_dataframe_from_pickle(client_id, data_name='item')
    code2name = {}
    # for i in range(mst_item.shape[0]):       
    #     code, name = mst_item.Item_code[i], mst_item.Item_name[i]
    #     if len(code) > 2:
    #         code2name[code] = name       
    
    for code, name in zip(saleid_items.Item_code, saleid_items.Item_name):
        #if code not in code2name:        
        code2name[code] = name    
            
    return code2name
    
    

def create_dic_item_idx(itemcodes):
    
    item2idx = {}
    idx2item = {}
    for i, item in enumerate(itemcodes):
        item2idx[item] = i
        idx2item[i] = item
        
    return item2idx, idx2item

def create_item_dict(client_id, code2name):
    
    item2idx, idx2item = create_dic_item_idx([*code2name])
    with open(PATH+f'{client_id}_item2idx.pkl', 'wb') as handle:
        pickle.dump(item2idx, handle, protocol=pickle.HIGHEST_PROTOCOL)
    with open(PATH+f'{client_id}_idx2item.pkl', 'wb') as handle:
        pickle.dump(idx2item, handle, protocol=pickle.HIGHEST_PROTOCOL)  
    with open(PATH+f'{client_id}_code2name.pkl', 'wb') as handle:
        pickle.dump(code2name, handle, protocol=pickle.HIGHEST_PROTOCOL)
    logging.info(f'created pickle code2name, idx2code for {client_id}in Market Basket analysis')
        
        
def load_item_dict(client_id):    
    
    if not os.path.exists(PATH+f'{client_id}_item2idx.pkl'):
        code2name = get_code2name_dict(client_id)
        create_item_dict(client_id, code2name)
    
    with open(PATH+f'{client_id}_item2idx.pkl', 'rb') as handle:
        item2idx = pickle.load(handle)
    with open(PATH+f'{client_id}_idx2item.pkl', 'rb') as handle:
        idx2item = pickle.load(handle)
    with open(PATH+f'{client_id}_code2name.pkl', 'rb') as handle:
        code2name = pickle.load(handle)   
            
    return item2idx, idx2item, code2name


def create_coocc_mat_mango_collecion(client_id):
   
    data_name = 'coocc_mat'
    
    if not does_collection_exist(data_name, client_id):
       
        vitemcodes_list = create_itemcodes_list(client_id)
        #print(vitemcodes_list[:10])                
        item2idx, idx2item, code2name = load_item_dict(client_id)
        #print(code2name)
        itemcodes = [*code2name]
        mat_sz = len(itemcodes)
        X = np.zeros((mat_sz, mat_sz), dtype=np.int32)
        #print(X.shape)

        for vitems in vitemcodes_list:
            n = len(vitems)
            if n == 1: continue
            for i in range(n):        
                vi = vitems[i]
                for j in range(i+1, n):
                    vj = vitems[j]
                    #print(item2idx[vi], item2idx[vj])
                    X[item2idx[vi], item2idx[vj]] += 1
                    X[item2idx[vj], item2idx[vi]] += 1
                    
        # insering data into mongo collecions row by row
        doc_list = []
        for i in range(X.shape[0]):            
            arr = np.nonzero(X[i,:] >=1)[0].tolist()   
            nonzero={}
            sorted_x = []
            if arr:        
                for idx in arr:
                    nonzero[idx] = int(X[i,:][idx])
                sorted_x = sorted(nonzero.items(), key=lambda x: x[1], reverse=True)
                sorted_x = [list(t) for t in sorted_x]
            doc = {'idx': i, 'freq': int(X[i, :].max()), 'bskts':len(arr), 'nonzero':sorted_x}            
            doc_list.append(doc)
        create_collection_and_insert_many(data_name, doc_list, client_id)
        logging.info(f'created coocc_matrix Mongo collection for {client_id}in Market Basket analysis')     
        
    else:
        #print(f'{client_id}_{data_name} collection exists')  
        logging.info(f'coocc_matrix Mongo collection for {client_id} in Market Basket analysis exists')   
        item2idx, idx2item, code2name = load_item_dict(client_id)   
              
   
    return item2idx, idx2item, code2name


def get_top20_items(client_id, by='freq', limit=2):
    """
    for GET request 
    Input: 
    cleind_id => saving data in mongo_colletion and fillteddata from db on client_id
    code2name => dict to map item_code to item_name
    idx2item => dict to map index to item
    idx2item => dict to map item to index
    
    Output: {1: 'FRANZISKANER WEISSBIER 16.9OZ BOT',
             2: 'FIREBALL CINNAMON WHISKY 50ML',
             ..................................,
             20: 'COPENHAGEN SNUFF'}
    """  
    data_name = 'coocc_mat'  
    item2idx, idx2item, code2name = create_coocc_mat_mango_collecion(client_id=client_id)
    dic = {}   
    #print(f'{client_id}_{data_name}')
    collection = db[f'{client_id}_{data_name}']
    res = collection.find({by:{'$gt': limit}}).sort(by, -1).limit(20)
    
    for i, d in enumerate(res):
        dic[i+1] = code2name[idx2item[d['idx']]]    
        
    
    return dic

def get_items_purchased_along(client_id, item):    
    """
    for GET request 
    Input: 
    cleind_id => saving data in mongo_colletion and fillteddata from db on client_id
    item => item code
    code2name => dict to map item_code to item_name
    idx2item => dict to map index to item
    idx2item => dict to map item to index
    
    Output: [{'item': 'FIREBALL CINNAMON WHISKY 50ML', 'qty': 369},
     {'item': 'FIREBALL CINNAMON WHISKY 200ML', 'qty': 98},    """
     
    item2idx, idx2item, code2name = create_coocc_mat_mango_collecion(client_id=client_id)
    data_name = 'coocc_mat'
    result = []
    items = []
    name2code = {v : k for k, v in code2name.items()}  
   
    idx = item2idx[name2code[item]]        
    #print(f'{client_id}_{data_name}')
    collection = db[f'{client_id}_{data_name}']
    res = collection.find_one({'idx':idx})    
   
    sorted_items = res['nonzero']
    for i, it in enumerate(sorted_items):
        result.append({'item':code2name[idx2item[it[0]]], 'qty':it[1]})
        if i > 10:break
    return result


def test_mba(client_id ='1'):
    
    top20_items = get_top20_items(client_id)
    print(top20_items)
    item = top20_items[2]
    print(get_items_purchased_along(client_id, item))
    
def process_mba(client_id):
    
    data_name = 'coocc_mat'
    try:
        create_coocc_mat_mango_collecion(client_id)        
    except Exception as e:
        logging.error(f"while creating coocc_matirx for {client_id} in market basket analysi{e}")        
     
    return does_collection_exist(data_name, client_id) and os.path.exists(PATH+f'{client_id}_item2idx.pkl')
    

if __name__=='__main__':
    CLIENT_ID = '1'
    test_mba(CLIENT_ID)
   
