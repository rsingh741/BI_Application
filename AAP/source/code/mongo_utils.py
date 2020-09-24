import pandas as pd
from gridfs import GridFS
from gridfs.errors import NoFile
import datetime

from db import nosqldb

FS = GridFS(nosqldb, collection="myfiles")


def create_collection_and_insert_doc(data_name, doc, client_id):   
    
    collection = nosqldb[f'{client_id}_{data_name}']
    res = collection.insert_one(doc) 
    
     
def create_collection_and_insert_many(data_name, doc_list, client_id):   
    
    collection = nosqldb[f'{client_id}_{data_name}']
    docs = []
    for i, doc in enumerate(doc_list):
        docs.append(doc)
        if i % 1000 == 0:
            res = collection.insert_many(docs) 
            docs = []
    res = collection.insert_many(docs)   
  

def create_collection_and_insert_datafrme(data_name, df, client_id):       
    
    collection = nosqldb[f'{client_id}_{data_name}']
    res = collection.insert_many(df.to_dict('records'))    

def get_dataframe_collection(data_name, client_id):        
    
    collection = nosqldb[f'{client_id}_{data_name}']
    return pd.DataFrame(list(collection.find())).drop('_id', axis=1)
    

def does_collection_exist(data_name, client_id):   
    
    collection_names = nosqldb.list_collection_names()   
    return f'{client_id}_{data_name}' in collection_names


def drop_collection(data_name, client_id):  
    """drop collection """  
    collection = nosqldb[f'{client_id}_{data_name}']
    collection.drop()


def delete_clients_mongo_collections(client_id, module='retail'):
    
    if module == 'retail':
        now = datetime.datetime.now()
        year = now.year    
        data_names = ['coocc_mat', 'sales_abc', 'profit_abc', f'{year}_sales_mat', f'{year-1}_sales_mat',
                    f'{year}_profit_mat', f'{year-1}_profit_mat', 'forecast_data']
    elif module == 'cash_flow':
        data_names = ['cash_flow_weekly_forecast_data']
    
    for data_name in data_names:
        #print(data_name)
        if does_collection_exist(data_name, client_id):
            drop_collection(data_name, client_id)
            
    return {f'deleted_mongo_collection_for_client {client_id}':True}
