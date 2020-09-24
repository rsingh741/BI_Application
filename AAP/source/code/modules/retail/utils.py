import pickle
import pandas as pd
import os

PATH = os.path.join(os.path.dirname( __file__ ), 'pickle_data/')


def creat_and_dump_dataframe(data, category, client_id):
    dics = None
    if category == 'sales':
        dics =  [
            {'Sales_id':row[0], 
            'Datetimestamp': row[1], 
            'Item_code': row[2], 
            'Item_name': row[3], 
            'Sale_quantity': float(row[4]),
            'Unitcost_price': float(row[5]), 
            'Unitsale_price': float(row[6]), 
            'Total_sales': float(row[7])
            } for row in data[1:]]
    elif category == 'item':
        dics = [
            {'Item_code': row[0],
             'Item_name': row[1],
             'Category' : row[2],
             'Sub_category':row[3]    
            } for row in data[1:]]
            
    df = pd.DataFrame(dics)
    #print(df.dtypes)
    df.to_pickle(PATH + f'{client_id}_{category}.pkl')

def read_dataframe_from_pickle(client_id, data_name):
    df = pd.read_pickle(PATH + f'{client_id}_{data_name}.pkl')
    return df

  

def delete_client_pickle_files(client_id):
    
    files = [PATH+f'{client_id}_item2idx.pkl',     
        PATH+f'{client_id}_idx2item.pkl' ,      
        PATH+f'{client_id}_code2name.pkl',
        PATH+f'{client_id}_top20_items_on_sales.pkl',
        PATH + f'{client_id}_sales.pkl',
        PATH + f'{client_id}_item.pkl',
        PATH + f'{client_id}_propeht_model.plk']
    for fl in files:
        if os.path.isfile(fl):
            os.remove(fl)
    return {'deleted_pickle':True}
        
       