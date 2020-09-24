import pickle
import pandas as pd
import os

PATH = os.path.join(os.path.dirname( __file__ ), 'pickle_data/')


def delete_client_pickle_files(client_id):
    
    files = [PATH + f'{client_id}_cash_flow_propeht_model.plk' ]
    for fl in files:
        if os.path.isfile(fl):
            os.remove(fl)
    return {'deleted_pickle':True}


def creat_and_dump_dataframe(data, category, client_id):
    dics = None
    if category == 'bank':        
        dics =  [
            {'date':row[0], 
            'account_entry': row[1], 
            'particulars': row[2], 
            'vch_type': row[3], 
            'vch_no': row[4],
            'debit': float(row[5]), 
            'credit': float(row[6]),                   
            } for row in data[1:]]
    
    df = pd.DataFrame(dics)
    #print(df.dtypes)
    df.to_pickle(PATH + f'{client_id}_{category}.pkl')