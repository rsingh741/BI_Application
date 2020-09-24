import os
import csv
import pandas as pd
import time
import logging
import json

from db import db


logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

PATH = os.path.join(os.path.dirname(os.path.dirname( __file__ )), 'data_files/cash_flow/')

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        logging.info('{0} {1:.2f} sec'.format(method.__name__, te-ts))
        return result
    return timed

class BankTransaction(db.Model):
    __tablename__='bank_transaction'    
   
    id = db.Column(db.Integer, primary_key=True)
    date =  db.Column(db.DateTime)
    account_entry = db.Column(db.String(20))
    particulars = db.Column(db.String(200))
    vch_type = db.Column(db.String(80), nullable=True)
    vch_no = db.Column(db.String(80), nullable=True)
    debit = db.Column(db.Float)
    credit = db.Column(db.Float)
    client_id = db.Column(db.String(80), index=True)
    
    def __init__(self, date, account_entry, particulars, vch_type, vch_no, debit, credit, client_id):
      self.date = date
      self.account_entry = account_entry
      self.particulars = particulars
      self.vch_type = vch_type
      self.vch_no = vch_no
      self.debit = debit
      self.credit = credit
      self.client_id = client_id
      
    @classmethod
    def does_client_exist(cls, client_id):        
        return (cls.query.filter_by(client_id=client_id).first() != None)
    

    @classmethod
    def get_column(cls, col, client_id):
        column =  cls.query.filter_by(client_id=client_id).with_entities(col).all()
        return [v for v, in column]
    
    @classmethod
    def insert_data_from_csv(cls):
        engine = db.get_engine()
        with open(PATH + 'bank_dump.csv', 'r') as f: 
            next(f)          
            data = csv.reader(f) 
            engine.execute(
                cls.__table__.insert(),
                [{'date':row[0], 
                  'account_entry': row[1], 
                  'particulars': row[2], 
                  'vch_type': row[3], 
                  'vch_no': row[4],
                  'debit': row[5], 
                  'credit': row[6],                   
                  'client_id':"1"}
                  for row in data]
            )
            
    @classmethod
    def insert_data_for_bank_by_row(cls, data, client_id):
        print ("-------------------------------")
        #print (client_id)        
        t0 = time.time()
        engine = db.get_engine()
        #print(len(data[1:]))
        error_row = None
        for row in data[1:]:
            if len(row) != 7:   
                error_row = row
                print(json.dumps(error_row))           
                break  
        # else:
        #     print(error_row)
        #     return('num value in row not match column', str(error_row))    
        if error_row:
            return {'num value in row not match column':error_row}               
         
        try:
            engine.execute(
                cls.__table__.insert(),
                [{'date':row[0], 
                  'account_entry': row[1], 
                  'particulars': row[2], 
                  'vch_type': row[3], 
                  'vch_no': row[4],
                  'debit': row[5], 
                  'credit': row[6], 
                  'client_id':str(client_id)}
                for row in data[1:]]
            )
        except Exception as e:
            print("exception", e)
            return str(e)
        print(
        "SQLAlchemy Core: Total time for " + str(len(data)) +
        " records " + str(time.time() - t0) + " secs")
        return None
    
    
    @classmethod
    @timeit
    def fetch_all_by_client(cls, client_id):          
        
        cols = ['date', 'account_entry', 'particulars', 'vch_type', 'vch_no', 'debit', 'credit']
        engine = db.get_engine()
        with engine.connect() as conn:
            d= [{c:v for c, v in zip(cols,row[1:-1])} 
                for row in 
                conn.execute(cls.__table__.select().where(cls.client_id==client_id)).fetchall()]
            df = pd.DataFrame(d)
        #print(df.head())
        return df[cols]    
    
    @classmethod
    def delete_client_data(cls, client_id):
        
        engine = db.get_engine()
        with engine.connect() as conn:
            conn.execute(cls.__table__.delete().where(cls.client_id==client_id))
            
    