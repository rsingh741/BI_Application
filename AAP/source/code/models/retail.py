import os
import csv
import pandas as pd
import time
import logging
from dateutil.relativedelta import relativedelta
from datetime import datetime

from db import db


logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

PATH = os.path.join(os.path.dirname(os.path.dirname( __file__ )), 'data_files/retail/')

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        logging.info('{0} {1:.2f} sec'.format(method.__name__, te-ts))
        return result
    return timed

class ItemMaster(db.Model):
    __tablename__ = 'item_master'

    id = db.Column(db.Integer, primary_key=True)
    Item_code = db.Column(db.String(80))
    Item_name = db.Column(db.String(80))    
    Category     = db.Column(db.String(80))
    Sub_category = db.Column(db.String(80))
    Client_id = db.Column(db.String(80), index=True)

    def __init__(self, Item_code, Item_name, Category, Sub_category, Client_id):
        self.Item_code = Item_code
        self.Item_name = Item_name
        self.Category = Category
        self.Sub_category = Sub_category,
        self.Client_id = Client_id
        

    def json(self):
        return {'Item_code': self.Item_code, 'Item_name': self.Item_name}

    @classmethod
    def find_by_name(cls, Item_code):
        return cls.query.filter_by(Item_code=Item_code).first()    
    
   
    @classmethod
    def does_client_exist(cls, client_id):     
        #print(type(client_id))
        return (cls.query.filter_by(Client_id=client_id).first() != None)
    
        
    @classmethod
    def get_column(cls, col, Client_id):
        column =  cls.query.filter_by(Client_id=Client_id).with_entities(col).all()
        return [v for v, in column]
    

    def save_to_db(self):
        db.session.add(self)
        db.session.commit()

    def delete_from_db(self):
        db.session.delete(self)
        db.session.commit()
        
        
    @classmethod
    @timeit
    def fetch_all_by_client(cls, client_id):          
        
        cols = ['Item_code', 'Item_name', 'Category', 'Sub_category']
        engine = db.get_engine()
        with engine.connect() as conn:
            d= [{c:v for c, v in zip(cols,row[1:-1])} 
                for row in 
                conn.execute(cls.__table__.select().where(cls.Client_id==client_id)).fetchall()]
            df = pd.DataFrame(d)
            
        return df[cols]

    @classmethod
    def generate_data_from_csv(cls):        
        with open(PATH + "mst_item.csv",'r')as f:
            next(f)
            data = csv.reader(f)
            for row in data:               
                item = cls(row[0], row[1], " ", " ", "1")
                item.save_to_db()
                
                
    @classmethod
    def insert_data_from_csv(cls):
        engine = db.get_engine()
        with open(PATH + 'mst_item.csv', 'r') as f: 
            next(f)          
            data = csv.reader(f)             
            engine.execute(
                cls.__table__.insert(),
                [{'Item_code':row[0], 'Item_name': row[1], 'Category':" ", 'Sub_category':" ", 'Client_id':"1"}
                  for row in data]
            )

    @classmethod
    def insert_data_for_items_by_row(cls, data, client_obj_id):
        t0 = time.time()
        engine = db.get_engine()
        # data = [i.split(',') for i in data]
        # with open(PATH + 'mst_item.csv', 'r') as f: 
            # next(f)          
        # data = csv.reader(f)

        engine.execute(
            cls.__table__.insert(),
            [{'Item_code':row[0], 'Item_name': row[1], 'Category':row[2], 'Sub_category':row[3], 'Client_id':str(client_obj_id)}
              for row in data[1:]]
        )
        print(
        "SQLAlchemy Core: Total time for " + str(len(data)) +
        " records " + str(time.time() - t0) + " secs")
    
    @classmethod
    def delete_client_data(cls, client_id):
        
        engine = db.get_engine()
        with engine.connect() as conn:
            conn.execute(cls.__table__.delete().where(cls.Client_id==client_id))
              

class MSalesDetailTxns(db.Model):
    __tablename__ = 'sales_detail_txns'

    id = db.Column(db.Integer, primary_key=True)
    Sales_id = db.Column(db.String(80))
    Datetimestamp = db.Column(db.DateTime)
    Item_code = db.Column(db.String(80))
    Item_name = db.Column(db.String(80))
    Sale_quantity = db.Column(db.Float)
    Unitcost_price = db.Column(db.Float)
    Unitsale_price = db.Column(db.Float)
    Total_sales = db.Column(db.Float)
    Client_id = db.Column(db.String(80), index=True)


    def __init__(self, Sales_id, Datetimestamp, Item_code, Item_name, Sale_quantity, Unitcost_price, Unitsale_price, Total_sales, Client_id):
        self.Sales_id = Sales_id
        self.Datetimestamp = Datetimestamp
        self.Item_code = Item_code
        self.Item_name = Item_name
        self.Sale_quantity = Sale_quantity
        self.Unitcost_price = Unitcost_price
        self.Unitsale_price = Unitsale_price
        self.Total_sales = Total_sales
        self.Client_id = Client_id
        
        
    @classmethod
    def find_by_name(cls, Sales_id):
        return cls.query.filter_by(Sales_id=Sales_id).first()
    
    
    @classmethod
    def does_client_exist(cls, client_id):        
        return (cls.query.filter_by(Client_id=client_id).first() != None)
    

    @classmethod
    def get_column(cls, col, Client_id):
        column =  cls.query.filter_by(Client_id=Client_id).with_entities(col).all()
        return [v for v, in column]

    @classmethod
    def generate_data_from_csv(cls):
        with open(PATH +"sales_details.csv",'r') as f:
            next(f)
            data = csv.reader(f)
            for row in data:     
                item = cls(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],"1")
                item.save_to_db()

    @classmethod
    def insert_data_from_csv(cls):
        engine = db.get_engine()
        with open(PATH + 'sales_details.csv', 'r') as f: 
            next(f)          
            data = csv.reader(f) 
            engine.execute(
                cls.__table__.insert(),
                [{'Sales_id':row[0], 
                  'Datetimestamp': row[1], 
                  'Item_code': row[2], 
                  'Item_name': row[3], 
                  'Sale_quantity': row[4],
                  'Unitcost_price': row[5], 
                  'Unitsale_price': row[6], 
                  'Total_sales': row[7],
                  'Client_id':"1"}
                  for row in data]
            )
    @staticmethod         
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""        
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
        
    @classmethod
    def insert_data_for_sales_by_row(cls, data, client_obj_id):
        print ("-------------------------------")
        print (client_obj_id)
        #print (data)
        # data = [i.split(',') for i in data]
        # print (data)
        t0 = time.time()
        engine = db.get_engine()
        print (data[1:2])  
        # for d in cls.chunks(data[1:], 6):
        try:
            engine.execute(
                cls.__table__.insert(),
                [{'Sales_id':row[0], 
                'Datetimestamp': row[1], 
                'Item_code': row[2], 
                'Item_name': row[3], 
                'Sale_quantity': row[4],
                'Unitcost_price': row[5], 
                'Unitsale_price': row[6], 
                'Total_sales': row[7],
                'Client_id':str(client_obj_id)}
                for row in data[1:]]
            )
        except Exception as e:
            print(e)
        print(
        "SQLAlchemy Core: Total time for " + str(len(data)) +
        " records " + str(time.time() - t0) + " secs")
        
    @classmethod
    @timeit
    def fetch_all_by_client(cls, client_id):          
        
        cols = ['Sales_id', 'Datetimestamp', 'Item_code', 'Item_name', 'Sale_quantity',
        'Unitcost_price', 'Unitsale_price', 'Total_sales']
        engine = db.get_engine()
        with engine.connect() as conn:
            d= [{c:v for c, v in zip(cols,row[1:-1])} 
                for row in 
                conn.execute(cls.__table__.select().where(cls.Client_id==client_id)).fetchall()]
            df = pd.DataFrame(d)
        #print(df.head())
        return df[cols]    
    
    @classmethod
    def delete_client_data(cls, client_id):
        
        engine = db.get_engine()
        with engine.connect() as conn:
            conn.execute(cls.__table__.delete().where(cls.Client_id==client_id))


    def save_to_db(self):
        db.session.add(self)
        db.session.commit()


def download_columns_from_db(col_list, table, client_id):
    df = pd.DataFrame()
    for col in col_list:
        df[col] = table.get_column(vars(table)[col], client_id)        
    return df


    
def roll_date_to_current_dates():
    
    df = pd.read_csv(PATH + 'sales_details.csv', parse_dates=['Datetimestamp'])
    curr_date = datetime.today().date()
    last_date = df['Datetimestamp'][-1:].dt.date.values[0]
    days_to_shift = (curr_date - last_date).days - 1
    
    df['Datetimestamp'] = df['Datetimestamp'].apply(lambda d : d + relativedelta(days=days_to_shift))
  
    df.to_csv(PATH + 'sales_details.csv', index=False)
    
    start_date = df['Datetimestamp'].dt.date.tolist()[0].strftime("%m-%d-%Y")
    last_date = df['Datetimestamp'].dt.date.tolist()[-1].strftime("%m-%d-%Y")
    
    return {"res": f"dates shifted range from {start_date} to {last_date}"}
    