from flask_restful import Resource, reqparse
from flask_jwt_extended import jwt_required
import time

from models.cash_flow_orm import BankTransaction



class InsertCsvIntoDb(Resource):
    
    @jwt_required
    def get(self):
        ts = time.time()
        if BankTransaction.does_client_exist('1'):  
            BankTransaction.delete_client_data('1')                 
        BankTransaction.insert_data_from_csv() # to database
        #create_and_dump_item_from_csv() # to pickle
        te = time.time()
        return {'Time taken for import bank_dump data': "{} min".format(round((te-ts)/60, 2))}