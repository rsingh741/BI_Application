from flask_restful import Resource, reqparse
from flask_jwt_extended import jwt_required
import time

from models.retail import ItemMaster, MSalesDetailTxns, roll_date_to_current_dates
from utils import create_and_dump_item_from_csv, create_and_dump_sales_from_csv

class r_item_master(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('Item_code',
                     
                        required=True,
                        help="Every item needs a Item_code."
                        )
    parser.add_argument('Item_name',
                      
                        required=True,
                        help="Every item needs a Item_name."
                        )

    @jwt_required
    def get(self, Item_code):
        item = ItemMaster.find_by_name(Item_code)
        if item:
            return item.json()
        return {'message': 'Item not found'}, 404

class data_import_item(Resource):
    @jwt_required
    def get(self):
        ts = time.time()        
        ItemMaster.insert_data_from_csv() # to database
        #create_and_dump_item_from_csv() # to pickle
        te = time.time()
        return {'Time taken for import item master': "{} min".format(round((te-ts)/60, 2))}


class data_import_sales(Resource):
    @jwt_required
    def get(self):
        ts = time.time()        
        MSalesDetailTxns.insert_data_from_csv() # to database
        #create_and_dump_sales_from_csv() # to pickle
        te = time.time()
        return {'Time taken for import sales master': "{} min".format(round((te-ts)/60, 2))}
    
    
    
class ShiftDateRange(Resource):
    
    @jwt_required
    def get(self):
        
        try:
            res = roll_date_to_current_dates()
        except Exception as e:
            error = str(e)
            res = None
        if not res:
            return {"error": error}, 401
        
        return res