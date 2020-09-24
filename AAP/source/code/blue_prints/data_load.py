from flask import Blueprint
from flask_restful import Api

from resources.retail import r_item_master, data_import_item, data_import_sales, ShiftDateRange
from resources.cash_flow.cash_flow_data_api import InsertCsvIntoDb


data_bp = Blueprint('data_api', __name__)
data_api = Api(data_bp)

data_api.add_resource(data_import_item,'/retail/import_items')
data_api.add_resource(data_import_sales,'/retail/import_sales') 
data_api.add_resource(ShiftDateRange,'/retail/shift_date_range')
data_api.add_resource(InsertCsvIntoDb, '/cash_flow/insert_bank_dump')

