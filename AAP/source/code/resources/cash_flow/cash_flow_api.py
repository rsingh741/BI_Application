from flask_jwt_extended import jwt_required
from flask_restful import Resource, reqparse
from bson import ObjectId

from modules.cash_flow.utils import delete_client_pickle_files, creat_and_dump_dataframe
from mongo_utils import delete_clients_mongo_collections
from db import nosqldb
from mongo_utils import FS

from models.cash_flow_orm import BankTransaction

from modules.cash_flow.view_360 import (
                    monthly_grouped_barchart_inflow_outflow,
                    monthly_cumulative_inflow_outflow_and_balance,
                    get_monthly_cash_balance,
                    kpi_inflow_outflow,
                    get_fiscalyear_revenue_timeseris_data
                    )   

from modules.cash_flow.in_and_out_flow import (
                    clientwise_inflow_in_percent,
                    headwise_outflow_in_percent,
                    monthly_topk_client_revenue,
                    monthly_topk_expense,
                    get_customer_list,
                    get_customer_monthly_revenue
                    )

from modules.cash_flow.cash_flow_forecast import (
                    get_monthly_prediction,
                    get_hist_and_pred_data,
                    process_forecast  
                    )



def try_and_except(func, *arg):
    #print(func, *arg)
    error = None
    try:
        result = func(*arg)        
    except Exception as e:
        error = str(e)        
        result = None
    if not result:        
        return {"error": error}, 404
    
    return result, 200


class MonthlyInflowOutflow(Resource):
    """
    api for grouped barchart for monthly inflow vs outflow
    """
    @jwt_required
    def get(self, client_id):      
        
        return try_and_except(monthly_grouped_barchart_inflow_outflow, client_id)
    

class MonthlyCumInflowOutflowBalance(Resource):
    """
    api for waterfall chat for monthly inflow, outflow, balance
    """
    @jwt_required
    def get(self, client_id):      
        
        return try_and_except(monthly_cumulative_inflow_outflow_and_balance, client_id)
    
    
class MonthlyBalance(Resource):
    """
    api for bar and trend chat for monthly balance
    """
    @jwt_required
    def get(self, client_id):      
        
        return try_and_except(get_monthly_cash_balance, client_id)
    
    

class ClintWiseInflowPercent(Resource):
    """
    api for clientwise inflow percent for barchart
    """
    @jwt_required
    def get(self, client_id):      
        
        return try_and_except(clientwise_inflow_in_percent, client_id)
    
    
class HeadWiseOutflowPercent(Resource):
    """
    api for diffent headwise outflow percent for barchart
    """
    @jwt_required
    def get(self, client_id):      
        
        return try_and_except(headwise_outflow_in_percent, client_id)
    

class RevenueExpenseKpi(Resource):
    """
    api for revenue & expense total and monthly average
    """
    @jwt_required
    def get(self, client_id):
        
        return try_and_except(kpi_inflow_outflow, client_id)
    

class MonthlyTopkRevenue(Resource):
    """
    api for monthly topk revenue cleintwise
    """
    @jwt_required
    def get(self, client_id):
        
        return try_and_except(monthly_topk_client_revenue, client_id)
    

class MonthlyTopkExpense(Resource):
    """
    api for monthly topk expense headwise
    """
    
    @jwt_required
    def get(self, client_id):
        
        return try_and_except(monthly_topk_expense, client_id)
    

class MonthlyForcast(Resource):
    """
    api for monthly cash balance prediction
    """
    
    @jwt_required
    def get(self, client_id):
        
        return try_and_except(get_monthly_prediction, client_id)


class HistAndPredWeeklyData(Resource):
    """
    api for weekly historic and prediction data
    """
    
    @jwt_required
    def get(self, client_id):
        
        return try_and_except(get_hist_and_pred_data  , client_id)  
    
    
class CustomerMonthlyRevenue(Resource):
    """
    api for customer list and cusotomer monthly revenue
    """
       
    
    @jwt_required
    def get(self, client_id):
       
        return try_and_except(get_customer_list, client_id)
    
    @jwt_required
    def post(self, client_id):
        parser = reqparse.RequestParser()
        parser.add_argument(
            'customer_id',
            type=int, 
            required=True, 
            help="customer_id required in body of request")
        data = parser.parse_args()
        idx = data['customer_id']
        customer_name = get_customer_list(client_id)[idx]
        
        return try_and_except(get_customer_monthly_revenue, client_id, customer_name)    
    
    

class AnnulQuarterMonthWeekRevenue(Resource):
    
    @jwt_required
    def get(self, client_id):
        
        return try_and_except(get_fiscalyear_revenue_timeseris_data, client_id)
    
    
class UploadDataFile(Resource):
    """
    insert retail data in database from mongo fsgrid file
    """
    @jwt_required
    def get(self, objectid):
        
        if FS.exists({"_id":ObjectId(objectid)})==True:             
            data = []
            m =FS.get(ObjectId(objectid))
            metadata= (m.metadata)
            client_obj_id= (metadata['client'])
            #print(client_obj_id)
 
            if metadata['category']== "bank":
                outputdata =FS.get(ObjectId(objectid)).read()

                for i in (str(outputdata)).split("\\n"):
                    byedata = bytes(i, 'utf-8')
                    d = (byedata.decode("utf-8"))
                    fresh= (str((d.strip('\\r'))) )
                    if len (fresh.split(','))>2:
                        data.append(fresh.split(','))
                #print(data[0])                
                try:
                    #print('item_delete') 
                    client_id = str(client_obj_id) # to convert bson objectId to str
                    #print(client_id)                   
                    if BankTransaction.does_client_exist(client_id):                       
                       BankTransaction.delete_client_data(client_id)
                       delete_clients_mongo_collections(client_id, 'cash_flow')
                       delete_client_pickle_files(client_id)  
                                         
                    error = BankTransaction.insert_data_for_bank_by_row(data, client_obj_id) #to database
                    if error:
                         return {"error": str(error)}, 404           

                    #creat_and_dump_dataframe(data, metadata['bank'], client_obj_id) #to pikcle
                    #set isProcessed to True in myfiles.files collection
                    
                    metadata.setdefault('isProcessed', True)
                    metadata['isProcessed']=True
                    files_coll = nosqldb['myfiles.files']
                    files_coll.update_one(
                            {'_id':ObjectId(objectid)}, {"$set": {'metadata': metadata}}
                        )
                except Exception as e:
                    print (str(e))
                    #error = e
                    return {"status": str(e)}, 404
               
                return  True, 200
                
            else:
                return {"status": False}, 404            

        return {"status": False}, 400


class ProcessCashFlowFiles(Resource):
    """
    process the data to create mongo collection and pickle file
    """
    @jwt_required
    def get(self, client_id):
        is_processed = True
        
        try:
            temp = process_forecast(client_id)
            is_processed = is_processed and temp
        except Exception as e:
            print (str(e))
            return {"Error": "Forecast data is not processed" , "status":"Failed" } 
             
        coll = nosqldb['users']
        user = coll.find_one({"client" :ObjectId(client_id)})  
        #print(user)         
        fl_info = user['filesInfo']
        #print(fl_info)
        coll.update({'_id':user['_id']},
            {"$set": { 
                'filesInfo': {'fileCount': 0, 
                              'fileTypes': [], 
                              'partialLock': False, 
                              'filesProcessed': fl_info['filesProcessed']
                              }
                 }
            }                                          
        )
                
        return {'processed':is_processed}


class CleanUpClientData(Resource):
    
    @jwt_required
    def get(self, client_id):
        error = None
        try:
            BankTransaction.delete_client_data(client_id)            
            delete_clients_mongo_collections(client_id)
            delete_client_pickle_files(client_id)
        except Exception as e:
            error = str(e) 
        if error:
            return {"error": error}
        return {"cleaned":True}, 200
              