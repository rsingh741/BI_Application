from flask_jwt_extended import jwt_required
from flask_restful import Resource, reqparse
from bson import ObjectId

from modules.retail.utils import  creat_and_dump_dataframe, delete_client_pickle_files
from mongo_utils import delete_clients_mongo_collections
from db import nosqldb
from mongo_utils import FS
#load data imports 
from models.retail import ItemMaster, MSalesDetailTxns

from modules.retail.sales_visualization import get_monthly_sales_yearwise_data
from modules.retail.sales_visualization import get_monthly_sales_data
from modules.retail.sales_visualization import top20_items_based_on_sales, top10_items_based_on_sales
from modules.retail.sales_visualization import top10_items_based_on_profit
from modules.retail.sales_visualization import get_quterly_monthly_weekly_sales_curr_prev_year 

from modules.retail.market_basket_analysis import get_top20_items
from modules.retail.market_basket_analysis import get_items_purchased_along, process_mba

from modules.retail.abc_analysis import get_sales_abc_cat_data
from modules.retail.abc_analysis import get_profit_abc_cat_data
from modules.retail.abc_analysis import get_abc_a_cat_table 
from modules.retail.abc_analysis import  (
                                        get_sales_and_profit_abc_summary, 
                                        process_abc_analysis
                                        )
from modules.retail.sales_forecast import get_current_next_month_pred_data
from modules.retail.sales_forecast import get_hist_and_pred_data
from modules.retail.sales_forecast import get_next_sevedays_pred_data, process_forecast

from modules.retail.view_360 import get_basket_kpi_details 
from modules.retail.view_360 import get_total_items 
from modules.retail.view_360 import get_weekly_sales_profit 

from modules.retail.item_analysis import (                  #new api
                                    get_top20_items_on_sales,
                                    get_itemwise_kpi,
                                    get_montly_sales_of_item,
                                    get_montly_profit_of_item,
                                    process_item_analysis
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


class GetMonthlySalesYearwiseData(Resource):

    @jwt_required
    def get(self,Client_id):
      
        #return get_monthly_sales_yearwise_data(Client_id), 200
        return try_and_except(get_monthly_sales_yearwise_data, Client_id)



class GetMonthlySalesData(Resource):

    @jwt_required
    def get(self,Client_id):
        
        return try_and_except(get_monthly_sales_data, Client_id)
        #return get_monthly_sales_data(Client_id), 200
        


class Top20ItemsBasedOnSales(Resource): 
    @jwt_required
    def get(self,Client_id):
        #print('hear')
        return try_and_except(top20_items_based_on_sales, Client_id)
        #return top20_items_based_on_sales(Client_id), 200
        
class Top10ItemsBasedOnSales(Resource): 
    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(top10_items_based_on_sales, Client_id)
        #return top10_items_based_on_sales(Client_id), 200
        


class Top10ItemsBasedOnProfit(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(top10_items_based_on_profit, Client_id)
        #return top10_items_based_on_profit(Client_id), 200



class GetTop20Items(Resource):


    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_top20_items, Client_id)
        #return get_top20_items(Client_id), 200


class GetSalesAbcCatData(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_sales_abc_cat_data, Client_id)
        #return get_sales_abc_cat_data(Client_id), 200




class GetProfitAbcCatData(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_profit_abc_cat_data, Client_id)
        #return get_profit_abc_cat_data(Client_id), 200



class GetAbcaACatTable(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_abc_a_cat_table, Client_id)
        #return get_abc_a_cat_table(Client_id), 200
    

class GetAbcSalesProfitSummary(Resource):
    
    @jwt_required
    def get(self, Client_id):
        
        return try_and_except(get_sales_and_profit_abc_summary, Client_id)
        #return get_sales_and_profit_abc_summary(Client_id), 200


class GetCurrentNextMonthPredData(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_current_next_month_pred_data, Client_id)
        #return get_current_next_month_pred_data(Client_id), 200


class GetHistAndPredData(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_hist_and_pred_data, Client_id)
        #return get_hist_and_pred_data(Client_id), 200


class GetNextSevedaysPredData(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_next_sevedays_pred_data, Client_id)
        #return get_next_sevedays_pred_data(Client_id), 200


class MarketBasket(Resource):

    @jwt_required
    def get(self,Client_id):
        
        top20_item = get_top20_items(Client_id)
        if not top20_item:
            return get_top20_items_on_sales(Client_id), 200
        else:
    	    return top20_item, 200

    @jwt_required
    def post(self,Client_id):
        parser = reqparse.RequestParser()
        parser.add_argument('item',
                            type=int,
                            required=True,
                            help="This field 'Item' cannot be blank.",
                            )

        data = parser.parse_args()
        inde =  (data["item"])

        items = get_top20_items(Client_id)
        if not items:
            items = get_top20_items_on_sales(Client_id)
        item_name= (items[int(data["item"])])

        return try_and_except(get_items_purchased_along, Client_id, item_name)
        #return get_items_purchased_along(Client_id, item_name), 200

  

class getQuterlyMonthlyWeeklySalesCurrPrevYear(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_quterly_monthly_weekly_sales_curr_prev_year, Client_id)
        #return get_quterly_monthly_weekly_sales_curr_prev_year(Client_id), 200




class getBasketKpiDetails(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_basket_kpi_details, Client_id)
        #return get_basket_kpi_details(Client_id), 200


class getTotalItems(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_total_items, Client_id)
        #return get_total_items(Client_id), 200


class getWeeklySalesProfit(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_weekly_sales_profit, Client_id)
        #return get_weekly_sales_profit(Client_id), 200





class getSalesAndProfitAbcSummary(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return try_and_except(get_sales_and_profit_abc_summary, Client_id)
        #return get_sales_and_profit_abc_summary(Client_id), 200




class getTop20ItemsOnSales(Resource):

    @jwt_required
    def get(self,Client_id):
      
        return get_top20_items_on_sales(Client_id), 200

class getItemwiseKpi(Resource):

    @jwt_required
    def post(self,Client_id):

        parser = reqparse.RequestParser()
        parser.add_argument('item',
                            type=int,
                            required=True,
                            help="This field 'Item' cannot be blank.",
                            )

        data = parser.parse_args()
        inde =  ((data["item"]))
        dic = get_top20_items_on_sales(Client_id)
        item = dic[inde]        
      
        return try_and_except(get_itemwise_kpi, Client_id, item)
        #return get_itemwise_kpi(Client_id,item), 200



class getMontlySalesOfItem(Resource):

    @jwt_required
    def post(self,Client_id):

        parser = reqparse.RequestParser()
        parser.add_argument('item',
                            type=int,
                            required=True,
                            help="This field 'Item' cannot be blank.",
                            )

        data = parser.parse_args()
        inde =  ((data["item"]))
        dic = get_top20_items_on_sales(Client_id)
        item = dic[inde]
        
        return try_and_except(get_montly_sales_of_item, Client_id, item) 
        #return get_montly_sales_of_item(Client_id,item), 200


class getMontlyProfitOfItem(Resource):

    @jwt_required
    def post(self,Client_id):

        parser = reqparse.RequestParser()
        parser.add_argument('item',
                            type=int,
                            required=True,
                            help="This field 'Item' cannot be blank.",
                            )

        data = parser.parse_args()
        inde =  ((data["item"]))
        dic = get_top20_items_on_sales(Client_id)
        item = dic[inde]
        
        return try_and_except(get_montly_profit_of_item, Client_id, item) 
        #return get_montly_profit_of_item(Client_id,item), 200    


class postdatafile(Resource):
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
 
            if metadata['category']== "item":
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
                    if ItemMaster.does_client_exist(client_id):                       
                       ItemMaster.delete_client_data(client_id)
                       delete_clients_mongo_collections(client_id)
                       delete_client_pickle_files(client_id)  
                                         
                    ItemMaster.insert_data_for_items_by_row(data, client_obj_id) #to database
                    #creat_and_dump_dataframe(data, metadata['category'], client_obj_id) #to pikcle
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
                    return {"status": False}, 404
                return  True, 200

            elif metadata['category']=="sales":
                outputdata =FS.get(ObjectId(objectid)).read()

                for i in (str(outputdata)).split("\\n"):
                    byedata = bytes(i, 'utf-8')
                    d = (byedata.decode("utf-8"))
                    fresh= (str((d.strip('\\r'))) )
                    if len (fresh.split(','))>4:
                        data.append(fresh.split(','))
                try:
                    client_id = str(client_obj_id) # to convert bson objectId to str
                    #print(client_id)
                    if MSalesDetailTxns.does_client_exist(client_id):
                        MSalesDetailTxns.delete_client_data(client_id)
                        delete_clients_mongo_collections(client_id)
                        delete_client_pickle_files(client_id)                         
                    MSalesDetailTxns.insert_data_for_sales_by_row(data, client_obj_id) #to database
                    #creat_and_dump_dataframe(data, metadata['category'], client_obj_id) #to pickle
                    #set isProcessed to True in myfiles.files collection
                    metadata.setdefault('isProcessed', True)
                    metadata['isProcessed']=True
                    files_coll = nosqldb['myfiles.files']
                    files_coll.update_one(
                            {'_id':ObjectId(objectid)}, {"$set": {'metadata': metadata}}
                        )
                except Exception as e:
                    print (str(e))                   
                    return { "status": False}, 404
                return  True, 200

            else:
                return {"status": False}, 404            

        return {"status": False}, 400


class PostFileUpload(Resource):
    """
    process the data to create mongo collection and pickle file
    """
    @jwt_required
    def get(self, client_id):
        is_processed = True
        
        try:
            temp = process_mba(client_id)
            is_processed = is_processed and temp
        except Exception as e:
            print (str(e))
            return {"Error": "Maketbasket data is not processed" , "status":"Failed" }

        try:
            temp = process_abc_analysis(client_id) 
            is_processed = is_processed and temp
        except Exception as e:
            print (str(e))
            return {"Error": "ABC analysis data is not processed" , "status":"Failed" }
        

        try:
            temp = process_item_analysis (client_id) 
            is_processed = is_processed and temp
        except Exception as e:
            print (str(e))
            return {"Error": "Item analysis data is not processed" , "status":"Failed" }


        try:
            temp=process_forecast(client_id)
            is_processed = is_processed and temp
        except Exception as e:
            print (str(e))
            return {"Error": "Forecast data is not processed" , "status":"Failed" } 
             
        if client_id != '1':
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
            MSalesDetailTxns.delete_client_data(client_id)
            ItemMaster.delete_client_data(client_id)
            delete_clients_mongo_collections(client_id)
            delete_client_pickle_files(client_id)
        except Exception as e:
            error = str(e) 
        if error:
            return {"error": error}
        return {"cleaned":True}, 200
              
        