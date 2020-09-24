import jwt
from flask_restful import Resource, reqparse
from flask_jwt_extended import jwt_required, create_access_token
from flask import request, abort 
from datetime import datetime, timedelta

from modules.retail.sales_visualization import (
                        get_average_sales, 
                        get_aggrigate_quarter_month_week, 
                        get_total_sales)

    
#chatbot 
class AvgSalesAPI(Resource):    
    
    @jwt_required    
    def post(self, client_id):           
        parser = reqparse.RequestParser()
        parser.add_argument('time_period',
                            type=str,
                            required=True,
                            help="This field 'time_period' cannot be blank.",
                            )
        parser.add_argument('year',
                            type=int,
                            required=True,
                            help="This field 'year' cannot be blank.",
                            )
        
        data= parser.parse_args()
        year =  data["year"]       
        time_period = data['time_period']
       
        print(time_period, year)
        try:
            result = get_average_sales(client_id, time_period, year )
        except Exception as e:
            print (str(e))
            error = str(e)
            result=None

        if not result:
            return {"error": error}, 404

        return {"res":result}, 201
    
#chatbot 
class TotalSalesAPI(Resource):    
    
    @jwt_required    
    def post(self, client_id):           
        parser = reqparse.RequestParser()
      
        parser.add_argument('year',
                            type=int,
                            required=True,
                            help="This field 'year' cannot be blank.",
                            )
        
        data= parser.parse_args()
        year =  data["year"]       
       
        print(year)
        try:
            result = get_total_sales(client_id,  year)
        except Exception as e:
            print (str(e))
            error = str(e)
            result=None

        if not result:
            return {"error": error}, 404

        return {"res":result}, 201
    
    
class QuarterMonthWiseSalesAPI(Resource):  
           
    @jwt_required
    def post(self, client_id):       
        parser = reqparse.RequestParser()
        parser.add_argument('year',
                            type=int,
                            required=True,
                            help="This field 'year' cannot be blank.",
                            )
        
        data= parser.parse_args()
        year =  (data["year"])        
        print(year)
        print("quarterwise & monthwise")
        try:
            result = get_aggrigate_quarter_month_week(client_id, year)
        except Exception as e:
            #print (str(e))
            result=None
            error = str(e)
        if not result:
            return {"error": error}, 404

        return result, 201
