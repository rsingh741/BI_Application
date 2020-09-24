from flask import Blueprint
from flask_restful import Api

from resources.retail_chatbot_api import (
                    AvgSalesAPI, 
                    QuarterMonthWiseSalesAPI, 
                    TotalSalesAPI, 
                   )

retail_chatbot_bp = Blueprint('retail_chatbot_api', __name__)
retail_chatbot_api = Api(retail_chatbot_bp)


retail_chatbot_api.add_resource(AvgSalesAPI, '/retail/average_sales/<string:client_id>')
retail_chatbot_api.add_resource(TotalSalesAPI, '/retail/total_sales/<string:client_id>')
retail_chatbot_api.add_resource(QuarterMonthWiseSalesAPI, '/retail/monthly_weekly_qurterly_for_year/<string:client_id>') 