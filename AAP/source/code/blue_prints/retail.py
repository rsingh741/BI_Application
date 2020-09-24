from flask import Blueprint
from flask_restful import Api

#from resources.user import UserRegister, UserAuthorization
from resources.retail_api import (
                                GetMonthlySalesYearwiseData, 
                                GetMonthlySalesData, 
                                Top20ItemsBasedOnSales, 
                                Top10ItemsBasedOnSales,
                                Top10ItemsBasedOnProfit, 
                                GetSalesAbcCatData, 
                                GetProfitAbcCatData, 
                                GetAbcaACatTable, 
                                GetAbcSalesProfitSummary,
                                GetCurrentNextMonthPredData, 
                                GetHistAndPredData, 
                                GetNextSevedaysPredData, 
                                MarketBasket,
                                getQuterlyMonthlyWeeklySalesCurrPrevYear,
                                getBasketKpiDetails,
                                getTotalItems,
                                getWeeklySalesProfit,
                                getSalesAndProfitAbcSummary,
                                # getTop20ItemsOnSales,
                                getItemwiseKpi,
                                getMontlySalesOfItem,
                                getMontlyProfitOfItem,
                                postdatafile,
                                PostFileUpload,
                                CleanUpClientData
                                )

retail_bp = Blueprint('retail_api', __name__)
retail_api = Api(retail_bp)

retail_api.add_resource(GetMonthlySalesYearwiseData, "/retail/GetMonthlySalesYearwiseData/<string:Client_id>")
retail_api.add_resource(GetMonthlySalesData, "/retail/GetMonthlySalesData/<string:Client_id>")
retail_api.add_resource(Top20ItemsBasedOnSales, "/retail/Top20ItemsBasedOnSales/<string:Client_id>")
retail_api.add_resource(Top10ItemsBasedOnSales, "/retail/Top10ItemsBasedOnSales/<string:Client_id>")
retail_api.add_resource(Top10ItemsBasedOnProfit, "/retail/Top10ItemsBasedOnProfit/<string:Client_id>")
# retail_api.add_resource(GetTop20Items, "/retail/GetTop20Items/<string:Client_id>")
retail_api.add_resource(GetSalesAbcCatData, "/retail/GetSalesAbcCatData/<string:Client_id>")
retail_api.add_resource(GetProfitAbcCatData, "/retail/GetProfitAbcCatData/<string:Client_id>")
retail_api.add_resource(GetAbcaACatTable, "/retail/GetAbcaACatTable/<string:Client_id>")
retail_api.add_resource(GetAbcSalesProfitSummary, "/retail/GetAbcSalesProfitSummary/<string:Client_id>") 
retail_api.add_resource(GetCurrentNextMonthPredData, "/retail/GetCurrentNextMonthPredData/<string:Client_id>")
retail_api.add_resource(GetHistAndPredData, "/retail/GetHistAndPredData/<string:Client_id>")
retail_api.add_resource(GetNextSevedaysPredData, "/retail/GetNextSevedaysPredData/<string:Client_id>")
retail_api.add_resource(MarketBasket, "/retail/MarketBasket/<string:Client_id>")
retail_api.add_resource(getQuterlyMonthlyWeeklySalesCurrPrevYear, "/retail/getQuterlyMonthlyWeeklySalesCurrPrevYear/<string:Client_id>")
retail_api.add_resource(getBasketKpiDetails, "/retail/getBasketKpiDetails/<string:Client_id>")
retail_api.add_resource(getTotalItems, "/retail/getTotalItems/<string:Client_id>")
retail_api.add_resource(getWeeklySalesProfit, "/retail/getWeeklySalesProfit/<string:Client_id>")
retail_api.add_resource(getSalesAndProfitAbcSummary, "/retail/getSalesAndProfitAbcSummary/<string:Client_id>")
# retail_api.add_resource(getTop20ItemsOnSales, "/retail/getTop20ItemsOnSales/<string:Client_id>")
retail_api.add_resource(getItemwiseKpi, "/retail/getItemwiseKpi/<string:Client_id>")
retail_api.add_resource(getMontlySalesOfItem, "/retail/getMontlySalesOfItem/<string:Client_id>")
retail_api.add_resource(getMontlyProfitOfItem, "/retail/getMontlyProfitOfItem/<string:Client_id>")
retail_api.add_resource(postdatafile, "/retail/postdatafile/<string:objectid>")
retail_api.add_resource(PostFileUpload, "/retail/PostFileUpload/<string:client_id>")
retail_api.add_resource(CleanUpClientData, '/retail/clean_client_data/<string:client_id>')