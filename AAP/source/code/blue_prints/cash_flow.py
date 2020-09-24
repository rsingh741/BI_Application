from flask import Blueprint
from flask_restful import Api

from resources.cash_flow.cash_flow_api import (
                            MonthlyInflowOutflow,
                            MonthlyCumInflowOutflowBalance,
                            MonthlyBalance,
                            RevenueExpenseKpi,
                            ClintWiseInflowPercent,
                            HeadWiseOutflowPercent,
                            MonthlyTopkRevenue,
                            MonthlyTopkExpense,
                            MonthlyForcast,
                            HistAndPredWeeklyData,
                            CustomerMonthlyRevenue,
                            AnnulQuarterMonthWeekRevenue,
                            UploadDataFile,
                            ProcessCashFlowFiles,
                            CleanUpClientData
                        )


cash_flow_bp = Blueprint('cash_flow_api', __name__)
cash_flow_api = Api(cash_flow_bp)

cash_flow_api.add_resource(MonthlyInflowOutflow,'/cash_flow/monthly_inflow_outflow/<string:client_id>')
cash_flow_api.add_resource(MonthlyCumInflowOutflowBalance,'/cash_flow/cum_inflow_outflow_balance/<string:client_id>')  
cash_flow_api.add_resource(MonthlyBalance, '/cash_flow/monthly_cash_balance/<string:client_id>' )
cash_flow_api.add_resource(RevenueExpenseKpi, '/cash_flow/revenue_expense_kpi/<string:client_id>' )

cash_flow_api.add_resource(ClintWiseInflowPercent, '/cash_flow/clientwise_inflow/<string:client_id>')
cash_flow_api.add_resource(HeadWiseOutflowPercent, '/cash_flow/deffent_headwise_outflow/<string:client_id>')
cash_flow_api.add_resource(MonthlyTopkRevenue, '/cash_flow/monthly_topk_revenue/<string:client_id>')
cash_flow_api.add_resource(MonthlyTopkExpense, '/cash_flow/monthly_topk_expense/<string:client_id>')

cash_flow_api.add_resource(MonthlyForcast, '/cash_flow/monthly_cash_blance_forecast/<string:client_id>')
cash_flow_api.add_resource(HistAndPredWeeklyData, '/cash_flow/weekly_hist_pred_data/<string:client_id>')

cash_flow_api.add_resource(CustomerMonthlyRevenue, '/cash_flow/customer_monthly_revenue/<string:client_id>')

cash_flow_api.add_resource(AnnulQuarterMonthWeekRevenue, '/cash_flow/interactive_time_sereis/<string:client_id>')

cash_flow_api.add_resource(UploadDataFile, '/cash_flow/upload_data_file/<string:objectid>')
cash_flow_api.add_resource(ProcessCashFlowFiles, '/cash_flow/process_data_file/<string:client_id>')
cash_flow_api.add_resource(CleanUpClientData, '/cash_flow/delete_client_data/<string:client_id>')