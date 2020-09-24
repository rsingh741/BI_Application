import os
import calendar
import numpy as np
import pandas as pd
from functools import lru_cache
from typing import Dict, List
from models.cash_flow_orm import BankTransaction

@lru_cache(maxsize=4)
def load_process_data(client_id):
    
    bank_df = BankTransaction.fetch_all_by_client(client_id)     
    bank_df['year'] = bank_df['date'].dt.year
    bank_df['month'] = bank_df['date'].dt.month
    bank_df['week'] = bank_df['date'].dt.week
    
    return bank_df


def monthly_grouped_barchart_inflow_outflow(client_id:str):
    """
    json data for monthly inflow and outflow details for 
    grouped barchart
    input=> client_id:str
    output => json 
    '[{"inflow":4928971.0,"outflow":2469778.0,"mm_yyyy":"Apr-2017"},
    {"inflow":2123588.0,"outflow":2593494.0,"mm_yyyy":"May-2017"}, ...]'
    """
    df =  load_process_data(client_id)
    df = df.groupby(['year', 'month'], as_index=False). \
    agg({'debit':np.sum, 'credit': np.sum}). \
    rename(columns={'debit':'inflow', 'credit':'outflow'})
    
    df['month'] = df['month'].apply(lambda x: calendar.month_abbr[x])
    df['mm_yyyy'] = df['month'] +'-'+ df['year'].astype(str)
    df.drop(['month', 'year'], inplace=True, axis=1)
    
    return df.round().to_dict(orient='records')


def monthly_cumulative_inflow_outflow_and_balance(client_id:str):
    """
    monthly cumulative inflow, outflow and balance for warter fall chart
    input=> client_id:str
    output=> json
    '[{"mm_yyyy":"Apr-2017","inflow_cumsum":4928971.0,"outflow_cumsum":2469778.0,"cash_balance":2459193.0},
      {"mm_yyyy":"May-2017","inflow_cumsum":7052559.0,"outflow_cumsum":5063272.0,"cash_balance":1989287.0}, 
      ...]
    """
    df =  load_process_data(client_id)
    df = df.groupby(['year', 'month'], as_index=False). \
    agg({'debit':np.sum, 'credit': np.sum}). \
    rename(columns={'debit':'inflow', 'credit':'outflow'})
    
    df['month'] = df['month'].apply(lambda x: calendar.month_abbr[x])
    df['mm_yyyy'] = df['month'] +'-'+ df['year'].astype(str)
    df.drop(['month', 'year'], inplace=True, axis=1)
    
    df['inflow_cumsum'] = df['inflow'].cumsum()
    df['outflow_cumsum'] = df['outflow'].cumsum()
    df['cash_balance'] = df['inflow_cumsum']- df['outflow_cumsum']
    df.drop(['inflow', 'outflow'], inplace=True, axis=1)
    
    return df.round().to_dict(orient='records')


def get_cash_balance_mid_month(df):
    
    temp = df[['date', 'debit', 'credit']].copy()
    temp['inflow_cumsum'] = temp['debit'].cumsum()
    temp['outflow_cumsum'] = temp['credit'].cumsum()
    temp['cash_balance'] = temp['inflow_cumsum'] - temp['outflow_cumsum'] 
    temp.drop(['debit', 'credit','inflow_cumsum', 'outflow_cumsum'], inplace=True, axis=1)
    balance_15th = temp[temp['date'].dt.day == 15].copy()
    balance_15th['mm_yyyy'] = balance_15th.date.dt.strftime('%b-%y')
    balance_15th = balance_15th.groupby('mm_yyyy')['cash_balance'].sum().reset_index()
    balance_15th = balance_15th[['mm_yyyy', 'cash_balance']]
    return balance_15th.round().to_dict(orient='records')


def monthly_cash_balance(df):
    
    df = df.groupby(['year', 'month'], as_index=False). \
    agg({'debit':np.sum, 'credit': np.sum}). \
    rename(columns={'debit':'inflow', 'credit':'outflow'})
    
    df['month'] = df['month'].apply(lambda x: calendar.month_abbr[x])
    df['mm_yyyy'] = df['month'] +'-'+ df['year'].astype(str)
    df.drop(['month', 'year'], inplace=True, axis=1)
    
    df['inflow_cumsum'] = df['inflow'].cumsum()
    df['outflow_cumsum'] = df['outflow'].cumsum()
    df['cash_balance'] = df['inflow_cumsum']- df['outflow_cumsum']
    df.drop(['inflow', 'outflow', 'inflow_cumsum', 'outflow_cumsum'], inplace=True, axis=1)
    
    return df.round().to_dict(orient='records')


def get_monthly_cash_balance(client_id:str):
    """inflow_cumsum
    monthly cash balance for for bar chart and trend
    input=> client_id:str
    output=> dict
    { "balance_at_15th": [{"mm_yyyy": "Apr-17", "cash_balance": 1170460.0},
                        {"mm_yyyy": " Aug-18", "cash_balance": 1309039.0}, ..]
      "monthly_agg_balance": [{"mm_yyyy":"Apr-2017","cash_balance":2459193.0},
                          {"mm_yyyy":"May-2017","cash_balance":1989287.0}, ...]
     }
    """
    df =  load_process_data(client_id)
    mid_month_balance = get_cash_balance_mid_month(df)
    monthly_agg_balance = monthly_cash_balance(df)
    
    return {'balance_at_15th': mid_month_balance, 
           'monthly_agg_balance': monthly_agg_balance}
    
    

def get_fiscalyears(client_id:str)->List:
    
    df =  load_process_data(client_id)
    df =  load_process_data(client_id)
    fy = df['date'].dt.to_period('A-MAR').astype(str).astype(int).unique().tolist()
    fy = sorted(fy, reverse=True)
    fiscalyears = list(map(lambda y: f'{y-1}-{y%1000}', fy))
    
    return fiscalyears


def kpi_inflow_outflow(client_id:str)->Dict[str,Dict[str, float]]:
    """
    input: client_id:srt
    """
    df =  load_process_data(client_id)
    fy = df['date'].dt.to_period('A-MAR').astype(str).astype(int).unique().tolist()
    fy = sorted(fy, reverse=True)
    fiscalyears = list(map(lambda y: f'{y-1}-{y%1000}', fy))
    
    df = df[df['date'].dt.to_period('A-MAR')==fy[0]]
    
    total_revenue = round(df['debit'].sum(),0)
    avg_revenue = round(df.groupby(df['date'].dt.strftime('%B'))['debit'].sum().mean(),0)

    total_expense = round(df['credit'].sum(),0)
    avg_expense = round(df.groupby(df['date'].dt.strftime('%B'))['credit'].sum().mean(),0)
    
    kpi = {
         'year': fiscalyears[0],
         'data':{
        'total_revenue': total_revenue,
        'monthly_avg_revenue': avg_revenue,
        'total_expense': total_expense,
        'monthly_avg_expense': avg_expense}
         }
    
    return kpi

def get_yearly_quterly_montly_weekly_revenue(fiscalyear, df):
    """ 
    return annual_revenue,  quterly_revenue, monthly_revenue,
        and  weekly_revenue for fiscalyear
    """
    df2 = df[df['date'].dt.to_period('A-MAR')==fiscalyear].copy()
    yearly_revenue = df2['debit'].sum().round()
    fy_name = f'{fiscalyear-1}-{fiscalyear%1000}'
    
    quarterly_agg = df2.groupby([df2.date.dt.quarter], sort=False).agg({'debit' :sum}). \
                  rename(columns={'debit':'revenue'}).reset_index()
    quarterly_agg['quarter'] = list(range(1,5))
    quterly_revenue = quarterly_agg[['quarter', 'revenue']].round().to_dict(orient='records')
    
    monthly_agg = df2.groupby(df.date.dt.strftime('%b-%Y'), sort=False).agg({'debit' :sum}). \
                  reset_index(). \
                  rename(columns={'date':'month','debit':'monthly_revenue'})
    monthly_revenue = monthly_agg.round().to_dict(orient='records')
    
    weekly_agg = df2.groupby([df2.date.dt.week], sort=False).agg({'debit' :sum}). \
                  rename(columns={'debit':'weekly_revenue'}).reset_index()
    weekly_agg['week'] = list(range(1,53))
    weekly_revenue = weekly_agg[['week', 'weekly_revenue']].round().to_dict(orient='records')
    
    # return {'fiscalyear': fy_name,
    #          'data': {'annual_revenue': yearly_revenue,
    #                   'quarterly_revenue': quterly_revenue,
    #                   'monthly_revenue': monthly_revenue,
    #                    'weekly_revenue': weekly_revenue}
    #        }
    
    return {'weekly': weekly_revenue, 
           'monthly': monthly_revenue, 
           'quarterly': quterly_revenue}


def get_fiscalyear_revenue_timeseris_data(client_id:str)->List[Dict]:
    
    """
     for interactice quaterly pie chart
    input: client_id
    output: list of dicts
    {2018-19":
     {"weekly": [{"week": 1, "weekly_revenue": 3300804.0}, ....]
      "monthly": [{"month": "Apr-2017", "monthly_revenue": 4928971.0},....],
      "quarterly": [{"quarter": 1, "revenue": 9127304.0} ...]}, ..
    
    }     
    """
    df =  load_process_data(client_id)
    fiscalyears  = df['date'].dt.to_period('A-MAR').astype(str).astype(int).unique().tolist()
    fiscalyears = sorted(fiscalyears, reverse=True)
    fy_names = list(map(lambda y: f'{y-1}-{y%1000}', fiscalyears))
    
    # return [get_yearly_quterly_montly_weekly_revenue(fy, df) for fy in fiscalyears]
    return {fy_names[i]:get_yearly_quterly_montly_weekly_revenue(fy, df) for i, fy in enumerate(fiscalyears)}
    
def test_cf(client_id): 
    print(monthly_grouped_barchart_inflow_outflow(client_id)[:2])
    print(monthly_cumulative_inflow_outflow_and_balance(client_id)[:2])
    print(get_monthly_cash_balance(client_id)[:3])   
    #print(get_fiscalyears(client_id))
    print(kpi_inflow_outflow(client_id))
    
if __name__=='__main__':
    
   test_cf('1')