import os
import calendar
import numpy as np
import pandas as pd
from typing import List, Dict

from modules.cash_flow.view_360  import load_process_data


def clientwise_inflow_in_percent(client_id:str)->List:
    """
    clientwise percent of inlflow 
    input=> client_id:str
    ouput=> List of dict 
    [{"particulars": "Fintellix Solutions Pvt. Ltd (Formerly ICreate)", "%-inflow": 0.2}, 
    {"particulars": "Collabera Technologies Private Limited", "%-inflow": 0.18}, ...]
    """
    
    df = load_process_data(client_id)
    fy = df['date'].dt.to_period('A-MAR').astype(str).astype(int).unique().tolist()
    fy = sorted(fy, reverse=True)
    fiscalyears = list(map(lambda y: f'{y-1}-{y%1000}', fy))    
    df = df[df['date'].dt.to_period('A-MAR')==fy[0]]
    
    inflow = df[df['debit'] != 0][['date', 'particulars', 'debit']]
    inflow['particulars'] = inflow['particulars'].apply(lambda x: ' '.join(x.split()[:2]))
    inflow_agg = inflow.groupby('particulars', as_index=False).agg({'debit':'sum'})
    inflow_agg = inflow_agg.sort_values('debit', ascending=False)
    inflow_agg['%-inflow'] = round(inflow_agg['debit']/inflow_agg['debit'].sum(), 2)
    
    res = inflow_agg.head(25)
    return {
        'year':fiscalyears[0],
        'data':res[['particulars', '%-inflow']].to_dict(orient='records')
        }


def headwise_outflow_in_percent(client_id:str)-> List:
    """
    outflow in percent under different heads
    input=> client_id:str
    output=> List of dict 
    {"particulars": "Salary Payable", "%-outflow": 0.553}, 
    {"particulars": "Electronic Cash Ledger", "%-outflow": 0.111}, ...]
    """  
    df = load_process_data(client_id)  
    fy = df['date'].dt.to_period('A-MAR').astype(str).astype(int).unique().tolist()
    fy = sorted(fy, reverse=True)
    fiscalyears = list(map(lambda y: f'{y-1}-{y%1000}', fy))    
    df = df[df['date'].dt.to_period('A-MAR')==fy[0]]
    
    outflow = df[df['credit'] != 0][['date', 'particulars', 'credit']]
    outflow_agg = outflow.groupby('particulars', as_index=False).agg({'credit':'sum'})
    outflow_agg = outflow_agg.sort_values('credit', ascending=False)
    outflow_agg['%-outflow'] = round(outflow_agg['credit']/outflow_agg['credit'].sum(), 3)
    res = outflow_agg.head(20)
    
    return {
        'year': fiscalyears[0],
        'data': res[['particulars', '%-outflow']].to_dict(orient='records')
        }

def monthly_topk_client_revenue(client_id:str, k:int=5)->Dict:
    """
    input: cliend_id, k topk
    output:{"finyear": "2018-19", 
            "data": [{"Apr-2018": [{"client": "Fintellix Solutions", "revenue": 1082440.0},...]
                     {"Aug-2018": [{"client": "Collabera Technologies", "revenue": 1950046.9}...] ...]}
    """                               
    df = load_process_data(client_id)
    df = df[df['debit'] != 0][['date','month', 'year', 'particulars', 'debit']]   
    fy = df['date'].dt.to_period('A-MAR').astype(str).astype(int).unique().tolist()
    fy = sorted(fy, reverse=True)
    #print(fy)
    fiscalyears = list(map(lambda y: f'{y-1}-{y%1000}', fy))
    
    df = df[df['date'].dt.to_period('A-MAR')==fy[0]] 
    df = df[df['particulars'] != 'Opening Balance']
    
    df['month'] = df['month'].apply(lambda x: calendar.month_abbr[x])
    df['mm_yyyy'] = df['month'] +'-'+ df['year'].astype(str)
    df.drop(['date', 'month', 'year'], inplace=True, axis=1)   
    df['particulars'] = df['particulars'].apply(lambda x: ' '.join(x.split()[:2]))
    
    df_agg = df.groupby(['mm_yyyy', 'particulars']).agg({'debit':'sum'})
    temp = df_agg['debit'].groupby(level=0, group_keys=False).nlargest(k).reset_index('particulars')            
    temp.columns = ['client', 'revenue']
    lst = []
    month_idxs = temp.index.unique().tolist()
    for m in month_idxs:
        lst.append({m:temp[temp.index==m][['client', 'revenue']].to_dict(orient='records')})
    
    return {'finyear':fiscalyears[0], 'data': lst}


def monthly_topk_expense(client_id:str, k:int=5)->Dict:
    """
    input: cliend_id, k topk
    output:{"finyear": "2018-19", 
            "data": [{"Apr-2018": [[{'particulars': 'Salary Payable', 'expense': 2536830.0},,...]
                     {"Aug-2018": [{'particulars': 'Salary Payable', 'expense': 2691122.0},...] ...]}
    """                               
    df = load_process_data(client_id)
    df = df[df['credit'] != 0][['date','month', 'year', 'particulars', 'credit']]   
    fy = df['date'].dt.to_period('A-MAR').astype(str).astype(int).unique().tolist()
    fy = sorted(fy, reverse=True)
    #print(fy)
    fiscalyears = list(map(lambda y: f'{y-1}-{y%1000}', fy))
    
    df = df[df['date'].dt.to_period('A-MAR')==fy[0]]     
    
    df['month'] = df['month'].apply(lambda x: calendar.month_abbr[x])
    df['mm_yyyy'] = df['month'] +'-'+ df['year'].astype(str)
    df.drop(['date', 'month', 'year'], inplace=True, axis=1)
   
    df['particulars'] = df['particulars'].apply(lambda x: ' '.join(x.split()[:2]))
    df_agg = df.groupby(['mm_yyyy', 'particulars']).agg({'credit':'sum'})
    
    temp = df_agg['credit'].groupby(level=0, group_keys=False).nlargest(k).reset_index('particulars')            
    temp.columns = ['particulars', 'expense']
    lst = []
    month_idxs = temp.index.unique().tolist()
    for m in month_idxs:
        lst.append({m:temp[temp.index==m][['particulars', 'expense']].to_dict(orient='records')})
    
    return {'finyear':fiscalyears[0], 'data': lst}

def get_customer_list(client_id:str)-> Dict[int, str]:
    """
    input: client_id
    output: Dict[int, str]
    {0: 'Iquanti India Private Limited',
     1: 'RPS Consulting Private Limited',
     2: 'M/s. Think N Solutions',
     3: 'Monsanto Holdings Pvt Ltd.,' ..}
    """
    
    df = load_process_data(client_id)
    df = df[df['debit'] != 0][['date','month', 'year', 'particulars', 'debit']]  
    df['particulars'] = df['particulars'].apply(lambda x: ' '.join(x.split()[:4]))
    
    customer_list = df['particulars'].unique().tolist()
    non_cust = ['Opening Balance', 'Other Income', 'Bank Charges', 'Income Tax Refund Due', 
                'Debtors (Transfer)', 'Capital - Rajesh Kumar', 'Salary Payable', 'Electricity / Water &']
    customer_list = [cust for cust in customer_list if cust not in non_cust]
    cust_dict = {i:c for i, c in enumerate(customer_list)}
    
    return cust_dict


def get_customer_monthly_revenue(client_id:str, customer_name:str)->Dict:
    
    """
    input: client_id string, customer_name string
    output: Dict
    {"customer_name": "M/s. Think N Solutions",
    "data": [{"mm_yyyy": "April-2017", "revenue": 40500.0}, ..
    """
    df = load_process_data(client_id)
    df = df[df['debit'] != 0][['date','month', 'year', 'particulars', 'debit']]  
    df['particulars'] = df['particulars'].apply(lambda x: ' '.join(x.split()[:4]))
    
    cust_montly_agg = df.groupby([df.date.dt.strftime('%B-%Y'), 'particulars']). \
                        agg({'debit':sum}).reset_index(). \
                        rename(columns={'date':'mm_yyyy', 'debit':'revenue'})
    
    res = cust_montly_agg[cust_montly_agg['particulars'] == customer_name]
    
    return {'customer_name': customer_name, 
            'data':res[['mm_yyyy', 'revenue']].to_dict(orient='records')}


def test_inoutflow(client_id):
    
    print(clientwise_inflow_in_percent(client_id)[:3])
    print(headwise_outflow_in_percent(client_id)[:3])
    print(monthly_topk_client_revenue(client_id)['data'][:1])
    print()
    print(monthly_topk_expense(client_id)['data'][:1])
    print()
    cust_dict = get_customer_list(client_id)
    print(cust_dict[2])
    print(get_customer_monthly_revenue(client_id, cust_dict[2]))
    
if __name__=='__main__':
    
    test_inoutflow('1')