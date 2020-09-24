import numpy as np
import pandas as pd
import datetime
import calendar
import os
import warnings
from functools import lru_cache

from utils import read_dataframe_from_pickle
from models.retail import ItemMaster as item_table
from models.retail import MSalesDetailTxns as sales_table
from models.retail import download_columns_from_db
from modules.retail.market_basket_analysis import load_item_dict

PATH = os.path.join(os.path.dirname( __file__ ), 'pickle_data/')

@lru_cache(maxsize=8)
def get_daily_sales_data(client_id):
    
    required_col = ['Datetimestamp', 'Total_sales']
    sales = sales_table.fetch_all_by_client(client_id)[required_col]  # from database
    # sales = read_dataframe_from_pickle(client_id, data_name='sales')[required_col] # from pickle      
    sales['Datetimestamp'] = pd.to_datetime(sales['Datetimestamp'])  
    #print(sales.head())
    sales['date'] = sales['Datetimestamp'].dt.date.astype('datetime64[ns]')
    daily_sales = sales.groupby('date', as_index=False).agg({'Total_sales':np.sum})
    daily_sales['month']= daily_sales.date.dt.month.astype(np.int8)
    daily_sales['week']= daily_sales.date.dt.week.astype(np.int8)
    #daily_sales['month'] = daily_sales['month'].apply(lambda x: calendar.month_abbr[x])
    daily_sales['year'] = daily_sales.date.dt.year.astype(np.int16)
    
    return daily_sales

@lru_cache(maxsize=8)
def load_n_process_salesdetail_data(client_id):    
      
    sales_details = sales_table.fetch_all_by_client(client_id) # from database
    #sales_details = read_dataframe_from_pickle(client_id, data_name='sales') # from pickle
    sales_details['Datetimestamp'] = pd.to_datetime(sales_details['Datetimestamp'])
    sales_details['item_len'] = sales_details['Item_code'].apply(lambda x : len(x))
    sales_details = sales_details[sales_details['item_len'] > 2].copy()
    
    # itemcode2name = {}    
    # for code, name in zip(sales_details.Item_code, sales_details.Item_name):
    #     itemcode2name[code] = name
        #itemname2code[name] = code
    item2idx, idx2item, code2name = load_item_dict(client_id)
        
    return sales_details, code2name


def get_monthly_sales_yearwise_data(client_id):
    """
    input: cleint_id 
    output: [{'2018_sales': 150720.0, '2019_sales': 168919.0, 'month': 1.0},
             {'2018_sales': 161158.0, '2019_sales': 0.0, 'month': 2.0}, ...]
    """
    #print("hear")
    
    df = get_daily_sales_data(client_id)      
    ar = np.zeros((12, 3))    
    now = datetime.datetime.now()
    year = now.year
    
    year_list = df.year.value_counts().index.tolist()
    #print(year_list)
    # current year not in data set
    if year not in year_list:
        year = max(year_list)  
          
    monthly_sales = df.groupby(['year', 'month'], as_index=False).agg({'Total_sales':np.sum})    
    df = monthly_sales[monthly_sales['year'] >= year-1].copy()
    df['Total_sales'] = round(df['Total_sales'])    
    
    ar[:, 0] = np.arange(1, 13)
    temp = df[df['year'] == year-1]
    for month,sales in zip(temp['month'],temp['Total_sales']):       
        ar[month-1,1]  = sales                  
    temp = df[df['year'] == year]
    #print(temp)
    for month,sales in zip(temp['month'],temp['Total_sales']): 
        ar[month-1,2]  = sales   
   
    
    temp = pd.DataFrame(ar, columns=['month', str(year-1)+'_sales', str(year)+'_sales'])
    
    #print(df)
    return  temp.to_dict(orient='records')


def get_quarterly_monthly_weekly_sales(client_id, df, year=2018):
   
    #df = get_daily_sales_data(client_id)
    df = df[df['year'] == year].copy()
    df['month'] = df['month'].apply(lambda x: calendar.month_abbr[x]) 
    df['quarter'] = df.date.dt.quarter
    
    def sales_aggregate(time_period):
        temp = df.groupby([time_period], as_index=False).agg({'Total_sales':np.sum}). \
                 rename(columns ={'Total_sales':f'{time_period}ly_sales'})
        return temp.round().to_dict(orient='records')         
    
    res = {'weekly':sales_aggregate('week'), 
           'monthly': sales_aggregate('month'), 
           'quarterly': sales_aggregate('quarter')}
   
    return res

def get_quterly_monthly_weekly_sales_curr_prev_year(client_id):
    
    """
    for interactice quaterly pie chart
    {2019:
       {'weekly': [{'week': 1.0, 'weekly_sales': 28709.0},
                  {'week': 2.0, 'weekly_sales': 37870.0},...]
       'monthly': [{'mm-yyyy': 'Jan-2019', 'monthly_sales': 162533.0},
                   {'mm-yyyy': 'Feb-2019', 'monthly_sales': 165203.0},...]
       'quarterly': [{'quarter': 1.0, 'quarterly_sales': 519100.0},
                    {'quarter': 2.0, 'quarterly_sales': 223611.0}]},..]
    """
    now = datetime.datetime.now()
    year = now.year
   
    monthlst = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']    
    df = get_daily_sales_data(client_id)
    year_list = df.year.value_counts().index.tolist()
    #print(year_list)
    if year not in year_list:
        year = max(year_list)
    curr_year = get_quarterly_monthly_weekly_sales(client_id, df, year) 
    prev_year = get_quarterly_monthly_weekly_sales(client_id, df, year-1) 
    
    for month in monthlst:
        if month not in [m['month'] for m in prev_year['monthly']]:            
            d = {'month': month, 'monthly_sales': 0}
            prev_year['monthly'].append(d)
                         
    for week in range(1,53):
        if week not in [w['week'] for w in prev_year['weekly']]:
            d = {'week': week, 'weekly_sales': 0}
            prev_year['weekly'].append(d)
    prev_year['weekly'] = sorted(prev_year['weekly'], key=lambda k: k['week'])    
    
    return {year: curr_year,
            year-1: prev_year}


def get_monthly_sales_data(client_id):
    """
    for grouped bar chart
    input: daily_sales data dataframe
    output: monthly sales data for prev & current years 
    [{'mm-yyyy': 'Jan-2018', 'monthly_sales': 150720.0},
             {'mm-yyyy': 'Feb-2018', 'monthly_sales': 161158.0},, ...]
    """
    df = get_daily_sales_data(client_id)
    ar = np.zeros((12, 3))    
    now = datetime.datetime.now()
    year = now.year
    year_list = df.year.value_counts().index.tolist()
    #print(year_list)
    if year not in year_list:
        year = max(year_list)
        
    monthly_sales = df.groupby(['year', 'month'], as_index=False).agg({'Total_sales':np.sum})    
    df = monthly_sales[monthly_sales['year'] >= year-1].copy()
    df['month'] = df['month'].apply(lambda x: calendar.month_abbr[x])
     
    df['mm-yyyy']  = df['month'] + '-' +df['year'].astype(str)
    df['monthly_sales'] = round(df['Total_sales'])
    df = df.drop(['year', 'month', 'Total_sales'], axis=1)
    
    #print(df)
    return  df.to_dict(orient='records')

def topK_items_based_on_sales(client_id, k):    
    
    df, itemcode2name = load_n_process_salesdetail_data(client_id)
    #print(df.head(4))
    #print(df.dypes)
    itemwise_sales = df.groupby(['Item_code'], as_index=False). \
                                   agg({'Unitsale_price':np.average, 'Sale_quantity': np.sum})
    
    itemwise_sales['totalsales'] = round(itemwise_sales['Unitsale_price'] * itemwise_sales['Sale_quantity'], 2)
    itemwise_sales = itemwise_sales.sort_values(by='totalsales', ascending=False).head(k)
    itemwise_sales.insert(1, 'Item_name', itemwise_sales['Item_code'].apply(lambda x: itemcode2name[x]))
    itemwise_sales['item_id'] = list(range(1, k+1))
    return itemwise_sales[['item_id','Item_name', 'totalsales']].to_dict(orient='records')


def top20_items_based_on_sales(client_id):
    
    """
    input: sales details dataframe, itemcode2name dict
    output: json [{'totalsales': 34396.74, 'Item_name': 'TITO S HANDMADE REGSTORE VODKA 1.75LT'},
                 {'totalsales': 31957.07, 'Item_name': 'MOET  CHANDON IMP 750ML'}, ....]
    """
    return topK_items_based_on_sales(client_id, 20)


def top10_items_based_on_sales(client_id):
    
    """
    input: sales details dataframe, itemcode2name dict
    output: json [{'totalsales': 34396.74, 'Item_name': 'TITO S HANDMADE REGSTORE VODKA 1.75LT'},
                 {'totalsales': 31957.07, 'Item_name': 'MOET  CHANDON IMP 750ML'}, ....]
    """
    return topK_items_based_on_sales(client_id, 10)

def top10_items_based_on_profit(client_id):
    """
    input: sales details dataframe, itemcode2name dict
    output: json [{'profit': 4969.89, 'Item_name': 'SUPERME ZEN 3500MG'},
                  {'profit': 4089.0, 'Item_name': 'LVOV REG 1.75LT'}, ....]
    """
    df, itemcode2name = load_n_process_salesdetail_data(client_id)
    itemwise_profit = df.groupby(['Item_code'], as_index=False). \
                        agg({'Unitsale_price':np.average, 'Unitcost_price':np.average, 'Sale_quantity': np.sum})

    itemwise_profit['profit'] = round(((itemwise_profit['Unitsale_price'] - itemwise_profit['Unitcost_price'])
                             * itemwise_profit['Sale_quantity']), 2)
    itemwise_profit = itemwise_profit.sort_values(by='profit', ascending=False).head(10)
    itemwise_profit.insert(1, 'Item_name', itemwise_profit['Item_code'].apply(lambda x: itemcode2name[x]))
    
    return itemwise_profit[['Item_name', 'profit']].to_dict(orient='records')


def get_total_sales(client_id,  year=2018):
    #chatbot
    "input: df daily_sales=>dataframe,  year"
    "output:daily or monthly sales for the year "
    df = get_daily_sales_data(client_id)
    year_list = df.year.value_counts().index.tolist()
    print(year_list)
    if year not in year_list:
        return {'error': f'year should be between {min(year_list)} and {max(year_list)}'}
    df = df[df['year']== year]
    total_sales = df['Total_sales'].sum().round()
    res = {'total_sales': total_sales}
    return res


def get_average_sales(client_id, time_periods, year=2018):
    #chatbot
    "input: df daily_sales=>dataframe, time_periods=>date, month, year"
    "output:daily or monthly sales for the year "
    df = get_daily_sales_data(client_id)
    year_list = df.year.value_counts().index.tolist()
    print(year_list)
    if year not in year_list:
        return {'error': f'year should be between {min(year_list)} and {max(year_list)}'}
    df = df[df['year']== year]
    
    if time_periods == 'daily':
        avg = df['Total_sales'].mean().round()
        res = {'avg_sales': avg}
        
    elif time_periods == 'weekly':        
        avg_df = df.groupby('week', as_index=False).agg({'Total_sales':np.sum})
        avg = avg_df['Total_sales'].mean().round()
        res = {'avg_sales': avg}
        
    elif time_periods == 'monthly':
        avg_df = df.groupby('month', as_index=False).agg({'Total_sales':np.sum})
        avg = avg_df['Total_sales'].mean().round()
        res = {'avg_sales': avg}
    else:
        res = {'error': f"some problem with time periods {time_periods}"}
        
    return res

def get_aggrigate_quarter_month_week(client_id, year=2018):
    
    """
    for chatbot    
    [{'monthly_sales': [{'mm-yyyy': 'Jan-2018', 'monthly_sales': 150720.0},...]
    {'quarterly_sales': [{'quarter': 1, 'quarterly_sales': 504424.0},]
    """
    df = get_daily_sales_data(client_id)
    return get_quarterly_monthly_weekly_sales(client_id, df, year) 


def test_visual(client_id):
    print('grouped bar chart') 
    print(get_monthly_sales_yearwise_data(client_id)[:2])
    print('monthly sales trend line chart' )
    print(get_monthly_sales_data(client_id)[:2])
    print('bar chart')
    print(top10_items_based_on_sales(client_id)[:2])
    print('bar chart')
    print(top20_items_based_on_sales(client_id)[:2])
    print('bar chart')
    print(top10_items_based_on_profit(client_id)[:2])
    print("interactive quarter & month")
    print(get_quterly_monthly_weekly_sales_curr_prev_year(client_id)[2019]['quarterly'])


if __name__=='__main__':
    
    test_visual('1')
