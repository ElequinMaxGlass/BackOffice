# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 14:53:34 2022

@author: marcj
"""

#Net Liquidity Report 
import os 
from os.path import exists, isfile 
import pandas as pd 
import datetime
from pandas.tseries.offsets import BDay
from xbbg import blp
import numpy as np 
from pandas.tseries.offsets import BDay

ticker_sums = {}

pd.options.mode.chained_assignment = None

#first trading day 
startDate = datetime.date(2022, 6, 17)
    
#last trading day
endDate = datetime.date(2022,6,30) 

#starting cash position
startCash = 22477168.38

def has_numbers(inputString):
    return any(char.isdigit() for char in inputString)

def unique(list1):
  
    # initialize a null list
    unique_list = []
  
    # traverse for all elements
    for x in list1:
        # check if exists in unique_list or not
        if x not in unique_list:
            unique_list.append(x)
            
    return unique_list

def not_whitespace(string):
    return not string.isspace()

def strip(string):
    return string.strip()

def side(row):
    if row["Transaction Type (M)"] == "BY":
        return row['Pos - Traded']
    
    return row['Pos - Traded'] * -1

def cancel_replace(row):

    if row["Record Type (M)"] == "N":
        return row['Pos - Traded']
    
    return row['Pos - Traded'] * -1

def cash_side(row):
    if row["Transaction Type (M)"] == "BY":
        return row['Cash'] * -1
    
    return row['Cash'] * 1

def cancel_replace_cash(row):

    if row["Record Type (M)"] == "N":
        return row['Cash']
    
    return row['Cash'] * -1
    
def tradeActivity_positions(addDate):
    
    path = os.path.expanduser('~/OneDrive - QEC Capital/Documents/NAVConsulting/NAV Trade Files/')
    
    arr = os.listdir(path)
    
    df = None
    
    #looks for tradefile by iterating through directory and finding a file that contains HS_lstbday
    for i in reversed(arr):
        if ('HS_' + addDate) in i:
            if len(i) < 44:
                trades = pd.read_csv(path + i)
                
                #only gets state street files 
                df = trades[trades['Prime Broker Account Number (M)'] == 'EQHA']
                
                df['Pos - Traded'] = df['Quantity (M)']
                
                #adjusts for side
                df['Pos - Traded'] = df.apply(side, axis = 1)
                
                #adjusts for cancel
                df['Pos - Traded'] = df.apply(cancel_replace, axis = 1)
                
                
                df['Cash'] = df['Net Amount (M)']
                
                #adjusts for side
                df['Cash'] = df.apply(cash_side, axis = 1)
                
                #adjusts for cancel
                df['Cash'] = df.apply(cancel_replace_cash, axis = 1)
                
                df['Ticker'] = df['Client Product Id (M)']
                
                df['Side'] = df["Transaction Type (M)"]
                
                df['Date'] = df['Trade Date (M)'].apply(str)
                
                df.rename(columns = {'Price (M)' : 'Price'}, inplace = True )
                
                #seperates dataframe by ticker and ISIN, then creates a new Dataframe that combines the two 
                df['isTicker'] = df['Ticker'].apply(lambda x: has_numbers(x))
                
                dfTicker = df[df['isTicker'] == False]
                
                dfCUSIP = df[df['isTicker'] == True]
                dfCUSIP.rename(columns = {'Ticker' : 'CUSIP'}, inplace = True)
                
                df = pd.concat([dfTicker, dfCUSIP])
                
                #creates final dateframe 
                df = df[['Ticker', 'CUSIP', 'Pos - Traded', 'Date', 'Price', 'Side', 'Cash']]
    
    return df

def STT_startingPosition():
    
    #add dynamic function to pull poistion at start of month 
    
    SS_startDate = startDate.strftime('%m%d%Y') #State Street files must be in MMDDYYYY format
    
    path = os.path.expanduser(f'~/OneDrive - QEC Capital/Documents/Operations/dailies/STT/eqha Positions by Category {SS_startDate}.CSV')
    
    df = pd.read_csv(path)
    
    positionDate = (startDate - BDay(1)).strftime('%Y%m%d')
    
    df['Date'] = positionDate
    
    df['CUSIP'] = df['SS Asset ID']
    
    df['QTY at time'] = df['Pos - Traded']
    
    df.rename(columns = {'Local Mkt Price' : 'Price'}, inplace = True)
    
    df = df[['Date', 'Ticker','CUSIP', 'Pos - Traded', 'Price', 'QTY at time']]
    
    return df

def add_dividens(report):
    
    bbg_ticker = []
    
    for i in report['Ticker']:
        if i != '' and pd.isna(i) == False:
            bbg_ticker.append( i + " US Equity")

    bbg_ticker = unique(bbg_ticker)
    
    dividends = blp.dividend(bbg_ticker, start_date = startDate, ) 
    
    dividends['Date'] = dividends['ex_date'] - BDay(1)

    dividends['Date'] = dividends['Date'].apply(lambda x: x.strftime('%Y%m%d'))

    
    dividends.reset_index(inplace = True)
    
    dividends['Ticker'] = dividends['index'].apply(lambda x: x.split()[0])
    
    dividends.drop(columns = ['index'], inplace = True )
    
    dividends.rename(columns = {'dvd_amt' : 'dvd_rate'}, inplace = True)

    return dividends

def add_CUSIP(report):
    
    tickers = unique(report.Ticker.dropna().tolist()) 
    tickers = [x for x in tickers if x != '']
    tickers = map(lambda x: x + " US Equity", tickers)
    
    CUSIPs  = blp.bdp(tickers, 'ID_CUSIP') 
    
    CUSIPs.reset_index(inplace = True)
    
    CUSIPs['Ticker'] = CUSIPs['index'].apply(lambda x: x.split()[0])
    
    CUSIPs.drop('index', inplace=True, axis = 1)
    
    CUSIPs.rename(columns = {'id_cusip' : 'CUSIP'}, inplace = True)
    
    return CUSIPs

def get_last_price(row):
    
    if pd.isnull(row['CUSIP']) == True or pd.isnull(row['Type']):
        return 
    
    security = row['CUSIP'] + ' US ' + row['Type']
    
    security_lp = blp.bdh(security,  start_date = row['Date'], end_date = row['Date'], flds=['last_price'])
    
    if security_lp.empty == True and (row['Type'] == 'Pfd' or row['Type'] == 'Corp'):
        for i in range(30):
            date = datetime.datetime.strptime(row['Date'], "%Y%m%d") - BDay(i)
            security_lp = blp.bdh(security,  start_date = date, end_date = date, flds=['last_price'])
            if security_lp.empty == False:
                break

    #odd case that doesnt pull CHNG        
    if security_lp.empty == True and pd.isnull(row['Ticker']) == False:
        security = row['Ticker'] + " US Equity" 
        security_lp = blp.bdh(security,  start_date = row['Date'], end_date = row['Date'], flds=['last_price'])
    
    if security_lp.empty == True:
        return
    
    security_lp = security_lp.loc[:, security]
    security_lp = security_lp['last_price'][0]
    
    return(security_lp)

def get_value_at_time(row):
    
    #adjusted for corporate bonds
    if row['Type'] == 'Corp': 
        return ((row["QTY at time"] * row['last price']) / 100)
    
    return row["QTY at time"] * row['last price']

def get_security_type(row):
    
    type_list = [' US Equity', ' US Corp', ' US Pfd', ' CN Equity']
    
    security_type = pd.DataFrame()
    
    for i in type_list:
        if security_type.empty == True:
            security = row['CUSIP'] + i 
            security_type = blp.bdp(security, flds=['MARKET_SECTOR_DES'])
    
    #odd case that doesnt pull CHNG value
    if security_type.empty == True and pd.isnull(row['Ticker']) == False:
        security = row['Ticker'] + " US Equity" 
        security_type = blp.bdp(security, flds=['MARKET_SECTOR_DES'])
    
    if security_type.empty == True:
        return
    
    security_type =  security_type['market_sector_des'][0]
    
    return(security_type)

def get_QTY_at_time(row):

    if pd.isna(row["CUSIP"]) or pd.isna(row["Pos - Traded"]) or row['CUSIP'] == '':
        return row['QTY at time'] 
        
    ticker = row["CUSIP"]
    
    if ticker in ticker_sums.keys():
        ticker_sums[ticker] += row["Pos - Traded"]
    else:
        ticker_sums[ticker] = row["Pos - Traded"]
    
    return ticker_sums[ticker]

def get_summary_position(report):
    
    ref = unique(report.CUSIP.dropna().tolist()) 
    
    ref = [x for x in ref if x != '']
    
    positions = []

    for i in ref:
        spot = report[report['CUSIP'] == i].index.values
        if len(spot) != 0: 
            positions.append(spot[-1])
            continue 
        
    positions = unique(positions)

    summary = report.iloc[positions].sort_values(by='Date')

    summary.reset_index(inplace = True, drop = True) 
    
    summary['Date'] = endDate.strftime('%Y%m%d')
    
    summary['last price'] = summary.apply(get_last_price, axis = 1) 
    
    summary['value at time'] = summary.apply(get_value_at_time, axis = 1)
    
    summary.drop(columns = ['Pos - Traded'], inplace = True)
    
    return summary


def main():
    
    date_diff = endDate - startDate
    date_diff = date_diff.days + 1
    
    dateList = [endDate - datetime.timedelta(days=x) for x in range(date_diff)]
    
    start = STT_startingPosition()
    
    counter = 0 
    
    for i in reversed(dateList):
        addDate = i.strftime('%Y%m%d')
        trading = tradeActivity_positions(addDate)
        if counter == 0:
            report = pd.concat([start, trading])
        else: 
            report = pd.concat([report, trading])
        counter += 1
    
    #removes whitespaces form CUSIPS and tickers 
    report['Ticker'] = report['Ticker'].apply(lambda x: x.strip() if (pd.isna(x) == False) else x)
    report['CUSIP'] = report['CUSIP'].apply(lambda x: x.strip() if (pd.isna(x) == False) else x)
    
    CUSIPs = add_CUSIP(report)
    
    report = report.merge(CUSIPs, how = 'left', on=['Ticker'], suffixes = (None, '_y'))
    report['CUSIP'] = report['CUSIP'].fillna(report['CUSIP_y'])
    report.drop('CUSIP_y', inplace=True, axis=1)
    
    report.reset_index(drop = True, inplace = True)
    report.sort_values(by='Date', inplace = True)
    
    report["Type"] = report.apply(get_security_type, axis = 1)
    report['QTY at time'] = report.apply(get_QTY_at_time, axis =1)
    report['last price'] = report.apply(get_last_price, axis=1)
    report["value at time"] = report.apply(get_value_at_time,axis=1)
    
    report = report[['Date', 'Side', 'Type', 'CUSIP', 'Ticker', 'Pos - Traded', 'QTY at time', 'last price', 'value at time', 'Cash' ]]
    
    dividends = add_dividens(report)
    report = report.merge(dividends, how = 'left', on = ['Date', 'Ticker'] )
    report['dvd_amt'] = report['QTY at time'] * report['dvd_rate']
    
    cash_report = report[['Date', 'Side', 'Type', 'CUSIP', 'Ticker', 'Cash']]
    
    report.drop( columns = ['Cash'], inplace = True)
    
    #creats DF with total net cash amount
    cash_summary = cash_report[['Date', 'Cash']].groupby(['Date']).sum()
    
    cash_summary['Cash'].iloc[0] = startCash
    
    print(cash_summary)
    
    #finds daily balaces
    cash_summary['Balance'] = np.cumsum(cash_summary['Cash'])

    summary_position = get_summary_position(report)    

    with pd.ExcelWriter("STT_liquidity.xlsx") as writer:
        report.to_excel(writer, sheet_name="Trading Report Positions")
        cash_report.to_excel(writer, sheet_name = 'Trading Report Cash')
        summary_position.to_excel(writer, sheet_name='Summary Positions')
        cash_summary.to_excel(writer, sheet_name = 'Cash Summary')
        dividends.to_excel(writer, sheet_name="Dividends")
        CUSIPs.to_excel(writer, sheet_name = 'CUSIPs')

    return report

if __name__ == "__main__":
    main()



