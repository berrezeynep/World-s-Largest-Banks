# -*- coding: utf-8 -*-
"""
Created on Sat Mar  2 18:40:38 2024

@author: berre
"""

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

#log progress

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
        
#extract
        
def extract(url, table_attribs):
    df=pd.DataFrame(columns=table_attribs)
    page=requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    tables=data.find_all('tbody')
    rows=tables[0].find_all('tr')
    for row in rows:
        col=row.find_all('td')
        if len(col)!=0:
                data_dict={"Name":col[1].find_all('a')[-1].contents[0],"MC_USD_Billion":col[2].contents[0]}
                df1=pd.DataFrame(data_dict,index=[0])
                df=pd.concat([df,df1],ignore_index=True)
      
    df['MC_USD_Billion']=df['MC_USD_Billion'].str.replace('\n',"").str.replace(',',"")
    df['MC_USD_Billion']=df['MC_USD_Billion'].astype("float")

    return df

#transform
 
def transform(df,csv_path):
    exchange_rate_df = pd.read_csv("exchange_rate.csv")
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df


def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
        

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df,csv_path)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table, Executing queries')

query_statement1=f"SELECT * FROM {table_name}" 
query_statement2=f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
query_statement3=f"SELECT \"Name\" from {table_name} LIMIT 5" 

run_query(query_statement1, sql_connection)
run_query(query_statement2, sql_connection)
run_query(query_statement3, sql_connection)
log_progress('Process Complete.')

sql_connection.close()
log_progress('Server Connection closed')
