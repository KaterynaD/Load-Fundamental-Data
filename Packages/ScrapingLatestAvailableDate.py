#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import os
import glob

import pandas as pd
import numpy as np

from datetime import datetime
import time


# In[2]:


from bs4 import BeautifulSoup
import urllib.request
from urllib.request import Request, urlopen
from io import StringIO

# In[3]:


from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
import itertools


# In[4]:



# In[5]:





# In[6]:


regular_load_delay=2

attempts=10
delay_when_somethong_wrong=60
save_every_n=100



# In[7]:






# In[8]:


num_threads=3
chunk_size=2




# In[9]:

Metric = 'SharesOutstanding(EOP)'
Base_URL = 'https://www.gurufocus.com/term/BS_share/%s/Shares-Outstanding-(EOP)/'
TempFolder = 'Temp/'
Tickers_all = []
chunk_size = 0

# In[10]:


def CleanUpTempFolder():
    files = glob.glob(os.path.join(TempFolder, 'fundamental*.csv'))
    for f in files:
        os.remove(f)


# In[11]:


def get_table(f,tables,ticker):
    #0 - annual
    #1 - quarterly
    if f not in [0,1]:
        return None
    try:
        df=pd.read_html(StringIO(str(tables)))[f].T.reset_index(drop=True).tail(5)
        df.columns=['Date',Metric]
        df['Ticker']=ticker
        df['LoadDate']=datetime.now()
        df=df[['Ticker','Date',Metric,'LoadDate']]
        return df
    except:
        return None

def get_data(ticker):
    
    url=Base_URL%ticker
    url=url.replace(' ','%20')
    req = Request(
    url=url, 
    headers={'User-Agent': 'Mozilla/5.0'}
                )
    html_page = urllib.request.urlopen(req)
    soup = BeautifulSoup(html_page, 'html.parser')
    #KD - 4/12/2024: There is an issue in GuruFocus html: a table inside a table has repeted </tr></table></div> but no </tr></table></div> at the end of the outer table
    cleanSoup_str=str(soup)
    cleanSoup_str=str(soup).replace('<div style="margin: 5px 10px 0 0"><table cellpadding="0" cellspacing="0.5" style="border-collapse:separate!important;float: right;" title="Trend"><tr class="trend_normal" style="width:25px !important; border: none !important; outline: none"></tr></table></div></td></tr></table></div>', '</td>',1)
    cleanSoup_str=cleanSoup_str.replace('<div style="width:100% !important; overflow-x:auto;">','</tr></table><div style="width:100% !important; overflow-x:auto;">')
    cleanSoup = BeautifulSoup(cleanSoup_str,'lxml')
    #
    tables = cleanSoup.find_all('table')
    
    
    
    df_quarterly=get_table(1,tables,ticker)
    
    return (df_quarterly)


# In[12]:


def run(Tickers_chunk,Tickers_chunk_num):
    def SaveData():
                
        if len(df_quarterly_all)>0:
            df_quarterly_all.to_csv(os.path.join(TempFolder, 'fundamental_'+str(Tickers_chunk_num)+'.csv'), header=True, index=False)  
               
    #-----------------------------------------------------------------------------------------
    print(f'Starting processing chunk #{Tickers_chunk_num}!')

    df_quarterly_all = pd.DataFrame()
    
    for idx,t in enumerate(Tickers_chunk):
        #print(t, end = '.'),
        cnt=1
        print(t)
        while cnt<=attempts:
            try:
                df_quarterly = get_data(t)
                                  
                if not(df_quarterly is None):
                    df_quarterly_all=pd.concat([df_quarterly_all, df_quarterly], ignore_index=True)                   
                  
                if (idx % save_every_n) == 0:
                    SaveData()
                
                #and quit the loop 
                time.sleep(regular_load_delay)
                cnt=attempts+1
            except Exception as e:
                print('Something went wrong...'+ str(e))
                cnt=cnt+1
                print('Waiting...')
                time.sleep(delay_when_somethong_wrong)
                print(f'Attempt to read ticker {t} # {cnt}')
        if cnt>attempts+1:
            print('Too many false attempts. Stop execution...')
 
            break
        
    SaveData() 
    print(f'Chunk #{Tickers_chunk_num} is done!')


# In[13]:


#https://superfastpython.com/threadpoolexecutor-in-python/
def RefreshMetric():
    with ThreadPoolExecutor(num_threads) as executor:
        Tickers_chunk_par=list()
        chunk_num_par=list()
        for idx, chunk in enumerate(itertools.zip_longest(*[iter(Tickers_all)]*chunk_size)):
            Tickers_chunk=[x for x in list(chunk) if x is not None]
            chunk_num=idx
            Tickers_chunk_par.append(Tickers_chunk)
            chunk_num_par.append(chunk_num)
        results=executor.map(run,Tickers_chunk_par, chunk_num_par)


# In[14]:




def RefreshMetricSingleThread():
    for idx, chunk in enumerate(itertools.zip_longest(*[iter(Tickers_all)]*chunk_size)):
        Tickers_chunk=[x for x in list(chunk) if x is not None]
        chunk_num=idx
        run(Tickers_chunk,chunk_num)



# In[17]:

def StartLoad(param_TempFolder, param_tickers_full_filename, param_Metric, param_Base_URL):
    global Metric 
    global Base_URL 
    global TempFolder 

    global Tickers_all 
    global chunk_size 

    Metric = param_Metric
    Base_URL = param_Base_URL
    TempFolder = param_TempFolder

    Tickers_all = pd.read_csv(param_tickers_full_filename)['ticker'].tolist()
    chunk_size =int(len(Tickers_all)/(num_threads-1))


      
    #
    CleanUpTempFolder()
    #
    RefreshMetric()
    #RefreshMetricSingleThread()

#if __name__ == '__main__':
#    StartLoad('/home/kdlab1/Projects/DataEngineering/airflow/dags/LoadFundamentalData/Temp', '/home/kdlab1/Projects/DataEngineering/airflow/dags/LoadFundamentalData/Temp/tickers.csv','SharesOutstanding(EOP)','https://www.gurufocus.com/term/BS_share/%s/Shares-Outstanding-(EOP)/')