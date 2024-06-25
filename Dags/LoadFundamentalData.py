import sys
import os
import glob

from airflow import DAG
from airflow.models import Variable

from airflow.hooks.S3_hook import S3Hook

from airflow.operators.python_operator import PythonOperator


from airflow.utils.dates import days_ago

from datetime import datetime

#from airflow import settings
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

#from airflow.models.connection import Connection

Title="Load Fundamental data"

HOME = os.environ["AIRFLOW_HOME"] # retrieve the location of your home folder
Temp_Data=Variable.get('My_Temp_Data') 

Temp_Data_full_path=os.path.join(HOME,'dags', Temp_Data)

dest_bucket=Variable.get('OHLC_data_bucket')  
fundamental_latest_date_folder=Variable.get('fundamental_latest_date_folder') 

 


def print_welcome():

    print('Welcome to Airflow!')

def scraping_latest_available_fundamental_date():
    from LoadFundamentalData.Packages.ScrapingLatestAvailableDate import StartLoad
    Metric = Variable.get('Metric') 
    Base_URL = Variable.get('Base_URL')
    tickers_filename=Variable.get('Ticker_filename') 
    tickers_full_filename=os.path.join(Temp_Data_full_path,  tickers_filename)
    #StartLoad(Temp_Data_full_path, tickers_full_filename, Metric, Base_URL)
    #
   
    files = glob.glob(os.path.join(Temp_Data_full_path, 'fundamental*.csv'))
    for f in files:        
        dest_key=Variable.get('fundamental_latest_date_folder') + '/' + os.path.basename(f)
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_file(f,
                       dest_key,
                       bucket_name=dest_bucket,
                       replace=True)

def get_tickers_to_download_fundamental():
    pg_hook = SnowflakeHook(snowflake_conn_id=dbt_conn_id)
    with open(growingstocks_report_sql_path) as f:

        
def get_api_data():
    # Create a connection to the source server
    #conn = Connection(conn_id='http_conn_eod',
    #                  conn_type='http',
    #                  host='https://eodhistoricaldata.com')  
    #session = settings.Session()  # get the session
    #session.add(conn)
    #session.commit()

    # Get the data file
    url = 'api/fundamentals/%s?api_token=%s'%('AAPL','demo')
    http_hook = HttpHook(method='GET', http_conn_id='http_conn_eod')
    response_eod_data = http_hook.run(url)

    # Store data file into s3 bucket
    dest_bucket='kd-projects'
    dest_key='fundamental/AAPL.json'
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_bytes(response_eod_data.content,
                       dest_key,
                       bucket_name=dest_bucket,
                       replace=True)



dag = DAG(

    'LoadFundamentalData_dag',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 23 * * *',

    catchup=False

)



print_welcome_task = PythonOperator(

    task_id='print_welcome',

    python_callable=print_welcome,

    dag=dag
)

web_scraping_task = PythonOperator(

    task_id='web_scraping_task',

   python_callable=scraping_latest_available_fundamental_date,

    dag=dag
)


#get_api_data_task = PythonOperator(
#
#    task_id='get_api_data_task',
#
#    python_callable=get_api_data,
#
#    dag=dag
#
#)

print_welcome_task >> web_scraping_task