import sys
import os
import glob

from airflow import DAG
from airflow.models import Variable

from airflow.hooks.S3_hook import S3Hook

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.utils.dates import days_ago

from datetime import datetime

#from airflow import settings
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


from airflow.models.connection import Connection
from airflow import settings
import json
import yaml

Title="Load Fundamental Data"

HOME = os.environ["AIRFLOW_HOME"] # retrieve the location of your home folder
Temp_Data=Variable.get('My_Temp_Data') 

Temp_Data_full_path=os.path.join(HOME,'dags', Temp_Data)

dest_bucket=Variable.get('OHLC_data_bucket')  
fundamental_latest_date_folder=Variable.get('fundamental_latest_date_folder') 

dbt_project='dbt_json_transformations'
dbt_project_home = Variable.get(dbt_project)
dbt_path = os.path.join(HOME,'dags', dbt_project_home)
manifest_path = os.path.join(dbt_path, "target/manifest.json") # path to manifest.json

#path to SQL files used in the orchestration
tickers_need_fundamental_load_sql_path = os.path.join(dbt_path, f"target/compiled/{dbt_project}/analyses/tickers_need_fundamental_load.sql")




with open(manifest_path) as f: # Open manifest.json
        manifest = json.load(f) # Load its contents into a Python Dictionary
        nodes = manifest["nodes"] # Extract just the nodes

dbt_conn_id = "dbt_snowflake_connection"
api_conn_id = "http_gurufocus_eod"


def SyncConnection():
   with open(os.path.join(dbt_path,'profiles.yml')) as stream:
        try:  
            profile=yaml.safe_load(stream)[dbt_project]['outputs']['dev']

            conn_type=profile['type']
            username=profile['user']
            password=profile['password']    
            host=f'https://.{profile['account']}snowflakecomputing.com/'
            role=profile['role']                     
            account=profile['account']
            warehouse=profile['warehouse']
            database=profile['database']
            schema=profile['schema']
            extra = json.dumps(dict(account=account, database=database,  warehouse=warehouse, role=role))

            session = settings.Session()           

            try:
                
                new_conn = session.query(Connection).filter(Connection.conn_id == dbt_conn_id).one()
                new_conn.conn_type = conn_type
                new_conn.login = username
                new_conn.password = password
                new_conn.host= host
                new_conn.schema = schema 
                new_conn.extra = extra
                                    
                
            except:

                new_conn = Connection(conn_id=dbt_conn_id,
                                  conn_type=conn_type,
                                  login=username,
                                  password=password,
                                  host=host,
                                  schema=schema,
                                  extra = extra
                                     )
                
            
            session.add(new_conn)
            session.commit()    

    

        
        except yaml.YAMLError as exc:
            print(exc)
#-----------------------------------------------------------------------------------------------

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

def Get_Fundamental_Data():

    # Get API Key and URL from the connection
    session = settings.Session() 
    conn = session.query(Connection).filter(Connection.conn_id == api_conn_id).one()
    api_key = json.loads(conn.extra)["key"]
    api_url = json.loads(conn.extra)["url"]

    pg_hook = SnowflakeHook(snowflake_conn_id=dbt_conn_id)
    with open(tickers_need_fundamental_load_sql_path) as f:
        sql = f.read()
        records = pg_hook.get_records(sql)  
        for record in records:  
            # Get fundamental data for ticker
            url = api_url%(api_key,record[0])
            http_hook = HttpHook(method='GET', http_conn_id=api_conn_id)
            response_data = http_hook.run(url)

            # Store data file into s3 bucket
            dest_key=Variable.get('fundamental_data_folder') + record[0] + '.json'
            s3_hook = S3Hook(aws_conn_id='aws_default')
            s3_hook.load_bytes(response_data.content,
                       dest_key,
                       bucket_name=dest_bucket,
                       replace=True)        

    
    



dag = DAG(

    'LoadFundamentalData_dag',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 23 * * *',

    catchup=False

)



Sync_dbt_Connection = PythonOperator(
    task_id="Sync_dbt_Connection",
    python_callable=SyncConnection,
    dag=dag
)

Web_Scraping_latest_available_fund_date = PythonOperator(
    task_id='Web_Scraping_latest_available_fund_date',
    python_callable=scraping_latest_available_fundamental_date,
    dag=dag
)

Refresh_Staging_table_latest_available_fund_date = SQLExecuteQueryOperator(
                        task_id="Refresh_Staging_table_latest_available_fund_date",
                        conn_id=dbt_conn_id,
                        sql="""
                            ALTER EXTERNAL TABLE  MYTEST_DB.STOCKS.fundamental_latest_date REFRESH;
                            """,
                            dag=dag)

dbt_Compile_Tickers_Need_Fund_Load_sql = BashOperator(
    task_id="dbt_Compile_Tickers_Need_Fund_Load_sql",
    bash_command='cd %s && dbt compile  --select tickers_need_fundamental_load'%dbt_path,
    dag=dag
        )  

API_Get_Tickers_Fundamental_Data = PythonOperator(
    task_id='API_Get_Tickers_Fundamental_Data',
    python_callable=Get_Fundamental_Data,
    dag=dag
        )  

Refresh_Staging_table_Fundamental = SQLExecuteQueryOperator(
                        task_id="Refresh_Staging_table_Fundamental",
                        conn_id=dbt_conn_id,
                        sql="""
                            ALTER EXTERNAL TABLE  MYTEST_DB.STOCKS.fundamental_stg REFRESH;
                            """,
                            dag=dag)

dbt_Parse_Json_To_Snowflake_Tables = BashOperator(
    task_id="dbt_Parse_Json_To_Snowflake_Tables",
    bash_command='cd %s && dbt run'%dbt_path,
    dag=dag
        )  

Sync_dbt_Connection >> Web_Scraping_latest_available_fund_date >> Refresh_Staging_table_latest_available_fund_date >> dbt_Compile_Tickers_Need_Fund_Load_sql >> API_Get_Tickers_Fundamental_Data >> Refresh_Staging_table_Fundamental >> dbt_Parse_Json_To_Snowflake_Tables