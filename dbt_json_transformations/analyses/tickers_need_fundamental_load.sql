with GF_data as (
select
ticker,
max(to_date(Date,'MONYY')) Latest_Available_Date
from  {{ source('Tickers_Data', 'fundamental_latest_date') }}
group by ticker
)
,DB_data as (
select 
ticker, 
max(to_date("Fiscal Year",'YYYY-MM')) Latest_Available_Date
from {{ ref('quarterly_income_statement') }}
group by ticker
)
select 
GF_data.ticker
from GF_data
left outer join DB_data
on GF_data.ticker = DB_data.ticker
where GF_data.Latest_Available_Date > DB_data.Latest_Available_Date
or DB_data.Latest_Available_Date is null