{% macro flatten_json(key1,key2) %}

{% set json_column_query %}

with stg as (select * from {{ source('Json_Data', 'fundamental_stg') }} limit 1)
select distinct json.key as column_name
from  stg,
lateral flatten(stg.value:financials:{{ key1 }}:{{ key2}}) json


{% endset %}

{% set results = run_query(json_column_query) %}

{% if execute %}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

with stg_data as (
select file_name_part ticker,value from {{ source('Json_Data', 'fundamental_stg') }} 
)
,d1 as (
select 
ticker,
INDEX IDX,
f.value::varchar Fiscal_Year
from 
stg_data, 
table(flatten(stg_data.value:financials:{{ key1 }}:"Fiscal Year")) f
)
,d2 as (
select 
ticker,
INDEX IDX,
f.value::varchar Preliminary
from 
stg_data, 
table(flatten(stg_data.value:financials:{{ key1 }}:"Preliminary")) f
)


{% for column_name in results_list %}

,"d_{{ column_name }}" as (
select 
ticker,
INDEX IDX,
f.value as "{{ column_name }}"
from 
stg_data, 
table(flatten(stg_data.value:financials:{{ key1 }}:{{ key2 }}:"{{ column_name }}")) f
)


{% endfor %}




select
d1.ticker,
d1.Fiscal_Year::VARCHAR(10) as "Fiscal Year",
d2.Preliminary::INTEGER as "Preliminary",

{% for column_name in results_list %}

"d_{{ column_name }}"."{{ column_name }}"::VARCHAR(20) as "{{ column_name }}" {% if not loop.last %} , {% endif %}

{% endfor %}

from
d1
join d2
on d1.ticker=d2.ticker
and d1.IDX=d2.IDX

{% for column_name in results_list %}

left outer join "d_{{ column_name }}"
on d1.ticker="d_{{ column_name }}".ticker
and d1.IDX="d_{{ column_name }}".IDX

{% endfor %}

{% endmacro %}