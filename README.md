GuruFocus.com provides payed API to get stocks fundamental data but there are only 2,000 calls per month. I monitor more then 5,000 stocks and need to know what new fundamental data were recently uploaded to GuruFocus to use my limited API calls only to get new data. 

The site provides free access to the recent 4 quarters of the data. I web-scrap the free web-pages to get most recent available date for each my stock. Then I compare the most recent available date on the site to the data I already downloaded. I use API calls only to get fundamental data if it's new.

![image](https://github.com/KaterynaD/Load-Fundamental-Data/assets/16999229/bf1d140a-c33a-464f-93de-6050246fcda5)

The data (most recent available dates in csv file and fundamental data in JSON files) are uploaded to S3 bucket.

There are Snowflake external tables configured to get staging data exactly as they are in S3 files.

DBT macro is used to parse JSON records in the staging table and each individual part is stored in its own table in Snowflake.

![image](https://github.com/KaterynaD/Load-Fundamental-Data/assets/16999229/a9538ab1-3ef1-4a1f-8f93-1d7df4b30e3e)


The steps are orchesttrated in Airflow DAG.

![image](https://github.com/KaterynaD/Load-Fundamental-Data/assets/16999229/d7f8b4fa-eb86-46cc-b1db-7b420760b058)

The project is rather POC then production and requre improvments: incremental load in the tables, clean-up S3 bucket.
