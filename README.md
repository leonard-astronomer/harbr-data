# harbr-data

To get sample data:

curl "https://api.mockaroo.com/api/511cd950?count=1000&key=a8034b60" > "Accounts.csv"

curl "https://api.mockaroo.com/api/511cd950?count=1000&key=a8034b60" > "ip_records.csv"

Harbr Ingestion Endpoint 

s3://walstad-astronomer/Wh8CGNSjQeWBbC0Q1j8XEA/


Libraries

pip install apache-airflow-providers-amazon

from airflow.providers.amazon.aws.hooks.s3 import S3Hook



