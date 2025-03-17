# Hadoop Demo Command:
"""

hdfs dfs -mkdir /testdata
hdfs dfs -put customer_purchase_data.csv /testdata

"""
## Hive Demo Command:

"""
hive
create database dataproc_demo;
use dataproc_demo;

CREATE TABLE IF NOT EXISTS customer_purchase_history(
user_id INT,
age INT,
annual_income Float,
purchase_amount float,
loyalty_score float,
region  STRING,
frequency int
)
row format delimited fields terminated BY ',' lines terminated BY '\n'
tblproperties("skip.header.line.count"="1");

LOAD DATA INPATH '/testdata/customer_purchase_data.csv' INTO TABLE customer_purchase_history;

select * from customer_purchase_history limit 20;

hdfs dfs -ls /user/hive/warehouse

"""

## Spark Demo Command:

"""

>>> first_df = spark.read.option('header',True).csv('/testdata/customer_purchase_data.csv');
>>> first_df.show()
>>> first_df.createTempView('cust_purchase_history')
>>> final_df = spark.sql('select region,age,sum(purchase_amount) from cust_purchase_history group by region, age')
>>> final_df.coalesce(1).write.csv('/testdata/sparkoutput')


"""

## Create data proc cluster from CLI

"""
gcloud dataproc clusters create dataproc-cluster-cli-demo \
--bucket demo-dp-cli \
--region us-central1 \
--zone us-central1-c \
--master-machine-type n1-standard-2 \
--master-boot-disk-size 50 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 50 \
--image-version 2.2.32-debian12 \
--scopes 'https://www.googleapis.com/auth/cloud-platform'
--tags dp-cli-demo \
--project melodic-bearing-430314-d2 \
--initialization-actions gs://goog-dataproc-initialization-actions-us-central1/connectors/connectors.sh --metadata bigquery-connector-version=1.2.0 --metadata spark-bigquery-connector-version=0.21.0

--optional-components JUPYTER
"""