-- 1. Create View
-- 2. Create External Table
-- 3. Accessing Historical Data using Time Travel
-- 4. Create Snapshot AND view list of snapshot
-- 5.) BigQuery -- Table Function
-- 6.) Partitions Table
-- 7.) Cluster Table

-- 1. Create View
create view sales_dataset.intl_sales_view_aus as
SELECT country, customer_id, invoice_no, sum(unit_purchased * unit_price) as total_order_amount
FROM `lexical-cider-440507-u0.sales_dataset.sales_data_intl_stg` where country = "Australia"
group by country , customer_id, invoice_no;

"""
To create an authorized view:
1. create a new dataset with suffix view
2. create new view under this dataset.
3. give permission to user to read this dataset.
4. authorize views on actual source dataset.

"""


-- 2. Create External Table
CREATE OR REPLACE EXTERNAL TABLE test_dcp.extenal_table_console_csv
OPTIONS (
format = 'CSV',
uris = ['gs://test-project-dcp/Customer_Purchasing_Behaviors.csv']
);



--3. Accessing Historical Data using Time Travel

select COUNT(*) from sales_dataset.sales_data_intl_stg FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 DAY);

select COUNT(*) from sales_dataset.sales_data_intl_stg FOR SYSTEM_TIME as of TIMESTAMP_MILLIS(1732862814359);


-- 4. Create Snapshot AND view list of snapshot

CREATE SNAPSHOT TABLE `lexical-cider-440507-u0.sales_data_view.sales_data_intl_stg_snapshot`
CLONE lexical-cider-440507-u0.sales_dataset.sales_data_intl_stg
OPTIONS(
  expiration_timestamp = TIMESTAMP "2024-12-31 23:59:59 UTC"
);


select * from lexical-cider-440507-u0.sales_data_view.INFORMATION_SCHEMA.TABLE_SNAPSHOTS WHERE BASE_tABLE_NAME = 'sales_data_intl_stg'


-- 5.) BigQuery -- Table Function

create or replace table function sales_data_view.get_country_sales_data(cntry STRING) AS
(
select * from `lexical-cider-440507-u0.sales_data_view.sales_view_in_table` where country = cntry
);


select * from sales_data_view.get_country_sales_data('Australia');

-- 6.) Partitions Table

--a.) Based on Ingestion timestamp

create or Replace table lexical-cider-440507-u0.sales_data_view.sales_data_intl_stg_snapshot_partition
(
  country String,
  customer_id Integer,
  invoice_date String,
  invoice_no STRING,
  product_code String,
  product_desc String,
  unit_price Float64,
  unit_purchased Integer
)
partition by DATETIME_TRUNC(_PARTITIONTIME, date);
OPTIONS (
partition_expiration_days = 3,
require_partition_filter = TRUE
);

insert into partition_table(colum list) select column list from existing_Table;


--b.) based on range
create or Replace table lexical-cider-440507-u0.my_dataset_api.customer_purchase_behavior_partition
(
  age Integer,
  annual_income Integer,
  loyalty_score Float64,
  purchase_amount Integer,
  purchase_frequency Integer,
  region String,
  user_id Integer
  )
PARTITION BY  RANGE_BUCKET(age, GENERATE_ARRAY(0, 100, 10))
;


-- 7.) Cluster Table

create or Replace table lexical-cider-440507-u0.sales_data_view.sales_data_intl_stg_snapshot_cluster
(
  country String,
  customer_id Integer,
  invoice_date String,
  invoice_no STRING,
  product_code String,
  product_desc String,
  unit_price Float64,
  unit_purchased Integer
)
cluster by Country
OPTIONS (
description = 'Table is clussterd on country column'
);