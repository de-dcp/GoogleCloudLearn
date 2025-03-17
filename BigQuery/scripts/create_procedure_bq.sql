--- Create Table

drop table sales_data_view.table_stats;

create table sales_data_view.table_stats
(
 dataset_name String,
 table_name String,
 stats_collect_date Date,
 record_count INTEGER,
 SIZE_IN_GB FLOAT64

);



--- Create Procedure


CREATE OR REPLACE PROCEDURE sales_data_view.sp_collect_stats(dataset_list STRING,OUT status STRING)

BEGIN

DECLARE qry STRING;

for rec in (
	select schema_name as dataset_name from INFORMATION_SCHEMA.SCHEMATA
	where schema_name in (select * from unnest(split(dataset_list))))

DO

delete from sales_data_view.table_stats where dataset_name = rec.dataset_name and stats_collect_date = current_date;

set qry = CONCAT("INSERT into sales_data_view.table_stats select dataset_id as dataset_name, table_id as table_name, current_date as stats_collect_data, row_count as record_Count, size_bytes/pow(10,9) as size_in_gb from lexical-cider-440507-u0.", rec.dataset_name,".__TABLES__ where type = 1 ");


execute immediate qry;

set status = 'success';

END FOR;

END;


---- Call Procdure

begin
declare out_status string;

CALL sales_data_view.sp_collect_stats('sales_data_view',out_status);

select out_status;

end;