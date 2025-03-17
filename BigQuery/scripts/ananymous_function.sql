-- Anonymous function are used to run multiple SQL statement together in a iterative manner.


DECLARE dataset_to_check ARRAY<STRING>;
DECLARE i INT64 DEFAULT 0;
DECLARE dataset STRING ;
DECLARE qry STRING;


-- list out dataset in project_name

SET dataset_to_check = (
with req_dataset as 
(select schema_name from 'project_name.INFORMATION_SCHEMA.SCHEMATA)
select array_agg(schema_name) from req_dataset
);


LOOP SET i = i + 1;
	BEGIN
	IF i > ARRAY_LENGTH(dataset_to_check) THEN
		LEAVE;
	END IF;
	
	DELETE FROM analysis.table_stats where dataset_name = dataset_to_check[ORDINAL(i)] and stats_collect_state = current_data;
	
	set qry = CONCAT("INSERT analysis.table_stats select dataset_id as dataset_name, table_id as table_name, current_date as stats_collect_data, row_count as record_Count, TIMESTAMP_MILLIS(last_modified_time,size_bytes/pow(10/9) as size_in_gb from projectid.", dataset_to_check[ORDINAL[i]),".__TBALES__ where type = 1 ");
	
	execute immediate qry;
	
	EXCEPTION
		WHEN ERROR THEN CONTINUE;
	
	END;

END LOOP;