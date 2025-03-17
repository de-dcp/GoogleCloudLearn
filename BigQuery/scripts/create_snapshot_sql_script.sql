---- Run this command on CLI
bq query --use_legacy_sql=false \
--display_name="Daily Snapshot of table" \
--location="us-east" \
--schedule="every 24 hours" \
--project_id="lexical-cider-440507-u0" \
create_snapshot_sql_script.sql



----------------------- save below code in create_snapshot_sql_script.sql file. Change dataset and table name accordingly
-- Declare Variables
DEClARE snapshot_name STRING;
DEClARE expiration TIMESTAMP;
DEClARE query STRING;



--Set variables
SET expiration = DATE_ADD(current_timestamp(), INTERVAL 10 DAY);
SET snapshot_name = CONCAT(<DataSet>.<Table>,FORMAT_DATETIME('%Y%m%d', current_date()));

-- construct query
SET query =  CONCAT( 'CREATE SNAPSHOT TABLE ', snapshot_name, ' CLONE dataset.table OPTIONS( expiration_timestamp = TIMESTAMP '", expiration ,"');");

--run query
EXECUTE IMMEDIATE query;

