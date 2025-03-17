"""
1. Create Dataset
2. Create Table
3. Load Data to BQ
"""

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def create_dataset(project, dataset):
    """
    1. Create BQ client
    2. set dataset id as project.datasetname and create a dataset reference
    3. set location and other parameter i.e. expiration time
    4. call create_dataset function passing dataset reference
    """

    client = bigquery.Client()
    dataset_id = f"{project}.{dataset}"

    try:
        client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists".format(dataset_id))

    except NotFound:
        dataset_ref = bigquery.Dataset(dataset_id)
        dataset_ref.location = 'us-east1'
        dataset = client.create_dataset(dataset_ref, timeout=30)  # API request
        print(f"Created dataset {client.project}.{dataset.dataset_id}")


def create_table(project, dataset, table):
    client = bigquery.Client()

    try:

        table_ref = client.dataset(dataset, project=project).table(table)
        client.get_table(table_ref)
        print("Table already exists!")

    except NotFound:

        table_id = f"{project}.{dataset}.{table}"
        table_schema = [
            bigquery.SchemaField("user_id", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("annual_income", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("purchase_amount", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("loyalty_score", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("purchase_frequency", "INTEGER", mode="NULLABLE")
        ]

        table = bigquery.Table(table_id, schema=table_schema)
        table = client.create_table(table)
        print(f"table created {table}")


def load_data_to_bq(project_name, dataset_name, table_name, bucket_file_uri):
    """
    Note: this load_table_from_uri will create table if not exists
    1. Create Bigquery client
    2. set table name
    3. set bq configuration i.e. source type, skip rows, schema, create if exist, append load
    4. create bq load job - providing source URI, tablename and config and trigger the job
    """

    bq_client = bigquery.Client()
    table_id = f"{project_name}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.write_disposition = 'WRITE_TRUNCATE'  # 'WRITE_APPEND'

    load_job = bq_client.load_table_from_uri(bucket_file_uri, table_id, job_config=job_config)

    load_job.result()

    destination_table = bq_client.get_table(table_id)
    return destination_table.num_rows


if __name__ == "__main__":
    try:
        project_name = "lexical-cider-440507-u0"
        dataset_name = "my_dataset_api"
        table_name = "customer_purchase_behavior_creat_api"
        file_location = "gs://test-project-dcp/Customer_Purchasing_Behaviors.csv"

        print("Starting Dataset Creation ")

        create_dataset(project_name, dataset_name)

        print("starting table load")

        create_table(project_name, dataset_name, table_name)

        print("Starting table load ")

        num_rows = load_data_to_bq(project_name, dataset_name, table_name, file_location)

        print(f"table loaded with {num_rows} rows")

    except Exception as e:
        print(f"Some exception occurred {e}")

