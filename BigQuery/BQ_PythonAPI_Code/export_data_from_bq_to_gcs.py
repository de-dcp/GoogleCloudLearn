"""
install pandas and db-dtypes library to export data from query result to gcs
OR
write data to temp table and export temp table to gcs

"""

from google.cloud import bigquery
from google.cloud import storage


def export_table_to_gcs(project_id,  bucket_name, destination_blob_name, dataset_id, table_id):
    client = bigquery.Client()

    destination_uri = f"gs://{bucket_name}/{destination_blob_name}"
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="us-east1",  # Location must match that of the source table.
    )  # API request

    extract_job.result()  # Waits for job to complete.

    print(
        f"Exported {project_id}:{dataset_id}.{table_id} to {destination_uri}"
    )


def export_query_result_to_gcs(query, bucket_name, blob_obj_name):
    # Execute the query
    client = bigquery.Client()
    query_job = client.query(query)

    # Wait for the query to finish
    result_df = query_job.to_dataframe()

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob('customer_age_upto_25_analysis.csv')
    blob.upload_from_string(result_df.to_csv(sep=';', index=False, encoding='utf-8'), content_type='application/octet-stream')
    return "success"


if __name__ == "__main__":
    query = "SELECT age,avg(annual_income) as avg_income,avg(loyalty_score) as avg_loyalty_score FROM lexical-cider-440507-u0.my_dataset_api.customer_purchase_behavior_creat_api where age <= 25 group by age order by age"

    project_id = "lexical-cider-440507-u0"
    bucket_name = "test-project-dcp"
    destination_blob_name = "cutomer_age_based_analysis.csv"
    dataset_id = "my_dataset_api"
    table_id = "customer_purchase_behavior_creat_api"

    print("Staring Export Job")

    #export_table_to_gcs(project_id, bucket_name, destination_blob_name, dataset_id, table_id)

    export_query_result_to_gcs(query, bucket_name, destination_blob_name)

    print("Export Job Ended")



