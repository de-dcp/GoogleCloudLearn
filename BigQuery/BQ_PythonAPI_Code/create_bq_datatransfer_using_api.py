## This is not tested yet

"""
To transfer data from one project to another especially when data is more than 1 TB
"""

"""
Pre-requsite
1. Create a empty dataset
2. Create a service account  IAM --> Serivce Account -->name --> Role Big Query Admin[BQTransUPD / Create]
3. Go To Keys --> Add key --> Generate JSON File
5. Go to Source Project and provide BQ Viewer to this service account.
6. Enable cloud shell. Upload Service account credentials
"""

#pip install google-cloud-bigquery-datatransfer
#export GOOGLE_APPLICATION_CREDENTIALS="path of key json file"

from google.cloud import bigquery_datatransfer

destination_project_id = ''
destination_dataset_id = ''
source_project_id = ''
source_dataset_id = ''

transfer_config = bigquery_datatransfer.TransferConfig
(
    destination_dataset_id = destination_dataset_id,
    display_name = 'copy_dataset_across_project',
    data_source_id="cross_region_copy"
    params = {
        "source_project_id": source_project_id,
        "source_dataset_id": source_dataset_id,
        },
    schedule = "every 24 hours"
    )

transfer.config= transfer.client.create_transfer_config(
    parent=transfer_client.common_project_path(destination_project_id),
    transfer_config=transfer_config,
    )

print(fcreated transfer config": {transfer_config}")

