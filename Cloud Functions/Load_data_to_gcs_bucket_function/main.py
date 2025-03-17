import functions_framework
import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")

    lst = []
    table_name = name.split('.')[0]

    # Event,File metadata details writing into Big Query
    dct={
         'Event_ID':event_id,
         'Event_type':event_type,
         'Bucket_name':bucket,
         'File_name':name,
         'Created':timeCreated,
         'Updated':updated
        }
    lst.append(dct)

    print(f" list of all the event details {lst}")
    df_metadata = pd.DataFrame.from_records(lst)
    df_metadata.to_gbq('cloud_func_dataset.data_loading_metadata', 
                        project_id='lexical-cider-440507-u0', 
                        if_exists='append',
                        location='us-east1')
    
    df_data = pd.read_csv('gs://' + bucket + '/' + name)

    df_data.to_gbq('cloud_func_dataset.'+table_name, 
                        project_id='lexical-cider-440507-u0', 
                        if_exists='append',
                        location='us-east1')



