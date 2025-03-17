""" import storage class from google.cloud library"""

"""
1. Create Bucket
2. Get Bucket Metadata
3. Add Lifecycle rule
4. Copy file from one bucket to another 
5. List object in a bucket
6. Check if object exists in a bucket.
"""


from google.cloud import storage

""" Create first bucket """
bucket_name = "dcp_bucket_random_420"


def create_gcs_bucket(bucket_name):
    storage_client = storage.Client()
    new_bucket = storage_client.bucket(bucket_name)
    new_bucket.storage_class = "STANDARD"
    my_new_bucket = storage_client.create_bucket(new_bucket, location="us")

    print(f"Bucket Created {my_new_bucket.name} @ {my_new_bucket.location} location")

    return my_new_bucket

#create_gcs_bucket(bucket_name)

def get_bucket_metadata(bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    print(f"ID: {bucket.id}")
    print(f"Name: {bucket.name}")
    print(f"Storage Class: {bucket.storage_class}")
    print(f"Location: {bucket.location}")
    print(f"Location Type: {bucket.location_type}")

    print(f"Default KMS Key Name: {bucket.default_kms_key_name}")

    print(f"Object Retention Mode: {bucket.object_retention_mode}")

    print(f"Time Created: {bucket.time_created}")
    print(f"Versioning Enabled: {bucket.versioning_enabled}")
    print(f"Labels: {bucket.labels}")


#get_bucket_metadata(bucket_name)

def add_lifecycle_rules(bucket_name):
    bucket_client = storage.Client()
    bucket = bucket_client.get_bucket(bucket_name)
    bucket.add_lifecycle_delete_rule(age=2)

    rules = bucket.lifecycle_rules

    print(f"Lifecycle management is enable for bucket {bucket_name} and the rules are {list(rules)}")

#add_lifecycle_rules(bucket_name)

def copy_file_to_another_bucket(source_bukcet_name, source_blob_name, destination_bucket_name, destination_blob_name):
    storage_client = storage.Client()

    source_bucket = storage_client.bucket(source_bukcet_name)
    source_blob = source_bucket.blob(source_blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
    print("Blob {} in bucket {} copied to blob {} in bucket {}.".format(source_blob.name,source_bucket.name,blob_copy.name,destination_bucket.name))

# Note: if you want to move, then after copy, delete object from current bucket.
# Note: To move data more than 1 TB , use Data Transfer Service
# copy_file_to_another_bucket('dcp_bucket_random_420','biq_query_learn.rtf','dcp_bucket_random_240', 'biq_query_learn.rtf')


def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:
        print(blob.name)

#list_blobs('test-project-dcp')


def file_to_check(bucket_name, object_name):
    """ to check folder, use folder/
        to check file inside folder give path foldername/filename.ext
        """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    if blob.exists():
        print(f"Object '{object_name}' exists in bucket '{bucket_name}'.")
    else:
        print(f"Object '{object_name}' does not exist in bucket '{bucket_name}'.")


file_to_check('test-project-dcp','sample_data/customer_purchasing_behaviors.csv')