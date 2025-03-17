# Import required modules and packages
from pyspark.sql import SparkSession


#Creating Spark Session
mySpark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('gcs-bigquery-spark-demo') \
  .getOrCreate()

bucket = 'demo-dp-cli'
mySpark.conf.set('temporaryGcsBucket', bucket)

# Read data into Spark dataframe from GCS bucket and process the data
first_df = mySpark.read.option('header', True).csv('gs://demo-dp-cli/customer_purchasing_behaviors.csv')
first_df.createTempView('cust_purchase_history')
final_df = mySpark.sql('select region,age,sum(purchase_amount) as total_purchase from cust_purchase_history group by region, age')

# Writing output to BigQuery
final_df.write.format('bigquery').option('table', 'dataproc_test.cust_purchase_udw').option('createDisposition','CREATE_IF_NEEDED').save()
mySpark.stop()