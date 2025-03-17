from pyspark.sql import SparkSession
from time import sleep

mySpark = SparkSession.builder.appName("LearnDataProc").getOrCreate()
first_df = mySpark.read.option('header', True).csv('/testdata/customer_purchase_data.csv')
first_df.createTempView('cust_purchase_history')
final_df = mySpark.sql('select region,age,sum(purchase_amount) from cust_purchase_history group by region, age')
final_df.coalesce(1).write.mode("overwrite").option("delimiter", ":").csv('/testdata/sparkoutput')

sleep(120)
print("waking up and closing spark session")
sleep(10)
mySpark.stop()