from google.cloud import bigquery


# Initialize a BigQuery client
def execute_query(query):
    client = bigquery.Client()

    # Execute the query
    query_job = client.query(query)

    # Wait for the query to finish
    results = query_job.result()  # create an empty row interator if no rows in output

    print(f"result is {results}")

    # Print the results
    """for row in results:
        print(f"age: {row.age}, average_income: {row.avg_income}, average_loyalty_score: {row.avg_loyalty_score}")

    """


if __name__ == "__main__":
    query = "insert into my_dataset_api.customer_age_group_info SELECT age,avg(annual_income) as avg_income,avg(loyalty_score) as avg_loyalty_score  FROM `lexical-cider-440507-u0.my_dataset_api.customer_purchase_behavior_creat_api` group by age order by age"
    execute_query(query)

