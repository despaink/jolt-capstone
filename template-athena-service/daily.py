import json
import boto3
import logging
from datetime import date

athena_client = boto3.client('athena')

# Requires:
#   date
#   storeName

# triggered at 5am every morning
def handle(event, context):
    # extract these from event
    storeName = 'store_name_1'  
    today = date.today().strftime('%y-%m-%d')

    baseOutputLocation = f's3://jolt.capstone/athena-query-logs/{storeName}/'

    response = uniquePerHour(today, baseOutputLocation)
    print(response)
    return response


def uniquePerHour(today, baseInputLocation):
    athenaQuery = (
        "SELECT date_trunc('hour', first_seen) time, Count(*) visits "
		"FROM store_name_1 "
        f"WHERE DATE(first_seen)=DATE('{today}') "
		"GROUP BY date_trunc('hour', first_seen) "
		"ORDER BY date_trunc('hour', first_seen)"
        )
    
    outputLocation = baseInputLocation + 'unique_per_hour/'

    response = athena_client.start_query_execution(
        QueryString = athenaQuery,
        QueryExecutionContext = { 'Database': 'capstone' },
        ResultConfiguration = { 'OutputLocation': outputLocation }
    )
    return response 