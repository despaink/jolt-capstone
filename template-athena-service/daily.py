import json
import boto3
import logging
from datetime import date, timedelta

athena_client = boto3.client('athena')

# triggered at 5am every morning
def handle(event, context):
    # extract storeName from event?
    storeName = 'store_name_1'  
    day = ( date.today() - timedelta(days=1) ).strftime('%Y-%m-%d')

    baseOutputLocation = f's3://jolt.capstone/athena-query-logs/{storeName}'

    response = uniquePerHour(storeName, day, baseOutputLocation)
    print(response)
    return response


def uniquePerHour(storeName, day, baseOutputLocation):
    athenaQuery = (
        "SELECT date_trunc('hour', first_seen) time, Count(*) visits "
		f"FROM {storeName} "
        "WHERE DATE(first_seen)=DATE('${day}') "
		"GROUP BY date_trunc('hour', first_seen) "
		"ORDER BY date_trunc('hour', first_seen)"
    )
    
    outputLocation = f'{baseOutputLocation}/unique_per_hour/{day}'

    response = athena_client.start_query_execution(
        QueryString = athenaQuery,
        QueryExecutionContext = { 'Database': 'capstone' },
        ResultConfiguration = { 'OutputLocation': outputLocation }
    )
    return response 