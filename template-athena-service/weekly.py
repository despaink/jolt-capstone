import json
import boto3
import logging
from datetime import date

athena_client = boto3.client('athena')

# triggered at 5am every Sunday
def handle(event, context):
    # TODO: extract storeName from event
    storeName = 'store_name_1'
    day = date.today()

    baseOutputLocation = f's3://jolt.capstone/athena-query-logs/{storeName}'

    response = uniquePerDay(storeName, day, baseOutputLocation)
    print(response)   
    return response 


def uniquePerDay(storeName, day, baseOutputLocation): # not completed
    weekStart = ''
    weekEnd = ''
    athenaQuery = (
        "SELECT date_trunc('day', first_seen) time, Count(*) visits "
		f"FROM {storeName} "
        f"WHERE DATE(first_seen) BETWEEN DATE({weekStart}) AND DATE({weekEnd})"
		"GROUP BY date_trunc('day', first_seen) "
		"ORDER BY date_trunc('day', first_seen)"
    )

    outputLocation = f'{baseOutputLocation}/unique_per_hour/{day}'

    response = athena_client.start_query_execution(
        QueryString = athenaQuery,
        QueryExecutionContext = { 'Database': 'capstone' },
        ResultConfiguration = { 'OutputLocation': outputLocation }
    )
    return response