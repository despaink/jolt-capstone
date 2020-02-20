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

    responses = []
    
    responses.append(uniquePerDay(storeName, day))
    responses.append(totalUnique(storeName, day))
    
    print(responses)
    return responses


# # # # # # # # # # # # # # # # # # 
# Helper functions
# # # # # # # # # # # # # # # # # # 
def executeQuery(query, outputLocation):
    return athena_client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = { 'Database': 'capstone' },
        ResultConfiguration = { 'OutputLocation': outputLocation }
    )


def constructOutputLocation(storeName, queryName, day):
    return f's3://jolt.capstone/athena-query-logs/{storeName}/{queryName}/{day}'


# # # # # # # # # # # # # # # # # # 
# Query functions
# # # # # # # # # # # # # # # # # # 
def uniquePerDay(storeName, day):
    query = (
        "SELECT date(date_trunc('day', first_seen)) time, Count(*) visits "
		f"FROM {storeName} "
        f"WHERE extract(week FROM first_seen)=extract(week FROM DATE('{day}'))"
		"GROUP BY date_trunc('day', first_seen) "
		"ORDER BY date_trunc('day', first_seen)"
    )

    outputLocation = constructOutputLocation(storeName, 'unique_per_day_by_week', day)
    return executeQuery(query, outputLocation)


def totalUnique(store, day): 
    query = (
        "SELECT COUNT(DISTINCT mac) "
        f"FROM {store} "
        f"WHERE extract(week FROM first_seen)=extract(week FROM DATE('{day}'))"
    )
    outputLocation = constructOutputLocation(store, 'weekly_total_unique', day)
    return executeQuery(query, outputLocation)