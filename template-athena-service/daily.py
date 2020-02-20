import json
import boto3
import logging
from datetime import date, timedelta

athena_client = boto3.client('athena')

# triggered at 5am every morning
def handle(event, context):
    # extract storeName from event?
    storeName = 'store_name_1'  
    day = ( date.today() - timedelta(1) ).strftime('%Y-%m-%d')

    response = uniquePerHour(storeName, day)
    response += '\n\n' + totalUnique(storeName, day)
    response += '\n\n' + repeatByMac(storeName, day)
    print(response)
    return response


def executeQuery(query, outputLocation):
    return athena_client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = { 'Database': 'capstone' },
        ResultConfiguration = { 'OutputLocation': outputLocation }
    )


def constructOutputLocation(storeName, queryName, day):
    return f's3://jolt.capstone/athena-query-logs/{storeName}/{queryName}/{day}'


def uniquePerHour(storeName, day):
    query = (
        "SELECT date_trunc('hour', first_seen) time, Count(*) visits "
		f"FROM {storeName} "
        "WHERE DATE(first_seen)=DATE('${day}') "
		"GROUP BY date_trunc('hour', first_seen) "
		"ORDER BY date_trunc('hour', first_seen)"
    )
    outputLocation = constructOutputLocation(storeName, 'unique_per_hour', day)
    return executeQuery(query, outputLocation)


def totalUnique(storeName, day):
    query = (
        "SELECT COUNT(DISTINCT mac) unique_devices "
        f"FROM {storeName} "
        f"WHERE DATE(first_seen)=DATE('{day}')"
    )
    outputLocation = constructOutputLocation(storeName, 'total_unique', day)
    return executeQuery(query, outputLocation)


def repeatByMac(storeName, day):
    query = (
        "SELECT mac, COUNT(*) visits "
        f"FROM {storeName} "
        f"WHERE DATE(first_seen)=DATE('{day}') "
        "GROUP BY mac "
        "HAVING COUNT(*) > 1 "
        "ORDER BY COUNT(*) DESC"
    )
    outputLocation = constructOutputLocation(storeName, 'repeat_by_mac', day)
    return executeQuery(query, outputLocation)
