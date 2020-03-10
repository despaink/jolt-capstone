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
    
    responses = []
    responses.append(joinDailyRecords(storeName, day))
    responses.append(addPartition(storeName, day))
    responses.append(uniquePerHour(storeName, day))
    responses.append(totalUnique(storeName, day))
    responses.append(repeatByMac(storeName, day))
    responses.append(averageVisitDurationInMinutes(storeName, day))
    
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
def joinDailyRecords(storeName, day): 
    query = (
        "SELECT mac, min(first_seen) first_seen, max(last_seen) last_seen, power "
        f"FROM {storeName} "
        f"WHERE date(first_seen)=date('{day}') and date(last_seen)=date('{day}') "
        "GROUP BY mac, power"
    )
    outputLocation = f's3://jolt.capstone/athena-query-logs/{storeName}/intermediate/dt={day}/daily_joined/'
    return executeQuery(query, outputLocation)


def addPartition(storeName, day):
    query = f"ALTER TABLE castone.{storeName}_intermediate ADD PARTITION (dt={day}) location 's3://jolt.capstone/athena-query-logs/{storeName}_intermediate/"
    outputLocation = f's3://jolt.capstone/athena-query-logs/{storeName}/partition-logs/dt={day}/'
    return executeQuery(query, outputLocation)


def uniquePerHour(storeName, day):
    query = (
        "SELECT date_trunc('hour', first_seen) time, Count(*) visits "
		f"FROM {storeName}_intermediate "
        f"WHERE dt=DATE('{day}') "
		"GROUP BY date_trunc('hour', first_seen) "
		"ORDER BY date_trunc('hour', first_seen)"
    )
    outputLocation = constructOutputLocation(storeName, 'unique_per_hour', day)
    return executeQuery(query, outputLocation)


# customers
def totalUnique(storeName, day):
    query = (
        "SELECT COUNT(DISTINCT mac) visits "
        f"FROM {storeName}_intermediate "
        f"WHERE dt=DATE('{day}')"
    )
    outputLocation = constructOutputLocation(storeName, 'daily_total_unique', day)
    return executeQuery(query, outputLocation)


def repeatByMac(storeName, day):
    query = (
        "SELECT mac, COUNT(*) visits "
        f"FROM {storeName}_intermediate "
        f"WHERE dt=DATE('{day}') "
        "GROUP BY mac "
        "HAVING COUNT(*) > 1 "
        "ORDER BY COUNT(*) DESC"
    )
    outputLocation = constructOutputLocation(storeName, 'daily_repeat_by_mac', day)
    return executeQuery(query, outputLocation)


def averageVisitDurationInMinutes(storeName, day): 
    query = (
        "SELECT avg(date_diff('minute', first_seen, last_seen)) duration "
        f"FROM {storeName}_intermediate "
        f"WHERE dt=date('{day}')"
    )
    outputLocation = constructOutputLocation(storeName, 'daily_avg_duration', day)
    return executeQuery(query, outputLocation)