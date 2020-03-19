import json
import boto3
import logging
from datetime import date, timedelta

athena_client = boto3.client('athena')
FIRST_SEEN = "date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s')"
LAST_SEEN = "date_parse(trim(last_seen), '%Y-%m-%d %H:%i:%s')"

# triggered at 5am every morning
def handle(event, context):
    # extract storeName from event?
    storeName = 'sams_house'
    # day = ( date.today() - timedelta(1) ).strftime('%Y-%m-%d')
    day = "2020-03-15"
    
    responses = []
    responses.append(joinDailyRecords(storeName, day))
    responses.append(addPartition(storeName, day))
    responses.append(uniquePerHour(storeName, day))
    responses.append(totalUnique(storeName, day))
    responses.append(averageVisitDurationInMinutes(storeName, day))
    
    #print(responses)
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
        "SELECT mac, min(first_seen) first_seen, max(last_seen) last_seen, min(power) power "
        f"FROM {storeName}_intermediate "
        f"WHERE date({FIRST_SEEN})=date('{day}') "
        f"and date({LAST_SEEN})=date('{day}') "
        "GROUP BY mac"
    )
    outputLocation = f's3://jolt.capstone/athena-query-logs/{storeName}/intermediate/dt={day}/'
    return executeQuery(query, outputLocation)


def addPartition(storeName, day):
    query = f"ALTER TABLE capstone.{storeName} ADD IF NOT EXISTS PARTITION (dt='{day}')"
    outputLocation = f's3://jolt.capstone/athena-query-logs/{storeName}/partition-logs/dt={day}/'
    return executeQuery(query, outputLocation)


def uniquePerHour(storeName, day):
    query = (
        f"SELECT date_trunc('hour', {FIRST_SEEN}) time, Count(*) visits "
        f"FROM {storeName} "
        f"WHERE dt='{day}' AND \"$PATH\" NOT LIKE '%metadata' "
        f"GROUP BY date_trunc('hour', {FIRST_SEEN}) "
        f"ORDER BY date_trunc('hour', {FIRST_SEEN})"
    )
    outputLocation = constructOutputLocation(storeName, 'unique_per_hour', day)
    return executeQuery(query, outputLocation)


# customers
def totalUnique(storeName, day):
    query = (
        "SELECT COUNT(DISTINCT mac) visits "
        f"FROM {storeName} "
        f"WHERE dt='{day}' AND \"$PATH\" NOT LIKE '%metadata'"
    )
    outputLocation = constructOutputLocation(storeName, 'daily_total_unique', day)
    return executeQuery(query, outputLocation)


def averageVisitDurationInMinutes(storeName, day): 
    query = (
        f"SELECT avg(date_diff('minute', {FIRST_SEEN}, {LAST_SEEN})) duration "
        f"FROM {storeName} "
        f"WHERE dt='{day}' AND \"$PATH\" NOT LIKE '%metadata'"
    )
    outputLocation = constructOutputLocation(storeName, 'daily_avg_duration', day)
    return executeQuery(query, outputLocation)