import json
import boto3
import logging
from datetime import date

athena_client = boto3.client('athena')

# triggered at 4am every Sunday
def handle(event, context):
    # TODO: extract storeName from event
    storeName = 'heritage_15'
    day = date.today()

    responses = []
    
    responses.append(uniquePerDay(storeName, day))
    responses.append(totalUnique(storeName, day))
    responses.append(totalRepeat(storeName, day))
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
def uniquePerDay(storeName, day):
    query = (
        "SELECT date(date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s')) day, Count(*) visits "
        f"FROM {storeName} "
        f"WHERE week(date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s'))=week(DATE('{day}')) "
        "GROUP BY date(date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s')) "
        "ORDER BY date(date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s'))"
    )

    outputLocation = constructOutputLocation(storeName, 'unique_per_day_by_week', day)
    return executeQuery(query, outputLocation)


# total number of unique mac addresses that have appeared during the scans
def totalUnique(storeName, day): 
    query = (
        "SELECT COUNT(DISTINCT mac) visits "
        f"FROM {storeName} "
        f"WHERE week(date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s'))=week(DATE('{day}'))"
    )
    outputLocation = constructOutputLocation(storeName, 'weekly_total_unique', day)
    return executeQuery(query, outputLocation)


# This is repeat customers, not visits. May be worthwhile making the distinction later
def totalRepeat(storeName, day):
    query = (
        "SELECT COUNT(*) repeat_customers "
        "FROM ( "
            "SELECT mac, COUNT(*) visits "
            f"FROM {storeName} "
            f"WHERE week(date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s'))=week(DATE('{day}')) "
            "GROUP BY mac "
            "HAVING COUNT(*) > 1 "
            "ORDER BY COUNT(*) DESC "
        ")"
    )
    outputLocation = constructOutputLocation(storeName, 'weekly_total_repeat_customers', day)
    return executeQuery(query, outputLocation)


def averageVisitDurationInMinutes(storeName, day):
    query = (
        "SELECT avg(date_diff('minute', date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s'), date_parse(trim(last_seen), '%Y-%m-%d %H:%i:%s'))) duration "
        f"FROM {storeName} "
        f"WHERE week(date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s'))=week(date('{day}'))"
    )
    outputLocation = constructOutputLocation(storeName, 'weekly_avg_duration', day)
    return executeQuery(query, outputLocation)