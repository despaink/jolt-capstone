import json
import boto3
import logging
from datetime import date, timedelta

athena_client = boto3.client('athena')
FIRST_SEEN = "date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s')"
LAST_SEEN = "date_parse(trim(last_seen), '%Y-%m-%d %H:%i:%s')"

# triggered at 4am every Sunday
def handle(event, context):
    responses = []

    for storeName in event.get('stores'):
        # day = date.today()
        # day = "2020-03-8"
        day = ( date.today() - timedelta(1) ).strftime('%Y-%m-%d')
        weekStart = ( day - timedelta(weeks=1) ).strftime('%Y-%m-%d')
        weekEnd = ( day - timedelta(days=1) ).strftime('%Y-%m-%d')
        res = [storeName]
        
        res.append(uniquePerDay(storeName, day, weekStart, weekEnd))
        res.append(totalUnique(storeName, day, weekStart, weekEnd))
        res.append(totalRepeat(storeName, day, weekStart, weekEnd))
        res.append(averageVisitDurationInMinutes(storeName, day, weekStart, weekEnd))

        responses.append(res)
    
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
def uniquePerDay(storeName, day, weekStart, weekEnd):
    query = (
        f"SELECT date({FIRST_SEEN}) day, Count(*) visits "
        f"FROM {storeName} "
        f"WHERE dt between '{weekStart}' and '{weekEnd}' AND \"$PATH\" NOT LIKE '%metadata' "
        f"GROUP BY date({FIRST_SEEN}) "
        f"ORDER BY date({FIRST_SEEN})"
    )

    outputLocation = constructOutputLocation(storeName, 'unique_per_day_by_week', day)
    return executeQuery(query, outputLocation)


# total number of unique mac addresses that have appeared during the scans
def totalUnique(storeName, day, weekStart, weekEnd): 
    query = (
        "SELECT COUNT(DISTINCT mac) visits "
        f"FROM {storeName} "
        f"WHERE dt between '{weekStart}' and '{weekEnd}' AND \"$PATH\" NOT LIKE '%metadata'"
    )
    outputLocation = constructOutputLocation(storeName, 'weekly_total_unique', day)
    return executeQuery(query, outputLocation)


# This is repeat customers, not visits. May be worthwhile making the distinction later
def totalRepeat(storeName, day, weekStart, weekEnd):
    query = (
        "SELECT COUNT(*) repeat_customers "
        "FROM ( "
            "SELECT mac, COUNT(*) visits "
            f"FROM {storeName} "
            f"WHERE dt between '{weekStart}' and '{weekEnd}' AND \"$PATH\" NOT LIKE '%metadata' "
            "GROUP BY mac "
            "HAVING COUNT(*) > 1 "
            "ORDER BY COUNT(*) DESC "
        ")"
    )
    outputLocation = constructOutputLocation(storeName, 'weekly_total_repeat_customers', day)
    return executeQuery(query, outputLocation)


def averageVisitDurationInMinutes(storeName, day, weekStart, weekEnd):
    query = (
        f"SELECT avg(date_diff('minute', {FIRST_SEEN}, {LAST_SEEN})) duration "
        f"FROM {storeName} "
        f"WHERE dt between '{weekStart}' and '{weekEnd}' AND \"$PATH\" NOT LIKE '%metadata'"
    )
    outputLocation = constructOutputLocation(storeName, 'weekly_avg_duration', day)
    return executeQuery(query, outputLocation)