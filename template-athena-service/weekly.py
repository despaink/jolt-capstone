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
    responses.append(averageVisitDurationInMinutes(storeName, day))
    responses.append(totalRepeat(storeName, day))
    
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
    # I think this is actually just counting the records per day
    query = (
        "SELECT date(date_trunc('day', first_seen)) time, Count(*) visits "
		f"FROM {storeName} "
        f"WHERE extract(week FROM first_seen)=extract(week FROM DATE('{day}'))"
		"GROUP BY date_trunc('day', first_seen) "
		"ORDER BY date_trunc('day', first_seen)"
    )

    outputLocation = constructOutputLocation(storeName, 'unique_per_day_by_week', day)
    return executeQuery(query, outputLocation)


# customers
def totalUnique(store, day): 
    query = (
        "SELECT COUNT(DISTINCT mac) visits "
        f"FROM {store} "
        f"WHERE extract(week FROM first_seen)=extract(week FROM DATE('{day}'))"
    )
    outputLocation = constructOutputLocation(store, 'weekly_total_unique', day)
    return executeQuery(query, outputLocation)


# This is currently the same as uniquePerDay, so
# TODO: get this to select repeat customers
def repeatPerDay(store, day):
    query = (
        "SELECT date(date_trunc('day', first_seen)) time, Count(*) visits "
		f"FROM {store} "
        f"WHERE extract(week FROM first_seen)=extract(week FROM DATE('{day}'))"
		"GROUP BY date_trunc('day', first_seen) "
		"ORDER BY date_trunc('day', first_seen)"
    )
    outputLocation = constructOutputLocation(store, 'weekly_repeat_by_day', day)
    return executeQuery(query, outputLocation)


# This is repeat customers, not visits. May be worthwhile making the distinction later
def totalRepeat(store, day):
    query = (
        "SELECT COUNT(*) repeat_customers "
        "FROM ( "
            "SELECT mac, COUNT(*) visits "
            f"FROM {store} "
            f"WHERE extract(week FROM first_seen)=extract(week FROM DATE('{day}')) "
            "GROUP BY mac "
            "HAVING COUNT(*) > 1 "
            "ORDER BY COUNT(*) DESC "
        ")"
    )
    outputLocation = constructOutputLocation(store, 'weekly_total_repeat_customers', day)
    return executeQuery(query, outputLocation)


def averageVisitDurationInMinutes(store, day):
    query = (
        "SELECT avg(date_diff('minute', first_seen, last_seen)) duration "
        f"FROM {store} "
        f"WHERE extract(week FROM first_seen)=extract(week FROM date('{day}'))"
    )
    outputLocation = constructOutputLocation(store, 'weekly_avg_duration', day)
    return executeQuery(query, outputLocation)