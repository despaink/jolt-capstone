import json
import boto3
import logging
from datetime import date

athena_client = boto3.client('athena')

# triggered at 5am on the first day of every month
def handle(event, context):
    store = 'store_name_1'
    day = date.today()

    responses = []

    responses.append(uniquePerWeek(store, day))
    responses.append(totalUnique(store, day))
    responses.append(totalRepeat(store, day))
    responses.append(averageVisitDurationInMinutes(store, day))

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


def constructOutputLocation(store, queryName, day):
    return f's3://jolt.capstone/athena-query-logs/{store}/{queryName}/{day}'


# # # # # # # # # # # # # # # # # # 
# Query functions
# # # # # # # # # # # # # # # # # #
def uniquePerWeek(store, day):
    query = (
        "SELECT date(date_trunc('week', first_seen)) time, Count(*) visits "
		f"FROM {store} "
        f"WHERE extract(month FROM first_seen)=extract(month FROM DATE('{day}'))"
		"GROUP BY date_trunc('week', first_seen) "
		"ORDER BY date_trunc('week', first_seen)"
    )
    outputLocation = constructOutputLocation(store, 'monthly_unique_per_week', day)
    return executeQuery(query, outputLocation)


def totalUnique(store, day):
    query = (
        "SELECT COUNT(DISTINCT mac) visits "
        f"FROM {store} "
        f"WHERE extract(month FROM first_seen)=extract(month FROM DATE('{day}'))"
    )
    outputLocation = constructOutputLocation(store, 'monthly_total_unique', day)
    return executeQuery(query, outputLocation)


def totalRepeat(store, day):
    query = (
        "SELECT COUNT(*) repeat_customers "
        "FROM ( "
            "SELECT mac, COUNT(*) visits "
            f"FROM {store} "
            f"WHERE extract(month FROM first_seen)=extract(month FROM DATE('{day}')) "
            "GROUP BY mac "
            "HAVING COUNT(*) > 1 "
            "ORDER BY COUNT(*) DESC"
        ")"
    )
    outputLocation = constructOutputLocation(store, 'monthly_total_repeat', day)
    return executeQuery(query, outputLocation)


def averageVisitDurationInMinutes(store, day):
    query = (
        "SELECT avg(date_diff('minute', first_seen, last_seen)) duration "
        f"FROM {store} "
        f"WHERE extract(month FROM first_seen)=extract(month FROM date('{day}'))"
    )
    outputLocation = constructOutputLocation(store, 'monthly_avg_duration', day)
    return executeQuery(query, outputLocation)