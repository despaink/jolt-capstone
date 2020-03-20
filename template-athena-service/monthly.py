import json
import boto3
import logging
from datetime import date

athena_client = boto3.client('athena')
FIRST_SEEN = "date_parse(trim(first_seen), '%Y-%m-%d %H:%i:%s')"
LAST_SEEN = "date_parse(trim(last_seen), '%Y-%m-%d %H:%i:%s')"

# triggered at 5am on the first day of every month
def handle(event, context):
    responses = []
    for storeName in event.get('store'):
        day = date.today()

        res = [storeName]

        res.append(uniquePerWeek(storeName, day))
        res.append(totalUnique(storeName, day))
        res.append(totalRepeat(storeName, day))
        res.append(averageVisitDurationInMinutes(storeName, day))

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
def uniquePerWeek(storeName, day):
    query = (
        f"SELECT date(date_trunc('week', {FIRST_SEEN})) week, Count(*) visits "
		f"FROM {storeName} "
        f"WHERE month({FIRST_SEEN})=month(DATE('{day}')) AND \"$PATH\" NOT LIKE '%metadata' "
		f"GROUP BY date_trunc('week', {FIRST_SEEN}) "
		f"ORDER BY date_trunc('week', {FIRST_SEEN})"
    )
    outputLocation = constructOutputLocation(storeName, 'monthly_unique_per_week', day)
    return executeQuery(query, outputLocation)


def totalUnique(storeName, day):
    query = (
        "SELECT COUNT(DISTINCT mac) visits "
        f"FROM {storeName} "
        f"WHERE month({FIRST_SEEN})=month(DATE('{day}')) AND \"$PATH\" NOT LIKE '%metadata'"
    )
    outputLocation = constructOutputLocation(storeName, 'monthly_total_unique', day)
    return executeQuery(query, outputLocation)


def totalRepeat(storeName, day):
    query = (
        "SELECT COUNT(*) repeat_customers "
        "FROM ( "
            "SELECT mac, COUNT(*) visits "
            f"FROM {storeName} "
            f"WHERE month({FIRST_SEEN})=month(DATE('{day}')) AND \"$PATH\" NOT LIKE '%metadata' "
            "GROUP BY mac "
            "HAVING COUNT(*) > 1 "
            "ORDER BY COUNT(*) DESC"
        ")"
    )
    outputLocation = constructOutputLocation(storeName, 'monthly_total_repeat', day)
    return executeQuery(query, outputLocation)


def averageVisitDurationInMinutes(storeName, day):
    query = (
        f"SELECT avg(date_diff('minute', {FIRST_SEEN}, {LAST_SEEN})) duration "
        f"FROM {storeName} "
        f"WHERE month({FIRST_SEEN})=month(date('{day}')) AND \"$PATH\" NOT LIKE '%metadata'"
    )
    outputLocation = constructOutputLocation(storeName, 'monthly_avg_duration', day)
    return executeQuery(query, outputLocation)