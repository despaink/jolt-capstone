import json
import boto3
import logging
import time
import csv
from datetime import date, timedelta

athena_client = boto3.client('athena')
s3_client = boto3.client('s3')

# triggered at 5am every morning
def handle(event, context):
    # extract storeName from event?
    campaignName = event.campaignName
    campaignType = event.campaignType
    campaignStart = event.campaignStart
    campaignEnd = event.campaignEnd
    storeName = event.storeName
    createdDate = (date.today()).strftime('%Y-%m-%d')
    
    responses = []

    if campaignType == "compareAll":
      responses.append(compareAllCampaign(storeName, campaignName, campaignStart, campaignEnd))
    
    print(responses)
    return {
        'statusCode': 200,
        'body': responses
    }


# # # # # # # # # # # # # # # # # # 
# Helper functions
# # # # # # # # # # # # # # # # # # 
def executeQuery(query, outputLocation):
    return athena_client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = { 'Database': 'capstone' },
        ResultConfiguration = { 'OutputLocation': outputLocation }
    )


def constructOutputLocation(storeName, campaignName):
    return f's3://jolt.capstone/athena-query-logs/{storeName}/campaigns/{campaignName}'

def waitForFinish(queryId):
  state = 'RUNNING'
  while (state == 'RUNNING'):
    response = athena_client.get_query_execution(queryId)
    if 'QueryExecution' in response and \
              'Status' in response['QueryExecution'] and \
              'State' in response['QueryExecution']['Status']:
          state = response['QueryExecution']['Status']['State']
          if state == 'FAILED':
              return False
          elif state == 'SUCCEEDED':
              return athena_client.get_query_results(queryId)
    time.sleep(1)


# # # # # # # # # # # # # # # # # # 
# Query functions
# # # # # # # # # # # # # # # # # # 

def compareAllCampaign(storeName, campaignName, start, end):
    query1 = (
        "SELECT date(date_trunc('day', first_seen)) time, Count(*) visits "
		f"FROM {storeName} "
        f"WHERE DATE(first_seen) BETWEEN DATE('{start}') and DATE('{end}') "
		"GROUP BY date_trunc('hour', first_seen) "
		"ORDER BY date_trunc('hour', first_seen)"
    )
    outputLocation = constructOutputLocation(storeName, campaignName)
    queryId1 = executeQuery(query1, outputLocation)
    
    query2 = (
        "SELECT date(date_trunc('day', first_seen)) time, Count(*) visits "
		f"FROM {storeName} "
		"GROUP BY date_trunc('hour', first_seen) "
		"ORDER BY date_trunc('hour', first_seen)"
    )
    queryId2 = executeQuery(query2, outputLocation)


    results1 = waitForFinish(queryId1)['ResultSet']['Rows']
    results2 = waitForFinish(queryId2)['ResultSet']['Rows']

    file1 = open('file1.csv', 'w')
    with file1:
      writer = csv.writer(file1)
      for row in results1:
        writer.writerow(row)

    s3_client.upload_file(file1, "s3://jolt.capstone", outputLocation + "target")
    s3_client.upload_file(file1, "s3://jolt.capstone", outputLocation + "compare")
    return 1
