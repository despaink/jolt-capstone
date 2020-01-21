import json
import boto3
import logging

athena_client = boto3.client('athena')

# This is the function that gets triggered on events
def athena(event, context):
    
    response = athena_client.start_query_execution(
        QueryString='SELECT mac FROM "capstone"."scan_data" limit 50;',
        QueryExecutionContext={
            'Database': 'capstone'
        },
        ResultConfiguration={
            'OutputLocation': "s3://jolt.capstone/athena-query-logs/",
        }
    )
    print (response)
    