import json
import boto3
import logging

athena_client = boto3.client('athena')

# triggered at 5am every morning
def handle(event, context):
    #        QueryString="select created interval, count(*) count from (  select substr(to_char(first_seen, 'hh24:mi'), 1, 4) || '0' created from test_timestamp ) group by created order by created;",

    response = athena_client.start_query_execution(
        QueryString="SELECT mac FROM capstone.scan_data limit 10;",
        QueryExecutionContext={
            'Database': 'capstone'
        },
        ResultConfiguration={
            'OutputLocation': 's3://jolt.capstone/athena-query-logs/dtest/daily',
        }
    )
    print(response)   
    return response 