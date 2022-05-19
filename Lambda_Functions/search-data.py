import json
import boto3
from boto3.dynamodb.conditions import Key
import decimal
from opensearchpy import OpenSearch, RequestsHttpConnection
from elasticsearch import Elasticsearch, RequestsHttpConnection
from botocore.vendored import requests
import os
import io
import sys
import requests

def lambda_handler(event, context):
    
    aws_access_key_id = "*****************"
    aws_secret_access_key = "*****************"
    
    client = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
    table = client.Table('forum_posts')
    res = table.scan()["Items"]
    res = sorted(res, key=lambda d: d['timestamp'], reverse=True)

    return {
        "headers": {"Access-Control-Allow-Origin": "*"},
        'statusCode': 200,
        'body': json.dumps({"a":res})
    }