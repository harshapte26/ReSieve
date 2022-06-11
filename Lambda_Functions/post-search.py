import boto3
import json
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import random

host = 'search-post-search-shshztb24utdmslsekxjaallti.us-east-1.es.amazonaws.com' 

es = Elasticsearch(
    http_auth = ('****', '****'),
    hosts = [{'host': host, 'port': 443}],
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)  

def lambda_handler(event, context):
    # TODO implement
    
    post_tag = event["body"]
    print("pot tag--->", post_tag)
    
    posts_list = findpost(post_tag)
    
    return {
        "headers": {"Access-Control-Allow-Origin": "*"},
        'statusCode': 200,
        'body': json.dumps({"a":posts_list})
    }

def findpost(post_tag):

    host = 'search-post-search-shshztb24utdmslsekxjaallti.us-east-1.es.amazonaws.com'
    index = 'post_tag'
    url = 'https://' + host + '/' + index + '/_search'

    query = {
            "size": 10,
            "query": {
                "multi_match": {
                    "query": post_tag,
                    "fields": ["post_tag"]
                }
            }
        }
        
    awsauth = ('*****','*****')
    headers = { "Content-Type": "application/json" }
    response = requests.get(url,auth=awsauth, headers=headers, data=json.dumps(query))
    res = response.json()
    print("response ",res)
    noOfHits = res['hits']['total']
    hits = res['hits']['hits']
    
    posts = []
    i = 0
    for hit in hits:
        print('hit ',hit['_source'])
        posts.append(hit['_source'])
    print('Posts: ',posts)
    return posts
    