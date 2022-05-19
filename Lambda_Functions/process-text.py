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
import re

ENDPOINT = os.environ['offensive_text_model']
runtime = boto3.Session().client(service_name='sagemaker-runtime',region_name='us-east-1')

aws_access_key_id = "*****************"
aws_secret_access_key = "*****************"

admin_email = "hsapte99@gmail.com"

def accessDB(user):
    client = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
    table = client.Table('user-activity')
    
    try:
        response = table.get_item(TableName='user-activity', Key={'user_id':user})
        count = response['Item']['count']
        print(count)
        if count > 2:
            #print("report sent")
            return True
            
        table.put_item(
            Item={
                'user_id': user,
                'count': count+1,
                }
            )
    except:
        table.put_item(
            Item={
                'user_id': user,
                'count': 1,
                }
            )

    return False
    
def sentimentAnalysis(text, user):   
    comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
    detected_sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    print("Detected Sentiment", detected_sentiment)
    
    sentiment_label = detected_sentiment['Sentiment']
    sentiment_score = detected_sentiment['SentimentScore']['Negative']
    
    if sentiment_score > 0.90:
        # access user from db, add count, check if count reaches 3+ then send a report through SES
        status = accessDB(user)
        if status==True:
            
            ses_client = boto3.client("ses", region_name="us-east-1")
            CHARSET = "UTF-8"
            SUBJECT = "Action Needed Regarding "+user
            BODY_TEXT = "According to your criteria, this " + user+" has 3 strikes on their sentiment score. Please take the action needed."
            response = ses_client.send_email(
            Destination={
                "ToAddresses": [
                    admin_email,
                ],
            },
            Message={
                "Body": {
                    "Text": {
                        "Charset": CHARSET,
                        "Data": BODY_TEXT,
                    }
                },
                "Subject": {
                    "Charset": CHARSET,
                    "Data": SUBJECT,
                },
            },
            Source="****@gmail.com",
            )   
            print("Report sent to the admin")

def insertRecord(index_data):   
    host = 'search-post-search-shshztb24utdmslsekxjaallti.us-east-1.es.amazonaws.com'

    es = OpenSearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth=('*****', '****'),
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    
    print('dataObject', index_data)
    es.index(index="post_tag", doc_type="Post", id=index_data['user_id'], body=index_data, refresh=True)
    
    aws_access_key_id = "*****************"
    aws_secret_access_key = "*****************"
    
    client = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
    table = client.Table('forum_posts')
    response = table.put_item(Item = index_data)
    print(response)
    
def lambda_handler(event, context):
    post_content_pre = json.loads(event['body'])
    post_content = post_content_pre["content_text"]
    
    preds_out = []
    endpoints = ['blazingtext-2022-05-17-09-03-23-314', 'blazingtext-2022-05-17-09-19-33-814', 'blazingtext-2022-05-17-09-34-18-938', 'blazingtext-2022-05-17-09-50-09-461', 'blazingtext-2022-05-17-10-04-39-561', 'blazingtext-2022-05-17-10-20-35-099']
  
    for endpoint in endpoints:
        ENDPOINT = endpoint
        runtime = boto3.Session().client(service_name='sagemaker-runtime',region_name='us-east-1')
    
        aws_access_key_id = "*****************"
        aws_secret_access_key = "*****************"
        
        post_content = re.sub(r'[^\w\s]', '', post_content) 
    
        payload = {"instances" : [post_content]}
    
        response = runtime.invoke_endpoint(
                EndpointName=ENDPOINT,
                Body=json.dumps(payload),
                ContentType='application/json')
    
        print('Model response : ',response)
        print()
    
            #print(payload)
        result = json.loads(response["Body"].read().decode("utf-8"))
        print("Prediction: ",result)
    
        for prediction in result:
            print('Predicted class: {}'.format(prediction['label'][0].lstrip('__label__')))
    
        pred_class = int(prediction['label'][0].lstrip('__label__'))
            # check for offenseive words
        preds_out.append(pred_class)
        print()
        print("pred_class type---->",type(pred_class), "pred_class value---->", pred_class)
            # resp_dict = {}
    
    
    sentiments = ["toxic","severe_toxic", "obscene", "threat", "insult", "identity_hate"]
    res_list = []
    
    for i in range(len(preds_out)):
        if preds_out[i] == 1:
            res_list.append(sentiments[i])
            
    
    
    resp_dict={}
    
    if len(res_list) != 0:
        print("Text is offensive")
        resp_dict['isValid'] = 1
        resp_dict['detect_labels'] = res_list
        
        return {
        "headers": {"Access-Control-Allow-Origin": "*"},
        'statusCode': 200,
        'body': json.dumps(resp_dict)
    }
    else:
        resp_dict['isValid'] = 0
        insertRecord(post_data)
        sentimentAnalysis(post_content['content_text'],post_content['user_id'])
        
        
        aws_access_key_id = "*****************"
        aws_secret_access_key = "*****************"
        
        client = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
        table = client.Table('forum_posts')
        res = table.scan()["Items"]
    
        return {
            "headers": {"Access-Control-Allow-Origin": "*"},
            'statusCode': 200,
            'body': json.dumps({"a":res})
        }

