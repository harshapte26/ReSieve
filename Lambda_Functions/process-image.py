import json
import boto3
import base64
import requests

aws_access_key_id="*****************",
aws_secret_access_key="*****************"

s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

s3_res = boto3.resource(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def moderate_image(photo, bucket):

    client=boto3.client('rekognition')

    response = client.detect_moderation_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}})

    print('Detected labels for ' + photo)    
    for label in response['ModerationLabels']:
        print (label['Name'] + ' : ' + str(label['Confidence']))
        print (label['ParentName'])
    return len(response['ModerationLabels'])

def lambda_handler(event, context):
    # TODO implement
    print("event------>>>>", event)
    post_content = json.loads(event['body'])
    image_base64 = post_content['content_image']
    content_type = event['headers']['content-type']
    file_name_with_extention = event['headers']['filename']
    obj = s3_res.Object('resieve-image',file_name_with_extention)
    obj.put(Body=base64.b64decode(image_base64), ContentType=content_type)
    
    bucket='resieve-image'
    label_count=moderate_image(file_name_with_extention, bucket)

    
    aws_access_key_id = aws_access_key_id
    aws_secret_access_key = aws_secret_access_key
    client = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
    table = client.Table('forum_posts')
    
    resp_dict = {}
    resp_dict['isValid'] = 0
    if label_count==0:
        resp_dict['isValid'] = 1
        
        index_data={
        "user_id":post_content['user_id'],
        "post_id":post_content['post_id'],
        'post_title': post_content['post_title'],
        'post_tag': post_content['post_tag'],
        'content_text': 'Null',
        'content_image': file_name_with_extention,
        'upvotes': post_content['upvotes'],
        'timestamp': post_content['timestamp']
        }
        
        response = table.put_item(Item = index_data)
        
        print("Labels detected: " + str(label_count))
    
    
        return {
            "headers": {"Access-Control-Allow-Origin": "*"},
            'statusCode': 200,
            'body': json.dumps(resp_dict)
        }
        
    else:
        print("HERE IN DETECTED")
        resp_dict['isValid'] = 0
        bucket_fetch = s3_res.Bucket('resieve-image')
        object_bucket = bucket_fetch.Object(file_name_with_extention)
        media = object_bucket.get().get('Body').read()
        print("after bucket read!!!!")
        # print("media", media)
        
        params = {
          'concepts': 'alcohol, weapon, profanity, recreational-drug',
          'api_user': '1299563353',
          'api_secret': 'yGueGQCGt64kcynYSkqr'
        }
        
        # files = {'media': open('https://resieve-image.s3.amazonaws.com/beer.jpeg', 'rb')}
        files = {'media':media}
        r = requests.post('https://api.sightengine.com/1.0/transform.json', files=files, data=params)
        
        print("RESPONSE RECEIVED!!!!!!++++>>>>", r, r.text)
        
        output = json.loads(r.text)
        print("Image detect output ---->>>", output)
        print("Image operation status --> ",output["status"])
        image_data_clean = output['transform']['base64']
        obj_clean = s3_res.Object('resieve-image','cleaned_'+file_name_with_extention)
        obj_clean.put(Body=base64.b64decode(image_data_clean), ContentType=output['transform']["content-type"])
        
        index_data={
        "user_id":post_content['user_id'],
        "post_id":post_content['post_id'],
        'post_title': post_content['post_title'],
        'post_tag': post_content['post_tag'],
        'content_text': 'Null',
        'content_image': 'cleaned_'+file_name_with_extention,
        'upvotes': post_content['upvotes'],
        'timestamp': post_content['timestamp']
        }
        
        response = table.put_item(Item = index_data)
        
        
        return {
            "headers": {"Access-Control-Allow-Origin": "*"},
            'statusCode': 200,
            'body': json.dumps(resp_dict)
        }
    
