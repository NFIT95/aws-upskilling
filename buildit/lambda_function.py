import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    """
    This function is triggered by an S3 event. It retrieves the metadata
    of the object that was uploaded and returns its content type.
    """
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the bucket and key from the S3 event record
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        # Retrieve the object from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        
        # Log and return the content type
        content_type = response['ContentType']
        print(f"CONTENT TYPE: {content_type}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'bucket': bucket,
                'key': key,
                'contentType': content_type
            })
        }
        
    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket}. '
              f'Make sure they exist and your bucket is in the same region as this function.')
        raise e