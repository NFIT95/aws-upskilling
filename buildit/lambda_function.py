import os
import boto3
import csv
import json
import urllib.parse
import uuid

# Initialize AWS clients
s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')

# Get the Kinesis stream name from environment variables
KINESIS_STREAM_NAME = os.environ.get('KINESIS_STREAM_NAME')
# Define the maximum number of records to send in a single PutRecords call
KINESIS_BATCH_SIZE = 500

def lambda_handler(event, context):
    """
    This function is triggered by an S3 event. It reads a CSV file,
    parses its records, and writes them to a Kinesis Data Stream.
    """

    # 1. Get the bucket and key from the S3 event record
    try:
        s3_record = event['Records'][0]['s3']
        bucket_name = s3_record['bucket']['name']
        # The key may have URL-encoded characters (e.g., spaces as '+')
        object_key = urllib.parse.unquote_plus(s3_record['object']['key'], encoding='utf-8')
    except (KeyError, IndexError) as e:
        print(f"Error: Could not extract bucket/key from event. {e}")
        return {'statusCode': 400, 'body': 'Invalid S3 event format.'}

    print(f"Processing file: s3://{bucket_name}/{object_key}")

    # 2. Get the CSV file content from S3
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        # Decode the file content and split into lines
        file_content = response['Body'].read().decode('utf-8').splitlines()
        csv_reader = csv.reader(file_content)
        # Assume the first row is the header
        header = next(csv_reader)
    except Exception as e:
        print(f"Error getting or parsing S3 object: {e}")
        return {'statusCode': 500, 'body': 'Failed to process S3 object.'}

    records_batch = []
    records_sent_count = 0

    # 3. Process each row and send to Kinesis in batches
    for row in csv_reader:
        # Create a dictionary from header and row values
        record_data = dict(zip(header, row))

        # Prepare the record for the Kinesis PutRecords API call
        # Data must be bytes, so we serialize the dictionary to a JSON string and encode it.
        # PartitionKey is used by Kinesis to distribute data across shards.
        # A random UUID is a good choice for even distribution.
        kinesis_record = {
            'Data': json.dumps(record_data) + '\n', # Add newline for downstream consumers
            'PartitionKey': str(uuid.uuid4())
        }
        records_batch.append(kinesis_record)

        # 4. When the batch is full, send it to Kinesis
        if len(records_batch) == KINESIS_BATCH_SIZE:
            _send_batch_to_kinesis(records_batch)
            records_sent_count += len(records_batch)
            records_batch = [] # Clear the batch

    # 5. Send any remaining records in the last batch
    if records_batch:
        _send_batch_to_kinesis(records_batch)
        records_sent_count += len(records_batch)

    print(f"Successfully processed and sent {records_sent_count} records from {object_key}.")
    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully sent {records_sent_count} records.')
    }


def _send_batch_to_kinesis(batch):
    """Helper function to send a batch of records to Kinesis."""
    try:
        response = kinesis.put_records(
            StreamName=KINESIS_STREAM_NAME,
            Records=batch
        )
        if response.get('FailedRecordCount', 0) > 0:
            print(f"Warning: {response['FailedRecordCount']} records failed to be sent.")
            # Basic error logging. For production, you might want to retry failed records.
            for record in response.get('Records', []):
                if 'ErrorCode' in record:
                    print(f"  - Failed Record: {record['ErrorCode']}: {record['ErrorMessage']}")
        else:
            print(f"Successfully sent a batch of {len(batch)} records.")
    except Exception as e:
        print(f"Error sending batch to Kinesis: {e}")