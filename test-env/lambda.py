import json
import boto3
import urllib.parse
import logging

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Get bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']

    # Decode the object key (in case it is URL-encoded)
    object_key = urllib.parse.unquote_plus(object_key)

    logger.info(f"Bucket: {bucket_name}, Key: {object_key}")

    try:
        # Get the object's metadata
        response = s3.head_object(Bucket=bucket_name, Key=object_key)
        metadata = response['Metadata']

        # Retrieve the specific metadata key
        aidial_resource_type_group = metadata.get('aidial_resource_type_group')

        if aidial_resource_type_group:
            # Set the tag
            tag_set = [
                {
                    'Key': 'aidial_resource_type_group',
                    'Value': aidial_resource_type_group
                }
            ]

            # Put the tag on the object
            s3.put_object_tagging(
                Bucket=bucket_name,
                Key=object_key,
                Tagging={'TagSet': tag_set}
            )

            return {
                'statusCode': 200,
                'body': json.dumps('Tagging successful')
            }
        else:
            return {
                'statusCode': 400,
                'body': json.dumps('Metadata key aidial_resource_type_group not found')
            }
    except s3.exceptions.NoSuchKey:
        logger.error(f"Object {object_key} not found in bucket {bucket_name}")
        return {
            'statusCode': 404,
            'body': json.dumps('Object not found')
        }
    except Exception as e:
        logger.error(f"Error processing object {object_key} from bucket {bucket_name}: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal server error')
        }