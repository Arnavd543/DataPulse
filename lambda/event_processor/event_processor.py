import json
import logging
import os
import boto3
from typing import Dict, Any
from datetime import datetime

# Configure structured logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3_client = boto3.client('s3')
sfn_client = boto3.client('stepfunctions')


def extract_s3_event_details(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract S3 event details from EventBridge event structure
    
    Args:
        event: Lambda event object (EventBridge format)
    
    Returns:
        Dictionary with bucket, key, size, and event metadata
    
    Raises:
        KeyError: If required fields missing from event
    """
    # EventBridge wraps S3 events in 'detail' field
    # Direct S3 events (legacy) are in root
    if 'detail' in event:
        s3_event = event['detail']
        event_source = "EventBridge"
    else:
        s3_event = event
        event_source = "S3 Direct"
    
    # Extract core fields with validation
    try:
        bucket = s3_event['bucket']['name']
        key = s3_event['object']['key']
        size = s3_event['object']['size']
    except KeyError as e:
        logger.error(f"Missing required field in S3 event: {e}")
        raise ValueError(f"Invalid S3 event structure: missing {e}")
    
    # Extract optional fields
    etag = s3_event.get('object', {}).get('etag', 'unknown')
    sequencer = s3_event.get('object', {}).get('sequencer', 'unknown')
    
    return {
        'bucket': bucket,
        'key': key,
        'size': size,
        'etag': etag,
        'sequencer': sequencer,
        'event_source': event_source,
    }

def get_object_metadata(bucket: str, key: str) -> Dict[str, Any]:
    """
    Retrieve detailed object metadata from S3
    
    Args:
        bucket: S3 bucket name
        key: Object key (file path)
    
    Returns:
        Dictionary with metadata (content type, modified time, etc.)
    
    Raises:
        botocore.exceptions.ClientError: If S3 operation fails
    """
    try:
        # HeadObject returns metadata without downloading file
        response = s3_client.head_object(Bucket=bucket, Key=key)
        
        metadata = {
            'content_type': response.get('ContentType', 'unknown'),
            'content_length': response.get('ContentLength', 0),
            'last_modified': response['LastModified'].isoformat(),
            'etag': response['ETag'].strip('"'),  # Remove quotes from ETag
            'version_id': response.get('VersionId', None),
            'metadata': response.get('Metadata', {}),  # User-defined metadata
            'storage_class': response.get('StorageClass', 'STANDARD'),
        }
        
        return metadata
        
    except Exception as e:
        logger.error(f"Error retrieving metadata for s3://{bucket}/{key}: {e}")
        raise


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for S3 event processing
    Now triggers Step Functions workflow
    
    Args:
        event: Lambda event object (from EventBridge)
        context: Lambda context object
    
    Returns:
        HTTP-style response with execution details
    """
    logger.info(f"Received event: {json.dumps(event)}")
    logger.info(f"Request ID: {context.aws_request_id}")
    
    try:
        
        event_details = extract_s3_event_details(event)
        
        bucket = event_details['bucket']
        key = event_details['key']
        size = event_details['size']
        
        logger.info(f"Processing file: s3://{bucket}/{key} ({size} bytes)")
        
        metadata = get_object_metadata(bucket, key)
        
        complete_metadata = {
            **event_details,
            **metadata,
            'processing_timestamp': datetime.utcnow().isoformat(),
        }
        
        # Get state machine ARN from environment
        state_machine_arn = os.getenv('STATE_MACHINE_ARN')
        
        if not state_machine_arn:
            raise ValueError("STATE_MACHINE_ARN environment variable not set")
        
        # Create unique execution name
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S-%f')
        filename = key.split('/')[-1].replace('.', '-')
        execution_name = f"exec-{timestamp}-{filename}"[:80]  # Max 80 chars
        
        # Initialize Step Functions client
        sfn_client = boto3.client('stepfunctions')
        
        # Start execution
        sfn_response = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            name=execution_name,
            input=json.dumps({
                'metadata': complete_metadata,
                'trigger_time': datetime.utcnow().isoformat(),
                'trigger_source': 'event_processor',
            })
        )
        
        execution_arn = sfn_response['executionArn']
        
        logger.info(f"Started Step Functions execution: {execution_arn}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Workflow started successfully',
                'execution_arn': execution_arn,
                'execution_name': execution_name,
                'metadata': complete_metadata,
            }, default=str)
        }
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }