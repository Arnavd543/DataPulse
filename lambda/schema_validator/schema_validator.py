import json
import logging
import os
import csv
import boto3
from datetime import datetime
from typing import Dict, Any, List, Optional
from io import StringIO
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
schema_table = dynamodb.Table(os.getenv('SCHEMA_TABLE_NAME'))

# Maximum rows to sample for type inference
SAMPLE_SIZE = int(os.getenv('SCHEMA_SAMPLE_SIZE', '1000'))

# Maximum file size to process (10 MB)
MAX_FILE_SIZE = int(os.getenv('MAX_FILE_SIZE_MB', '10')) * 1024 * 1024


def convert_floats_to_decimal(obj: Any) -> Any:
    """
    Recursively convert Python floats to Decimal for DynamoDB compatibility

    DynamoDB doesn't support native Python float types.
    This function walks through nested dicts/lists and converts all floats to Decimal.

    Args:
        obj: Any Python object (dict, list, float, etc.)

    Returns:
        Same structure with floats converted to Decimal
    """
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj

def infer_column_type(value: str) -> str:
    """
    Infer data type from string value
    
    Type priority (in order):
    1. Integer (whole numbers)
    2. Float (decimal numbers)
    3. Boolean (true/false variants)
    4. String (everything else)
    
    Args:
        value: String value from CSV cell
    
    Returns:
        Type name: "integer", "float", "boolean", or "string"
    
    Examples:
        infer_column_type("123") → "integer"
        infer_column_type("123.45") → "float"
        infer_column_type("true") → "boolean"
        infer_column_type("hello") → "string"
        infer_column_type("") → "string"
    """
    # Handle empty values
    if not value or value.strip() == '':
        return 'string'  # Default for nulls
    
    value = value.strip()
    
    # Try integer
    try:
        int(value)
        return 'integer'
    except ValueError:
        pass
    
    # Try float
    try:
        float(value)
        return 'float'
    except ValueError:
        pass
    
    # Try boolean
    if value.lower() in ['true', 'false', 't', 'f', 'yes', 'no', '1', '0']:
        return 'boolean'
    
    # Default to string
    return 'string'


def extract_schema_from_csv(bucket: str, key: str, sample_size: int = SAMPLE_SIZE) -> Dict[str, Any]:
    """
    Extract schema from CSV file by sampling rows
    
    Process:
    1. Download file from S3
    2. Parse CSV headers
    3. Sample up to sample_size rows
    4. Infer type for each column using majority vote
    5. Calculate null percentages
    
    Args:
        bucket: S3 bucket name
        key: Object key (file path)
        sample_size: Number of rows to sample (default: 1000)
    
    Returns:
        Schema dictionary with columns, types, and metadata
    
    Raises:
        ValueError: If file is not valid CSV or empty
        boto3.exceptions.S3UploadFailedError: If S3 download fails
    """
    logger.info(f"Extracting schema from s3://{bucket}/{key}")

    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Check file size
        content_length = response['ContentLength']
        if content_length > MAX_FILE_SIZE:
            raise ValueError(
                f"File too large: {content_length} bytes (max: {MAX_FILE_SIZE})"
            )
        
        # Read and decode
        content = response['Body'].read().decode('utf-8')
        
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        raise

    
    try:
        csv_reader = csv.DictReader(StringIO(content))
        
        # Get field names (column headers)
        fieldnames = csv_reader.fieldnames
        if not fieldnames:
            raise ValueError("CSV has no headers")
        
        logger.info(f"Found {len(fieldnames)} columns: {fieldnames}")
        
    except Exception as e:
        logger.error(f"Error parsing CSV: {e}")
        raise ValueError(f"Invalid CSV format: {e}")

    
    # Store samples for each column
    column_samples = {col: [] for col in fieldnames}
    row_count = 0
    
    for row in csv_reader:
        row_count += 1
        
        # Collect samples for each column
        for col, value in row.items():
            if len(column_samples[col]) < sample_size:
                column_samples[col].append(value if value is not None else '')
        
        # Stop after sampling enough rows
        if row_count >= sample_size:
            break
    
    if row_count == 0:
        raise ValueError("CSV has no data rows")
    
    logger.info(f"Sampled {row_count} rows for type inference")

    
    schema = {
        'columns': {},
        'metadata': {
            'sampled_rows': row_count,
            'column_count': len(fieldnames),
            'extraction_timestamp': datetime.utcnow().isoformat(),
        }
    }
    
    for col, samples in column_samples.items():
        # Count types in samples (majority vote)
        type_counts = {}
        null_count = 0
        
        for value in samples:
            if not value or value.strip() == '':
                null_count += 1
                continue
            
            inferred_type = infer_column_type(value)
            type_counts[inferred_type] = type_counts.get(inferred_type, 0) + 1
        
        # Select most common type
        if type_counts:
            dominant_type = max(type_counts, key=type_counts.get)
            type_confidence = type_counts[dominant_type] / sum(type_counts.values())
        else:
            # All values are null
            dominant_type = 'string'
            type_confidence = 0.0
        
        # Calculate null percentage
        null_percentage = (null_count / len(samples)) * 100 if samples else 0
        
        # Store column schema
        schema['columns'][col] = {
            'type': dominant_type,
            'nullable': null_count > 0,
            'null_percentage': round(null_percentage, 2),
            'type_confidence': round(type_confidence, 2),
            'sample_values': samples[:5],  # First 5 values for reference
            'unique_values': len(set(samples)),
        }
    
    return schema

def store_baseline(pipeline_id: str, schema: Dict[str, Any], is_new: bool = False) -> int:
    """
    Store schema baseline in DynamoDB with versioning
    
    Process:
    1. Query latest version for this pipeline
    2. Clear is_latest flag on old version
    3. Increment version number
    4. Store new baseline with is_latest=true
    
    Args:
        pipeline_id: Pipeline identifier (derived from S3 key)
        schema: Extracted schema dictionary
        is_new: Whether this is the first baseline for this pipeline
    
    Returns:
        Version number of stored baseline
    
    Raises:
        boto3.exceptions.Boto3Error: If DynamoDB operation fails
    """
    try:
        
        response = schema_table.query(
            KeyConditionExpression='pipeline_id = :pid',
            ExpressionAttributeValues={':pid': pipeline_id},
            ScanIndexForward=False,  # Descending order (latest first)
            Limit=1
        )
        
        if response['Items']:
            # Existing baseline found
            latest_version = int(response['Items'][0]['version'])
            
            # Clear is_latest flag on old version
            schema_table.update_item(
                Key={
                    'pipeline_id': pipeline_id,
                    'version': latest_version
                },
                UpdateExpression='SET is_latest = :false',
                ExpressionAttributeValues={':false': 'false'}
            )
            
            new_version = latest_version + 1
            logger.info(f"Upgrading from v{latest_version} to v{new_version}")
        else:
            # First baseline for this pipeline
            new_version = 1
            logger.info(f"Creating initial baseline v{new_version}")
        
    except Exception as e:
        logger.warning(f"Error checking existing version: {e}")
        new_version = 1


    # Convert all floats to Decimal for DynamoDB compatibility
    schema_for_dynamo = convert_floats_to_decimal(schema)

    item = {
        'pipeline_id': pipeline_id,
        'version': new_version,
        'schema': schema_for_dynamo,
        'is_latest': 'true',
        'created_at': datetime.utcnow().isoformat(),
        'is_initial_baseline': is_new,
    }

    try:
        schema_table.put_item(Item=item)
        logger.info(
            f"Stored baseline v{new_version} for {pipeline_id} "
            f"({len(schema['columns'])} columns)"
        )
    except Exception as e:
        logger.error(f"Error storing baseline: {e}")
        raise

    return new_version


def detect_schema_drift(
    current_schema: Dict[str, Any],
    baseline_schema: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compare current schema against baseline to detect drift
    
    Checks for:
    1. New columns (not in baseline)
    2. Missing columns (in baseline but not in current)
    3. Type changes (column exists in both but type different)
    
    Args:
        current_schema: Schema from current file
        baseline_schema: Stored baseline schema
    
    Returns:
        Drift report with has_drift flag and details
    
    Example output:
        {
            "has_drift": True,
            "new_columns": ["country"],
            "missing_columns": [],
            "type_changes": [
                {"column": "age", "old_type": "integer", "new_type": "string"}
            ],
            "summary": "Schema drift detected: 1 new columns, 1 type changes"
        }
    """
    drift = {
        'has_drift': False,
        'new_columns': [],
        'missing_columns': [],
        'type_changes': [],
        'summary': '',
    }
    
    # Extract column sets
    current_cols = set(current_schema['columns'].keys())
    baseline_cols = set(baseline_schema['columns'].keys())
 
    
    new_cols = current_cols - baseline_cols
    if new_cols:
        drift['has_drift'] = True
        drift['new_columns'] = sorted(list(new_cols))
        logger.warning(f"Detected {len(new_cols)} new columns: {new_cols}")
    
    missing_cols = baseline_cols - current_cols
    if missing_cols:
        drift['has_drift'] = True
        drift['missing_columns'] = sorted(list(missing_cols))
        logger.warning(f"Detected {len(missing_cols)} missing columns: {missing_cols}")
    
    
    common_cols = current_cols & baseline_cols
    for col in common_cols:
        current_type = current_schema['columns'][col]['type']
        baseline_type = baseline_schema['columns'][col]['type']
        
        if current_type != baseline_type:
            drift['has_drift'] = True
            drift['type_changes'].append({
                'column': col,
                'old_type': baseline_type,
                'new_type': current_type,
            })
            logger.warning(
                f"Type change in '{col}': {baseline_type} → {current_type}"
            )

    
    if drift['has_drift']:
        parts = []
        if drift['new_columns']:
            parts.append(f"{len(drift['new_columns'])} new column(s)")
        if drift['missing_columns']:
            parts.append(f"{len(drift['missing_columns'])} missing column(s)")
        if drift['type_changes']:
            parts.append(f"{len(drift['type_changes'])} type change(s)")
        
        drift['summary'] = f"Schema drift detected: {', '.join(parts)}"
    else:
        drift['summary'] = "No schema drift detected"
    
    return drift


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Schema validation Lambda handler
    
    Process:
    1. Extract metadata from event
    2. Derive pipeline_id from S3 key
    3. Extract current schema from CSV
    4. Retrieve baseline schema from DynamoDB
    5. Compare schemas and detect drift
    6. Store new baseline if first time
    7. Return validation result
    
    Args:
        event: Lambda event (from Step Functions or direct invocation)
        context: Lambda context
    
    Returns:
        Validation result with schema and drift information
    """
    logger.info(f"Schema validation event: {json.dumps(event)}")
    
    try:

        
        # Event can come from Step Functions or direct invocation
        metadata = event.get('metadata', {})
        
        bucket = metadata.get('bucket')
        key = metadata.get('key')
        
        if not bucket or not key:
            raise ValueError("Missing required fields: bucket, key")
        
        # Derive pipeline_id from S3 key
        # Example: raw/customers/data.csv → pipeline_id = "customers"
        # Example: raw/sales/2024/data.csv → pipeline_id = "sales"
        key_parts = key.split('/')
        if len(key_parts) >= 2:
            pipeline_id = key_parts[1]  # Second part after raw/
        else:
            pipeline_id = 'default'
        
        logger.info(f"Processing pipeline: {pipeline_id}")

        
        current_schema = extract_schema_from_csv(bucket, key)
        logger.info(
            f"Extracted schema: {current_schema['metadata']['column_count']} columns"
        )

        
        try:
            # Query using GSI for latest version
            response = schema_table.query(
                IndexName='pipeline-latest-index',
                KeyConditionExpression='pipeline_id = :pid AND is_latest = :latest',
                ExpressionAttributeValues={
                    ':pid': pipeline_id,
                    ':latest': 'true'
                }
            )
            
            if response['Items']:
                # Baseline exists
                baseline_schema = response['Items'][0]['schema']
                baseline_version = response['Items'][0]['version']
                is_new_pipeline = False
                logger.info(f"Found baseline v{baseline_version}")
            else:
                # No baseline - first time seeing this pipeline
                baseline_schema = current_schema
                baseline_version = None
                is_new_pipeline = True
                logger.info("No baseline found - creating initial baseline")
        
        except Exception as e:
            logger.warning(f"Error fetching baseline: {e}")
            # Treat as new pipeline
            baseline_schema = current_schema
            baseline_version = None
            is_new_pipeline = True

        
        drift_report = detect_schema_drift(current_schema, baseline_schema)
        logger.info(f"Drift detection: {drift_report['summary']}")

        
        if is_new_pipeline:
            version = store_baseline(pipeline_id, current_schema, is_new=True)
            logger.info(f"Created initial baseline v{version}")
        
        
        result = {
            'pipeline_id': pipeline_id,
            'schema': current_schema,
            'drift': drift_report,
            'baseline_version': baseline_version,
            'is_new_pipeline': is_new_pipeline,
            'validation_timestamp': datetime.utcnow().isoformat(),
        }
        
        return {
            'statusCode': 200,
            'validation_result': result,
            'metadata': metadata,
        }
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {
            'statusCode': 400,
            'error': f"Validation error: {str(e)}",
            'metadata': metadata,
        }
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'error': f"Internal error: {str(e)}",
            'metadata': metadata,
        }