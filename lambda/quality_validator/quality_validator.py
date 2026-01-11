import json
import logging
import os
import csv
import boto3
from datetime import datetime, timedelta
from typing import Dict, Any, List
from io import StringIO
from collections import Counter
from decimal import Decimal
import statistics


logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

s3_client = boto3.client('s3')
cloudwatch = boto3.client('cloudwatch')
dynamodb = boto3.resource('dynamodb')
quality_table = dynamodb.Table(os.getenv('QUALITY_TABLE_NAME'))

# Maximum allowed null percentage per column
NULL_PERCENTAGE_THRESHOLD = float(os.getenv('NULL_THRESHOLD', '10'))

# Maximum allowed duplicate percentage for unique columns
DUPLICATE_THRESHOLD = float(os.getenv('DUPLICATE_THRESHOLD', '5'))

# Statistical range: mean ± N standard deviations
RANGE_STDDEV_MULTIPLIER = float(os.getenv('RANGE_STDDEV_MULTIPLIER', '3'))

# Maximum file size to process
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


def check_null_percentage(
    data: List[Dict],
    column: str,
    threshold: float
) -> Dict[str, Any]:
    """
    Check if null percentage in column exceeds threshold
    
    Args:
        data: List of row dictionaries
        column: Column name to check
        threshold: Maximum allowed null percentage (0-100)
    
    Returns:
        Check result with pass/fail status
    
    Example:
        check_null_percentage(data, "email", 10)
        → {"passed": false, "null_percentage": 15.5, ...}
    """
    total_rows = len(data)
    
    if total_rows == 0:
        return {
            'check': 'null_percentage',
            'column': column,
            'passed': False,
            'message': 'No data to check',
        }
    
    # Count nulls (empty strings or None)
    null_count = sum(
        1 for row in data
        if not row.get(column, '').strip()
    )
    
    null_percentage = (null_count / total_rows * 100)
    passed = null_percentage <= threshold
    
    return {
        'check': 'null_percentage',
        'column': column,
        'passed': passed,
        'null_count': null_count,
        'total_rows': total_rows,
        'null_percentage': round(null_percentage, 2),
        'threshold': threshold,
        'severity': 'HIGH' if not passed else 'NONE',
        'message': f"{null_percentage:.2f}% null values (threshold: {threshold}%)"
    }

def check_numeric_range(
    data: List[Dict],
    column: str,
    min_val: float = None,
    max_val: float = None
) -> Dict[str, Any]:
    """
    Check if numeric values fall within expected range
    Uses statistical bounds: mean ± 3σ
    
    Args:
        data: List of row dictionaries
        column: Column name to check
        min_val: Minimum expected value (auto-calculated if None)
        max_val: Maximum expected value (auto-calculated if None)
    
    Returns:
        Check result with range statistics
    """
    values = []
    invalid_count = 0
    parse_errors = 0
    
    # Parse numeric values
    for row in data:
        val_str = row.get(column, '').strip()
        if not val_str:
            continue
        
        try:
            val = float(val_str)
            values.append(val)
        except ValueError:
            parse_errors += 1
    
    if not values:
        return {
            'check': 'numeric_range',
            'column': column,
            'passed': False,
            'message': 'No valid numeric values found',
            'parse_errors': parse_errors,
            'severity': 'HIGH' if parse_errors > 0 else 'NONE',
        }
    
    # Calculate actual range
    actual_min = min(values)
    actual_max = max(values)
    
    # Auto-calculate expected range if not provided
    if min_val is None or max_val is None:
        mean = statistics.mean(values)
        
        if len(values) > 1:
            stdev = statistics.stdev(values)
        else:
            stdev = 0
        
        # Expected range: mean ± 3 standard deviations (99.7% of data)
        if min_val is None:
            min_val = mean - (RANGE_STDDEV_MULTIPLIER * stdev)
        if max_val is None:
            max_val = mean + (RANGE_STDDEV_MULTIPLIER * stdev)
    
    # Count values outside range
    for val in values:
        if val < min_val or val > max_val:
            invalid_count += 1
    
    passed = invalid_count == 0 and parse_errors == 0
    
    return {
        'check': 'numeric_range',
        'column': column,
        'passed': passed,
        'actual_min': round(actual_min, 2),
        'actual_max': round(actual_max, 2),
        'expected_min': round(min_val, 2) if min_val else None,
        'expected_max': round(max_val, 2) if max_val else None,
        'invalid_count': invalid_count,
        'parse_errors': parse_errors,
        'total_values': len(values),
        'severity': 'MEDIUM' if invalid_count > 0 else 'NONE',
        'message': f"Range [{actual_min:.2f}, {actual_max:.2f}], expected [{min_val:.2f if min_val else 'auto'}, {max_val:.2f if max_val else 'auto'}]"
    }

def check_uniqueness(
    data: List[Dict],
    column: str,
    threshold: float
) -> Dict[str, Any]:
    """
    Check for duplicate values in column
    Useful for ID columns that should be unique
    
    Args:
        data: List of row dictionaries
        column: Column name to check
        threshold: Maximum allowed duplicate percentage
    
    Returns:
        Check result with uniqueness statistics
    """
    # Extract non-empty values
    values = [
        row.get(column, '').strip()
        for row in data
        if row.get(column, '').strip()
    ]
    
    total_count = len(values)
    
    if total_count == 0:
        return {
            'check': 'uniqueness',
            'column': column,
            'passed': False,
            'message': 'Column is empty',
            'severity': 'HIGH',
        }
    
    # Count occurrences of each value
    value_counts = Counter(values)
    
    # Find duplicates
    duplicates = {
        val: count
        for val, count in value_counts.items()
        if count > 1
    }
    
    # Calculate duplicate percentage
    duplicate_count = sum(duplicates.values()) - len(duplicates)
    duplicate_percentage = (duplicate_count / total_count * 100)
    
    unique_count = len(value_counts)
    passed = duplicate_percentage <= threshold
    
    return {
        'check': 'uniqueness',
        'column': column,
        'passed': passed,
        'total_values': total_count,
        'unique_values': unique_count,
        'duplicate_count': duplicate_count,
        'duplicate_percentage': round(duplicate_percentage, 2),
        'threshold': threshold,
        'top_duplicates': [
            {'value': val, 'count': count}
            for val, count in sorted(
                duplicates.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]  # Top 5 duplicates
        ],
        'severity': 'HIGH' if not passed else 'NONE',
        'message': f"{duplicate_percentage:.2f}% duplicates (threshold: {threshold}%)"
    }

def run_quality_checks(
    bucket: str,
    key: str,
    schema: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Run all quality checks on data file
    
    Checks performed:
    1. Null percentage for all columns
    2. Numeric range for integer/float columns
    3. Uniqueness for ID-like columns
    
    Args:
        bucket: S3 bucket name
        key: Object key
        schema: Schema from schema validator
    
    Returns:
        Quality report with all check results and overall score
    """
    logger.info(f"Running quality checks on s3://{bucket}/{key}")
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Check file size
        content_length = response['ContentLength']
        if content_length > MAX_FILE_SIZE:
            raise ValueError(f"File too large: {content_length} bytes")
        
        content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(content))
        data = list(csv_reader)
        
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return {
            'passed': False,
            'checks': [],
            'message': f'Error reading file: {e}',
        }
    
    if not data:
        return {
            'passed': False,
            'checks': [],
            'message': 'No data found in file',
        }
    
    logger.info(f"Loaded {len(data)} rows for quality checks")
    
    checks = []
    
    for col, col_info in schema['columns'].items():
        logger.debug(f"Checking column: {col} (type: {col_info['type']})")
        
        null_check = check_null_percentage(data, col, NULL_PERCENTAGE_THRESHOLD)
        checks.append(null_check)
        
        if not null_check['passed']:
            logger.warning(f"Null check failed for {col}: {null_check['message']}")
        
        if col_info['type'] in ['integer', 'float']:
            # Auto-detect reasonable range from data
            try:
                range_check = check_numeric_range(data, col)
                checks.append(range_check)
                
                if not range_check['passed']:
                    logger.warning(f"Range check failed for {col}: {range_check['message']}")
            except Exception as e:
                logger.error(f"Error in range check for {col}: {e}")
        
        # Check uniqueness for columns with 'id' in name
        if 'id' in col.lower() or col.lower().endswith('_id'):
            uniqueness_check = check_uniqueness(data, col, DUPLICATE_THRESHOLD)
            checks.append(uniqueness_check)
            
            if not uniqueness_check['passed']:
                logger.warning(f"Uniqueness check failed for {col}: {uniqueness_check['message']}")
    
    passed_checks = sum(1 for check in checks if check['passed'])
    total_checks = len(checks)
    
    if total_checks > 0:
        quality_score = (passed_checks / total_checks * 100)
    else:
        quality_score = 0
    
    all_passed = all(check['passed'] for check in checks)
    
    # Count checks by severity
    severity_counts = {
        'HIGH': sum(1 for c in checks if c.get('severity') == 'HIGH' and not c['passed']),
        'MEDIUM': sum(1 for c in checks if c.get('severity') == 'MEDIUM' and not c['passed']),
        'LOW': sum(1 for c in checks if c.get('severity') == 'LOW' and not c['passed']),
    }
    
    report = {
        'passed': all_passed,
        'quality_score': round(quality_score, 2),
        'total_checks': total_checks,
        'passed_checks': passed_checks,
        'failed_checks': total_checks - passed_checks,
        'severity_counts': severity_counts,
        'checks': checks,
        'message': f"Quality score: {quality_score:.2f}% ({passed_checks}/{total_checks} checks passed)"
    }
    
    logger.info(f"Quality assessment complete: {report['message']}")
    
    return report

def store_quality_metrics(
    pipeline_id: str,
    quality_report: Dict[str, Any],
    metadata: Dict[str, Any]
):
    """
    Store quality metrics in DynamoDB for historical tracking
    
    Args:
        pipeline_id: Pipeline identifier
        quality_report: Quality check results
        metadata: File metadata
    """
    # Calculate TTL (90 days from now)
    ttl = int((datetime.utcnow() + timedelta(days=90)).timestamp())
    
    item = {
        'pipeline_id': pipeline_id,
        'timestamp': datetime.utcnow().isoformat(),
        'ttl': ttl,
        'quality_score': quality_report['quality_score'],
        'total_checks': quality_report['total_checks'],
        'passed_checks': quality_report['passed_checks'],
        'failed_checks': quality_report['failed_checks'],
        'severity_counts': quality_report['severity_counts'],
        'file_key': metadata['key'],
        'file_size': metadata.get('size', 0),
        'checks_summary': [
            {
                'check': c['check'],
                'column': c.get('column'),
                'passed': c['passed'],
                'severity': c.get('severity', 'NONE'),
                'message': c.get('message', '')
            }
            for c in quality_report['checks']
        ],
    }

    # Convert all floats to Decimal for DynamoDB compatibility
    item = convert_floats_to_decimal(item)

    try:
        quality_table.put_item(Item=item)
        logger.info(f"Stored quality metrics for {pipeline_id}")
    except Exception as e:
        logger.error(f"Error storing quality metrics: {e}")
        # Don't fail the whole function if storage fails
        # This is monitoring data, not critical path

def publish_quality_metrics(
    pipeline_id: str,
    quality_report: Dict[str, Any]
):
    """
    Publish custom metrics to CloudWatch
    
    Metrics published:
    - Quality score (percentage)
    - Failed checks count
    - Passed checks count
    
    Args:
        pipeline_id: Pipeline identifier
        quality_report: Quality check results
    """
    try:
        cloudwatch.put_metric_data(
            Namespace='DataPulse',
            MetricData=[
                {
                    'MetricName': 'QualityScore',
                    'Value': quality_report['quality_score'],
                    'Unit': 'Percent',
                    'Dimensions': [
                        {'Name': 'PipelineId', 'Value': pipeline_id},
                    ],
                    'Timestamp': datetime.utcnow(),
                },
                {
                    'MetricName': 'FailedChecks',
                    'Value': quality_report['failed_checks'],
                    'Unit': 'Count',
                    'Dimensions': [
                        {'Name': 'PipelineId', 'Value': pipeline_id},
                    ],
                    'Timestamp': datetime.utcnow(),
                },
                {
                    'MetricName': 'PassedChecks',
                    'Value': quality_report['passed_checks'],
                    'Unit': 'Count',
                    'Dimensions': [
                        {'Name': 'PipelineId', 'Value': pipeline_id},
                    ],
                    'Timestamp': datetime.utcnow(),
                },
            ]
        )
        logger.info(f"Published CloudWatch metrics for {pipeline_id}")
    except Exception as e:
        logger.warning(f"Error publishing CloudWatch metrics: {e}")

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Quality validation Lambda handler
    
    Process:
    1. Extract metadata and schema from previous step
    2. Run all quality checks on data
    3. Store metrics in DynamoDB
    4. Publish to CloudWatch
    5. Return quality report
    
    Args:
        event: Lambda event (from Step Functions)
        context: Lambda context
    
    Returns:
        Quality report with check results
    """
    logger.info(f"Quality validation event: {json.dumps(event)}")
    
    try:
        
        # Event structure from Step Functions:
        # {
        #   "metadata": {...},
        #   "validation_result": {
        #     "pipeline_id": "...",
        #     "schema": {...}
        #   }
        # }
        
        metadata = event.get('metadata', {})
        validation_result = event.get('validation_result', {})
        
        bucket = metadata.get('bucket')
        key = metadata.get('key')
        pipeline_id = validation_result.get('pipeline_id')
        schema = validation_result.get('schema')
        
        if not all([bucket, key, pipeline_id, schema]):
            raise ValueError("Missing required fields from previous step")
        
        logger.info(f"Running quality checks for pipeline: {pipeline_id}")
        
        quality_report = run_quality_checks(bucket, key, schema)
        
        store_quality_metrics(pipeline_id, quality_report, metadata)
        
        publish_quality_metrics(pipeline_id, quality_report)
        
        logger.info(f"Quality validation complete: {quality_report['message']}")
        
        return {
            'statusCode': 200,
            'validation_result': validation_result,
            'quality_report': quality_report,
            'metadata': metadata,
        }
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {
            'statusCode': 400,
            'error': f"Validation error: {str(e)}",
            'validation_result': validation_result,
            'metadata': metadata,
        }
    
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'error': f"Internal error: {str(e)}",
            'validation_result': validation_result,
            'metadata': metadata,
        }