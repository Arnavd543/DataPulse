import json
import logging
import os
import boto3
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional


logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb = boto3.resource('dynamodb')
cloudwatch = boto3.client('cloudwatch')
quality_table = dynamodb.Table(os.getenv('QUALITY_TABLE_NAME'))

# Z-score threshold (3 = 99.7% of data)
Z_SCORE_THRESHOLD = float(os.getenv('Z_SCORE_THRESHOLD', '3'))

# EWMA smoothing factor (0-1, higher = more weight on recent data)
EWMA_ALPHA = float(os.getenv('EWMA_ALPHA', '0.2'))

# Historical lookback period (days)
LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', '7'))

# Minimum historical data points needed
MIN_HISTORICAL_POINTS = int(os.getenv('MIN_HISTORICAL_POINTS', '3'))

def get_historical_metrics(
    pipeline_id: str,
    days: int = LOOKBACK_DAYS
) -> List[Dict[str, Any]]:
    """
    Fetch historical quality metrics from DynamoDB
    
    Args:
        pipeline_id: Pipeline identifier
        days: Number of days to look back
    
    Returns:
        List of historical metric items, sorted by timestamp ascending
    """
    cutoff_time = (datetime.utcnow() - timedelta(days=days)).isoformat()
    
    try:
        response = quality_table.query(
            KeyConditionExpression='pipeline_id = :pid AND #ts > :cutoff',
            ExpressionAttributeNames={'#ts': 'timestamp'},
            ExpressionAttributeValues={
                ':pid': pipeline_id,
                ':cutoff': cutoff_time,
            },
            ScanIndexForward=True,  # Ascending order (oldest first)
        )
        
        items = response.get('Items', [])
        logger.info(f"Retrieved {len(items)} historical data points for {pipeline_id}")
        
        return items
        
    except Exception as e:
        logger.warning(f"Error fetching historical metrics: {e}")
        return []

def calculate_z_score(
    value: float,
    historical_values: List[float]
) -> Dict[str, Any]:
    """
    Calculate z-score: (value - mean) / std_dev
    
    Z-score indicates how many standard deviations
    a value is from the mean.
    
    Interpretation:
    - |z| < 1: Within 1 std dev (68% of data)
    - |z| < 2: Within 2 std dev (95% of data)
    - |z| < 3: Within 3 std dev (99.7% of data)
    - |z| > 3: Outlier (0.3% of data)
    
    Args:
        value: Current value to test
        historical_values: List of historical values
    
    Returns:
        Dict with z-score, mean, std_dev, and is_anomaly flag
    """
    if len(historical_values) < 2:
        return {
            'z_score': 0,
            'mean': value,
            'std_dev': 0,
            'is_anomaly': False,
            'message': 'Insufficient historical data for z-score',
            'confidence': 'LOW',
        }
    
    # Calculate statistics
    mean = sum(historical_values) / len(historical_values)
    variance = sum((x - mean) ** 2 for x in historical_values) / len(historical_values)
    std_dev = math.sqrt(variance)
    
    if std_dev == 0:
        # No variance in historical data
        is_anomaly = (value != mean)
        return {
            'z_score': 0,
            'mean': mean,
            'std_dev': 0,
            'is_anomaly': is_anomaly,
            'message': 'No variance in historical data',
            'confidence': 'LOW',
        }
    
    # Calculate z-score
    z_score = (value - mean) / std_dev
    is_anomaly = abs(z_score) > Z_SCORE_THRESHOLD
    
    # Determine confidence based on sample size
    if len(historical_values) >= 30:
        confidence = 'HIGH'
    elif len(historical_values) >= 10:
        confidence = 'MEDIUM'
    else:
        confidence = 'LOW'
    
    return {
        'z_score': round(z_score, 2),
        'mean': round(mean, 2),
        'std_dev': round(std_dev, 2),
        'is_anomaly': is_anomaly,
        'threshold': Z_SCORE_THRESHOLD,
        'confidence': confidence,
        'sample_size': len(historical_values),
        'message': f"Z-score: {z_score:.2f} (threshold: ±{Z_SCORE_THRESHOLD})",
    }

def calculate_ewma(
    value: float,
    previous_ewma: float,
    alpha: float = EWMA_ALPHA
) -> float:
    """
    Calculate Exponentially Weighted Moving Average
    
    Formula: EWMA_t = α × value_t + (1 - α) × EWMA_{t-1}
    
    Where:
    - α (alpha): Smoothing factor (0-1)
    - Higher α: More weight on recent values
    - Lower α: More smoothing, slower to react
    
    Args:
        value: Current value
        previous_ewma: Previous EWMA value
        alpha: Smoothing factor (default: 0.2)
    
    Returns:
        New EWMA value
    """
    return alpha * value + (1 - alpha) * previous_ewma

def detect_ewma_drift(
    current_value: float,
    historical_values: List[float],
    alpha: float = EWMA_ALPHA,
    sensitivity: float = 2.0
) -> Dict[str, Any]:
    """
    Detect drift using EWMA
    
    Compares current value against EWMA trend
    Anomaly if deviation exceeds sensitivity × std_dev
    
    Args:
        current_value: Current value to test
        historical_values: List of historical values
        alpha: EWMA smoothing factor
        sensitivity: Deviation multiplier (default: 2.0)
    
    Returns:
        Dict with EWMA, deviation, and is_anomaly flag
    """
    if len(historical_values) < 5:
        return {
            'ewma': current_value,
            'deviation': 0,
            'is_anomaly': False,
            'message': 'Insufficient data for EWMA trend analysis',
        }
    
    # Calculate EWMA from historical data
    ewma = historical_values[0]
    for value in historical_values[1:]:
        ewma = calculate_ewma(value, ewma, alpha)
    
    # Calculate historical standard deviation
    mean = sum(historical_values) / len(historical_values)
    variance = sum((x - mean) ** 2 for x in historical_values) / len(historical_values)
    std_dev = math.sqrt(variance)
    
    # Check if current value deviates from EWMA trend
    deviation = abs(current_value - ewma)
    threshold = sensitivity * std_dev
    is_anomaly = deviation > threshold
    
    return {
        'ewma': round(ewma, 2),
        'current_value': round(current_value, 2),
        'deviation': round(deviation, 2),
        'threshold': round(threshold, 2),
        'is_anomaly': is_anomaly,
        'sensitivity': sensitivity,
        'message': f"Deviation from EWMA: {deviation:.2f} (threshold: {threshold:.2f})",
    }

def detect_anomalies(
    pipeline_id: str,
    current_metrics: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Detect anomalies in current metrics using statistical methods
    
    Checks performed:
    1. Quality score z-score
    2. File size z-score
    3. Quality score EWMA drift
    4. Failed checks count spike
    
    Args:
        pipeline_id: Pipeline identifier
        current_metrics: Current quality metrics
    
    Returns:
        Anomaly report with detected issues and severity
    """
    logger.info(f"Detecting anomalies for pipeline: {pipeline_id}")

    historical = get_historical_metrics(pipeline_id, LOOKBACK_DAYS)
    
    if len(historical) < MIN_HISTORICAL_POINTS:
        logger.info(
            f"Only {len(historical)} historical points, "
            f"need {MIN_HISTORICAL_POINTS} for reliable detection"
        )
        return {
            'has_anomalies': False,
            'anomalies': [],
            'overall_severity': 'NONE',
            'historical_count': len(historical),
            'message': f'Building baseline - need more historical data ({len(historical)}/{MIN_HISTORICAL_POINTS})',
        }
    
    anomalies = []

    current_score = float(current_metrics.get('quality_score', 0))
    historical_scores = [
        float(item.get('quality_score', 0))
        for item in historical
    ]
    
    score_zscore = calculate_z_score(current_score, historical_scores)
    
    if score_zscore['is_anomaly']:
        # Determine severity based on magnitude
        z_magnitude = abs(score_zscore['z_score'])
        
        if z_magnitude > 5:
            severity = 'CRITICAL'
        elif z_magnitude > 4:
            severity = 'HIGH'
        elif z_magnitude > 3:
            severity = 'MEDIUM'
        else:
            severity = 'LOW'
        
        anomalies.append({
            'metric': 'quality_score',
            'detection_method': 'z-score',
            'current_value': current_score,
            'expected_range': [
                round(score_zscore['mean'] - score_zscore['std_dev'], 2),
                round(score_zscore['mean'] + score_zscore['std_dev'], 2)
            ],
            'analysis': score_zscore,
            'severity': severity,
            'description': f"Quality score ({current_score}%) significantly different from historical mean ({score_zscore['mean']}%)",
        })
        
        logger.warning(
            f"Quality score anomaly detected: {current_score}% "
            f"(z-score: {score_zscore['z_score']}, severity: {severity})"
        )
  
    current_size = int(current_metrics.get('file_size', 0))
    historical_sizes = [
        int(item.get('file_size', 0))
        for item in historical
        if item.get('file_size')
    ]
    
    if historical_sizes and current_size > 0:
        size_zscore = calculate_z_score(current_size, historical_sizes)
        
        if size_zscore['is_anomaly']:
            anomalies.append({
                'metric': 'file_size',
                'detection_method': 'z-score',
                'current_value': current_size,
                'analysis': size_zscore,
                'severity': 'MEDIUM',
                'description': f"File size ({current_size} bytes) unusual compared to historical average ({size_zscore['mean']} bytes)",
            })
            
            logger.warning(f"File size anomaly: {current_size} bytes (z-score: {size_zscore['z_score']})")
      
    if len(historical) >= 5:
        ewma_analysis = detect_ewma_drift(current_score, historical_scores)
        
        if ewma_analysis['is_anomaly']:
            anomalies.append({
                'metric': 'quality_score_trend',
                'detection_method': 'ewma',
                'current_value': current_score,
                'expected_value': ewma_analysis['ewma'],
                'analysis': ewma_analysis,
                'severity': 'LOW',
                'description': f"Quality score trending away from EWMA ({ewma_analysis['ewma']}%)",
            })
            
            logger.warning(f"EWMA trend anomaly: deviation={ewma_analysis['deviation']}")
  
    current_failed = int(current_metrics.get('failed_checks', 0))
    historical_failed = [
        int(item.get('failed_checks', 0))
        for item in historical
    ]
    
    if historical_failed:
        failed_zscore = calculate_z_score(current_failed, historical_failed)
        
        if failed_zscore['is_anomaly'] and current_failed > failed_zscore['mean']:
            anomalies.append({
                'metric': 'failed_checks',
                'detection_method': 'z-score',
                'current_value': current_failed,
                'analysis': failed_zscore,
                'severity': 'HIGH',
                'description': f"Unusual spike in failed checks ({current_failed} vs avg {failed_zscore['mean']})",
            })
   
    has_anomalies = len(anomalies) > 0
    
    if has_anomalies:
        severities = [a['severity'] for a in anomalies]
        
        if 'CRITICAL' in severities:
            overall_severity = 'CRITICAL'
        elif 'HIGH' in severities:
            overall_severity = 'HIGH'
        elif 'MEDIUM' in severities:
            overall_severity = 'MEDIUM'
        else:
            overall_severity = 'LOW'
    else:
        overall_severity = 'NONE'

    report = {
        'has_anomalies': has_anomalies,
        'anomaly_count': len(anomalies),
        'anomalies': anomalies,
        'overall_severity': overall_severity,
        'historical_count': len(historical),
        'detection_timestamp': datetime.utcnow().isoformat(),
        'message': (
            f"Detected {len(anomalies)} anomal{'y' if len(anomalies)==1 else 'ies'} "
            f"(severity: {overall_severity})"
            if has_anomalies
            else "No anomalies detected"
        ),
    }
    
    logger.info(f"Anomaly detection complete: {report['message']}")
    
    return report

def publish_anomaly_metrics(
    pipeline_id: str,
    anomaly_report: Dict[str, Any]
):
    """
    Publish anomaly metrics to CloudWatch
    """
    try:
        cloudwatch.put_metric_data(
            Namespace='DataPulse',
            MetricData=[
                {
                    'MetricName': 'AnomalyCount',
                    'Value': anomaly_report['anomaly_count'],
                    'Unit': 'Count',
                    'Dimensions': [
                        {'Name': 'PipelineId', 'Value': pipeline_id},
                    ],
                },
                {
                    'MetricName': 'HasAnomalies',
                    'Value': 1 if anomaly_report['has_anomalies'] else 0,
                    'Unit': 'Count',
                    'Dimensions': [
                        {'Name': 'PipelineId', 'Value': pipeline_id},
                    ],
                },
            ]
        )
        logger.info(f"Published anomaly metrics for {pipeline_id}")
    except Exception as e:
        logger.warning(f"Error publishing CloudWatch metrics: {e}")

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Anomaly detector Lambda handler
    """
    logger.info(f"Anomaly detection event: {json.dumps(event)}")
    
    try:
        
        metadata = event.get('metadata', {})
        
        # Get schema result
        schema_result = event.get('schemaResult', {}).get('Payload', {})
        validation_result = schema_result.get('validation_result', {})
        
        # Get quality result
        quality_result = event.get('qualityResult', {}).get('Payload', {})
        quality_report = quality_result.get('quality_report', {})
        
        pipeline_id = validation_result.get('pipeline_id', 'unknown')
        
        current_metrics = {
            'quality_score': quality_report.get('quality_score', 0),
            'file_size': metadata.get('size', 0),
            'passed_checks': quality_report.get('passed_checks', 0),
            'failed_checks': quality_report.get('failed_checks', 0),
            'total_checks': quality_report.get('total_checks', 0),
        }

        
        anomaly_report = detect_anomalies(pipeline_id, current_metrics)
   
        
        publish_anomaly_metrics(pipeline_id, anomaly_report)
        
        
        logger.info(f"Anomaly detection: {anomaly_report['message']}")
        
        return {
            'statusCode': 200,
            'validation_result': validation_result,
            'quality_report': quality_report,
            'anomaly_report': anomaly_report,
            'metadata': metadata,
        }
        
    except Exception as e:
        logger.error(f"Anomaly detection error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'error': str(e),
            'metadata': metadata,
        }