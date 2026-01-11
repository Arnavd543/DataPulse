import json
import logging
import os
import boto3
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

sns_client = boto3.client('sns')

ALERT_TOPIC_ARN = os.getenv('ALERT_TOPIC_ARN')
CRITICAL_ALERT_TOPIC_ARN = os.getenv('CRITICAL_ALERT_TOPIC_ARN')

def format_schema_drift_alert(
    drift: Dict[str, Any],
    pipeline_id: str,
    file_key: str
) -> str:
    """
    Format schema drift alert message
    
    Creates detailed, actionable alert for schema changes
    """
    lines = [
        "=" * 70,
        "🚨 SCHEMA DRIFT DETECTED",
        "=" * 70,
        "",
        f"Pipeline: {pipeline_id}",
        f"File: {file_key}",
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "SUMMARY",
        "-" * 70,
        f"{drift['summary']}",
        "",
    ]
    
    # New columns section
    if drift.get('new_columns'):
        lines.append("NEW COLUMNS DETECTED:")
        lines.append("-" * 70)
        for col in drift['new_columns']:
            lines.append(f"  + {col}")
        lines.append("")
    
    # Missing columns section
    if drift.get('missing_columns'):
        lines.append("MISSING COLUMNS:")
        lines.append("-" * 70)
        for col in drift['missing_columns']:
            lines.append(f"  - {col}")
        lines.append("")
    
    # Type changes section
    if drift.get('type_changes'):
        lines.append("TYPE CHANGES:")
        lines.append("-" * 70)
        for change in drift['type_changes']:
            lines.append(
                f"  • {change['column']}: "
                f"{change['old_type']} → {change['new_type']}"
            )
        lines.append("")
    
    # Action items
    lines.extend([
        "ACTION REQUIRED",
        "-" * 70,
        "1. Review data source for unexpected schema changes",
        "2. Verify with data engineering team if change was intentional",
        "3. Update pipeline configuration if schema change is permanent",
        "4. Contact data team if schema change was unauthorized",
        "",
        "INVESTIGATION STEPS",
        "-" * 70,
        "• Check upstream ETL processes for recent modifications",
        "• Review S3 bucket for additional files with same schema",
        "• Query DynamoDB schema table for baseline comparison",
        "• Examine CloudWatch logs for related errors",
        "",
        "=" * 70,
    ])
    
    return "\n".join(lines)

def format_quality_failure_alert(
    quality_report: Dict[str, Any],
    pipeline_id: str,
    file_key: str
) -> str:
    """
    Format quality failure alert message
    
    Highlights failed checks with specific details
    """
    lines = [
        "=" * 70,
        "⚠️  DATA QUALITY FAILURE",
        "=" * 70,
        "",
        f"Pipeline: {pipeline_id}",
        f"File: {file_key}",
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "QUALITY METRICS",
        "-" * 70,
        f"Quality Score: {quality_report['quality_score']}%",
        f"Checks Passed: {quality_report['passed_checks']}/{quality_report['total_checks']}",
        f"Checks Failed: {quality_report['failed_checks']}",
        "",
    ]
    
    # Failed checks by severity
    severity_counts = quality_report.get('severity_counts', {})
    if severity_counts:
        lines.append("FAILURES BY SEVERITY:")
        lines.append("-" * 70)
        if severity_counts.get('HIGH', 0) > 0:
            lines.append(f"  🔴 HIGH:   {severity_counts['HIGH']}")
        if severity_counts.get('MEDIUM', 0) > 0:
            lines.append(f"  🟡 MEDIUM: {severity_counts['MEDIUM']}")
        if severity_counts.get('LOW', 0) > 0:
            lines.append(f"  🟢 LOW:    {severity_counts['LOW']}")
        lines.append("")
    
    # Detailed failed checks
    lines.append("FAILED CHECKS (DETAILS)")
    lines.append("-" * 70)
    
    failed_checks = [c for c in quality_report.get('checks', []) if not c['passed']]
    
    for i, check in enumerate(failed_checks, 1):
        severity_icon = {
            'HIGH': '🔴',
            'MEDIUM': '🟡',
            'LOW': '🟢',
        }.get(check.get('severity', 'NONE'), '⚪')
        
        lines.append(f"{i}. {severity_icon} {check['check'].upper()}")
        lines.append(f"   Column: {check.get('column', 'N/A')}")
        lines.append(f"   Issue: {check.get('message', 'Check failed')}")
        
        # Add specific details based on check type
        if check['check'] == 'null_percentage':
            lines.append(f"   Null %: {check.get('null_percentage')}% (threshold: {check.get('threshold')}%)")
            lines.append(f"   Null count: {check.get('null_count')}/{check.get('total_rows')}")
        
        elif check['check'] == 'numeric_range':
            lines.append(f"   Actual range: [{check.get('actual_min')}, {check.get('actual_max')}]")
            lines.append(f"   Expected range: [{check.get('expected_min')}, {check.get('expected_max')}]")
            lines.append(f"   Invalid values: {check.get('invalid_count')}")
        
        elif check['check'] == 'uniqueness':
            lines.append(f"   Duplicate %: {check.get('duplicate_percentage')}% (threshold: {check.get('threshold')}%)")
            lines.append(f"   Unique values: {check.get('unique_values')}/{check.get('total_values')}")
            
            # Show top duplicates if available
            top_dupes = check.get('top_duplicates', [])
            if top_dupes:
                lines.append("   Top duplicates:")
                for dupe in top_dupes[:3]:
                    lines.append(f"     - '{dupe['value']}' appears {dupe['count']} times")
        
        lines.append("")
    
    # Action items
    lines.extend([
        "ACTION REQUIRED",
        "-" * 70,
        "1. Investigate data source for quality issues",
        "2. Check ETL processes for data transformation errors",
        "3. Verify upstream data dependencies",
        "4. Review recent code changes that might affect data",
        "5. Consider quarantining this data file until issues resolved",
        "",
        "INVESTIGATION CHECKLIST",
        "-" * 70,
        "[ ] Check source database for data corruption",
        "[ ] Review ETL logs for processing errors",
        "[ ] Validate data against business rules",
        "[ ] Contact data owners/providers",
        "[ ] Update data validation rules if needed",
        "",
        "=" * 70,
    ])
    
    return "\n".join(lines)

def format_anomaly_alert(
    anomaly_report: Dict[str, Any],
    pipeline_id: str,
    file_key: str
) -> str:
    """
    Format anomaly detection alert message
    
    Explains statistical deviations with context
    """
    severity = anomaly_report['overall_severity']
    severity_icon = {
        'CRITICAL': '🔴',
        'HIGH': '🟠',
        'MEDIUM': '🟡',
        'LOW': '🟢',
    }.get(severity, '⚪')
    
    lines = [
        "=" * 70,
        f"{severity_icon} ANOMALY DETECTED ({severity} SEVERITY)",
        "=" * 70,
        "",
        f"Pipeline: {pipeline_id}",
        f"File: {file_key}",
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "SUMMARY",
        "-" * 70,
        f"Anomalies Found: {anomaly_report['anomaly_count']}",
        f"Overall Severity: {severity}",
        f"Historical Data Points: {anomaly_report['historical_count']}",
        "",
    ]
    
    # Detailed anomalies
    lines.append("ANOMALY DETAILS")
    lines.append("-" * 70)
    
    for i, anomaly in enumerate(anomaly_report.get('anomalies', []), 1):
        severity_icon = {
            'CRITICAL': '🔴',
            'HIGH': '🟠',
            'MEDIUM': '🟡',
            'LOW': '🟢',
        }.get(anomaly.get('severity', 'NONE'), '⚪')
        
        lines.append(f"{i}. {severity_icon} {anomaly['metric'].upper()} ({anomaly['severity']})")
        lines.append(f"   Detection Method: {anomaly['detection_method']}")
        lines.append(f"   Current Value: {anomaly['current_value']}")
        
        # Add analysis details
        analysis = anomaly.get('analysis', {})
        
        if anomaly['detection_method'] == 'z-score':
            lines.append(f"   Z-Score: {analysis.get('z_score')}")
            lines.append(f"   Historical Mean: {analysis.get('mean')}")
            lines.append(f"   Std Deviation: {analysis.get('std_dev')}")
            lines.append(f"   Threshold: ±{analysis.get('threshold')}")
            lines.append(f"   Confidence: {analysis.get('confidence')}")
        
        elif anomaly['detection_method'] == 'ewma':
            lines.append(f"   Expected (EWMA): {anomaly.get('expected_value')}")
            lines.append(f"   Deviation: {analysis.get('deviation')}")
            lines.append(f"   Threshold: {analysis.get('threshold')}")
        
        lines.append(f"   Description: {anomaly.get('description')}")
        lines.append("")
    
    # Action items
    lines.extend([
        "ACTION REQUIRED",
        "-" * 70,
        "1. Review recent changes to data pipeline or source systems",
        "2. Check for data source issues or outages",
        "3. Verify metric values are correct (not false positive)",
        "4. Investigate if anomaly correlates with known events",
        "5. Consider adjusting anomaly detection thresholds if needed",
        "",
        "INVESTIGATION GUIDE",
        "-" * 70,
        "• Check CloudWatch dashboards for trends",
        "• Review DynamoDB quality metrics table for history",
        "• Examine previous files from same pipeline",
        "• Look for infrastructure changes (deployments, scaling)",
        "• Validate against business metrics/KPIs",
        "",
        "=" * 70,
    ])
    
    return "\n".join(lines)

def send_alert(
    subject: str,
    message: str,
    severity: str = "MEDIUM",
    pipeline_id: str = "unknown"
):
    """
    Send alert via SNS
    
    Routes to appropriate topic based on severity
    
    Args:
        subject: Email subject line
        message: Alert message body
        severity: Alert severity (CRITICAL/HIGH/MEDIUM/LOW)
        pipeline_id: Pipeline identifier
    
    Returns:
        SNS message ID or None if failed
    """
    # Choose topic based on severity
    if severity == 'CRITICAL' and CRITICAL_ALERT_TOPIC_ARN:
        topic_arn = CRITICAL_ALERT_TOPIC_ARN
    else:
        topic_arn = ALERT_TOPIC_ARN
    
    if not topic_arn:
        logger.error("No alert topic ARN configured")
        return None
    
    try:
        # Add severity to subject
        subject_with_severity = f"[{severity}] {subject}"
        
        # Publish to SNS
        response = sns_client.publish(
            TopicArn=topic_arn,
            Subject=subject_with_severity[:100],  # SNS max 100 chars
            Message=message,
            MessageAttributes={
                'severity': {
                    'DataType': 'String',
                    'StringValue': severity
                },
                'pipeline_id': {
                    'DataType': 'String',
                    'StringValue': pipeline_id
                },
                'timestamp': {
                    'DataType': 'String',
                    'StringValue': datetime.utcnow().isoformat()
                },
            }
        )
        
        message_id = response['MessageId']
        logger.info(f"Alert sent: {message_id} (severity: {severity})")
        
        return message_id
        
    except Exception as e:
        logger.error(f"Error sending alert: {e}", exc_info=True)
        return None

def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Alert router Lambda handler
    
    Evaluates workflow results and sends appropriate alerts
    
    Process:
    1. Extract results from all previous steps
    2. Check for schema drift → send alert
    3. Check for quality failures → send alert
    4. Check for anomalies → send alert (if HIGH or CRITICAL)
    5. Return summary of alerts sent
    
    Args:
        event: Lambda event (from Step Functions)
        context: Lambda context
    
    Returns:
        Alert summary with message IDs
    """
    logger.info(f"Alert router event: {json.dumps(event)}")
    
    try:
    
        metadata = event.get('metadata', {})
        
        # Schema validation result
        schema_result = event.get('schemaResult', {}).get('Payload', {})
        validation_result = schema_result.get('validation_result', {})
        
        # Quality validation result
        quality_result = event.get('qualityResult', {}).get('Payload', {})
        quality_report = quality_result.get('quality_report', {})
        
        # Anomaly detection result
        anomaly_result = event.get('anomalyResult', {}).get('Payload', {})
        anomaly_report = anomaly_result.get('anomaly_report', {})
        
        pipeline_id = validation_result.get('pipeline_id', 'unknown')
        file_key = metadata.get('key', 'unknown')
        
        alerts_sent = []

        drift = validation_result.get('drift', {})
        
        if drift.get('has_drift'):
            logger.warning(f"Schema drift detected for {pipeline_id}")
            
            message = format_schema_drift_alert(drift, pipeline_id, file_key)
            
            message_id = send_alert(
                subject=f"Schema Drift Detected - {pipeline_id}",
                message=message,
                severity="HIGH",
                pipeline_id=pipeline_id
            )
            
            if message_id:
                alerts_sent.append({
                    'type': 'schema_drift',
                    'severity': 'HIGH',
                    'message_id': message_id,
                    'pipeline_id': pipeline_id,
                })

        if not quality_report.get('passed'):
            logger.warning(f"Quality checks failed for {pipeline_id}")
            
            # Determine severity based on failed check severities
            severity_counts = quality_report.get('severity_counts', {})
            
            if severity_counts.get('HIGH', 0) > 0:
                alert_severity = 'HIGH'
            elif severity_counts.get('MEDIUM', 0) > 0:
                alert_severity = 'MEDIUM'
            else:
                alert_severity = 'LOW'
            
            message = format_quality_failure_alert(quality_report, pipeline_id, file_key)
            
            message_id = send_alert(
                subject=f"Quality Failure - {pipeline_id}",
                message=message,
                severity=alert_severity,
                pipeline_id=pipeline_id
            )
            
            if message_id:
                alerts_sent.append({
                    'type': 'quality_failure',
                    'severity': alert_severity,
                    'message_id': message_id,
                    'pipeline_id': pipeline_id,
                })

        if anomaly_report.get('has_anomalies'):
            overall_severity = anomaly_report['overall_severity']
            
            # Only alert on HIGH or CRITICAL anomalies
            if overall_severity in ['HIGH', 'CRITICAL']:
                logger.warning(f"Anomaly detected for {pipeline_id} (severity: {overall_severity})")
                
                message = format_anomaly_alert(anomaly_report, pipeline_id, file_key)
                
                message_id = send_alert(
                    subject=f"Anomaly Detected - {pipeline_id}",
                    message=message,
                    severity=overall_severity,
                    pipeline_id=pipeline_id
                )
                
                if message_id:
                    alerts_sent.append({
                        'type': 'anomaly',
                        'severity': overall_severity,
                        'message_id': message_id,
                        'pipeline_id': pipeline_id,
                    })
            else:
                logger.info(f"Anomaly detected but severity {overall_severity} below alert threshold")

        logger.info(f"Alert routing complete: {len(alerts_sent)} alerts sent")
        
        return {
            'statusCode': 200,
            'alerts_sent': alerts_sent,
            'alert_count': len(alerts_sent),
            'pipeline_id': pipeline_id,
        }
        
    except Exception as e:
        logger.error(f"Alert router error: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'error': str(e),
        }