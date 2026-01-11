from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_logs as logs,
    RemovalPolicy,
    Duration,
    CfnOutput,
    aws_events as events,
    aws_events_targets as targets,
    aws_dynamodb as dynamodb,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cw_actions,
)
from constructs import Construct

class DataPulseStack(Stack):
    """
    DataPulse CDK Stack
    Deploys serverless data quality monitoring infrastructure
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        # S3 bucket for data ingestion
        self.data_bucket = s3.Bucket(
            self, "DataIngestionBucket",
            # Unique bucket name using AWS account ID
            bucket_name=f"datapulse-ingestion-{self.account}",
            
            # Enable versioning to track file changes
            versioned=True,
            
            # Server-side encryption for data at rest
            encryption=s3.BucketEncryption.S3_MANAGED,
            
            # Block all public access
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            
            # IMPORTANT: These settings are for DEVELOPMENT ONLY
            # For production, change to RETAIN and remove auto_delete_objects
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            
            # Enable bucket lifecycle management
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="archive-old-data",
                    enabled=False,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90)
                        )
                    ]
                )
            ]
        )
        
        # DynamoDB table for schema baselines and validation history
        self.schema_table = dynamodb.Table(
            self, "SchemaBaselineTable",
            table_name="datapulse-schemas",
            
            # Composite primary key for versioning
            # partition_key: pipeline_id (e.g., "customers", "orders")
            partition_key=dynamodb.Attribute(
                name="pipeline_id",
                type=dynamodb.AttributeType.STRING
            ),
            
            # sort_key: version number (1, 2, 3, ...)
            sort_key=dynamodb.Attribute(
                name="version",
                type=dynamodb.AttributeType.NUMBER
            ),
            
            # On-demand billing (no capacity planning needed)
            # Automatically scales based on traffic
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            
            # Enable point-in-time recovery (backup capability)
            point_in_time_recovery=True,
            
            # Stream configuration for change data capture (optional)
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            
            # Development setting (change to RETAIN for production)
            removal_policy=RemovalPolicy.DESTROY,
            
            # Encryption at rest using AWS-managed keys
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
        )
        
        # Global Secondary Index for querying latest version
        # Allows quick lookup: "Give me the latest schema for pipeline X"
        self.schema_table.add_global_secondary_index(
            index_name="pipeline-latest-index",
            
            # Same partition key
            partition_key=dynamodb.Attribute(
                name="pipeline_id",
                type=dynamodb.AttributeType.STRING
            ),
            
            # Sort by is_latest flag ("true" or "false")
            sort_key=dynamodb.Attribute(
                name="is_latest",
                type=dynamodb.AttributeType.STRING
            ),
            
            # Include all attributes in index
            projection_type=dynamodb.ProjectionType.ALL,
        )
        
        # Add output for table name
        CfnOutput(
            self, "SchemaTableName",
            value=self.schema_table.table_name,
            description="DynamoDB table for schema baselines",
            export_name="DataPulse-SchemaTable"
        )


        # DynamoDB table for quality metrics and check history
        self.quality_table = dynamodb.Table(
            self, "QualityMetricsTable",
            table_name="datapulse-quality-metrics",
            
            # Composite key: pipeline_id + timestamp
            partition_key=dynamodb.Attribute(
                name="pipeline_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            ),
            
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            
            # TTL for automatic cleanup of old metrics (90 days)
            time_to_live_attribute="ttl",
            
            # Stream for change data capture
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            
            removal_policy=RemovalPolicy.DESTROY,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
        )
        
        # Add GSI for querying by quality score
        self.quality_table.add_global_secondary_index(
            index_name="quality-score-index",
            partition_key=dynamodb.Attribute(
                name="pipeline_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="quality_score",
                type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )
        
        CfnOutput(
            self, "QualityTableName",
            value=self.quality_table.table_name,
            description="DynamoDB table for quality metrics",
        )

        # Main alert topic for all notifications
        self.alert_topic = sns.Topic(
            self, "AlertTopic",
            topic_name="datapulse-alerts",
            display_name="DataPulse Quality Alerts",
            
            # Enable FIFO for ordered delivery
            # fifo=True,
        )
        
        self.alert_topic.add_subscription(
            subs.EmailSubscription("arnavd543@gmail.com")
        )
        
        # Optional: Separate topic for high-severity alerts
        self.critical_alert_topic = sns.Topic(
            self, "CriticalAlertTopic",
            topic_name="datapulse-critical-alerts",
            display_name="DataPulse CRITICAL Alerts",
        )
        
        # Can also add SMS for critical alerts
        # self.critical_alert_topic.add_subscription(
        #     subs.SmsSubscription("+1234567890")
        # )
        
        CfnOutput(
            self, "AlertTopicArn",
            value=self.alert_topic.topic_arn,
            description="SNS topic ARN for alerts",
        )

        # Lambda function for schema validation
        self.schema_validator = lambda_.Function(
            self, "SchemaValidator",
            function_name="datapulse-schema-validator",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="schema_validator.handler",
            code=lambda_.Code.from_asset("lambda/schema_validator"),
            
            # Longer timeout for large file processing
            timeout=Duration.seconds(120),
            
            # More memory for CSV parsing and type inference
            memory_size=1024,
            
            # Environment variables
            environment={
                "SCHEMA_TABLE_NAME": self.schema_table.table_name,
                "SCHEMA_SAMPLE_SIZE": "1000",
                "MAX_FILE_SIZE_MB": "10",
                "LOG_LEVEL": "INFO",
            },
            
            log_retention=logs.RetentionDays.ONE_WEEK,
            tracing=lambda_.Tracing.ACTIVE,
        )
        
        # Grant permissions
        self.data_bucket.grant_read(self.schema_validator)
        self.schema_table.grant_read_write_data(self.schema_validator)
        
        # Output Lambda ARN
        CfnOutput(
            self, "SchemaValidatorArn",
            value=self.schema_validator.function_arn,
            description="Schema validator Lambda ARN",
        )

        self.quality_validator = lambda_.Function(
            self, "QualityValidator",
            function_name="datapulse-quality-validator",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="quality_validator.handler",
            code=lambda_.Code.from_asset("lambda/quality_validator"),
            timeout=Duration.seconds(120),
            memory_size=1024,
            environment={
                "QUALITY_TABLE_NAME": self.quality_table.table_name,
                "NULL_THRESHOLD": "10",
                "DUPLICATE_THRESHOLD": "5",
                "RANGE_STDDEV_MULTIPLIER": "3",
                "MAX_FILE_SIZE_MB": "10",
                "LOG_LEVEL": "INFO",
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
            tracing=lambda_.Tracing.ACTIVE,
        )
        
        # Grant permissions
        self.data_bucket.grant_read(self.quality_validator)
        self.quality_table.grant_read_write_data(self.quality_validator)
        
        # Grant CloudWatch metrics permission
        self.quality_validator.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["cloudwatch:PutMetricData"],
                resources=["*"]
            )
        )
        
        CfnOutput(
            self, "QualityValidatorArn",
            value=self.quality_validator.function_arn,
        )

        # ========================================
        # PHASE 5: ANOMALY DETECTOR LAMBDA
        # ========================================
        # NOTE: Must be defined BEFORE Step Functions workflow

        self.anomaly_detector = lambda_.Function(
            self, "AnomalyDetector",
            function_name="datapulse-anomaly-detector",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="anomaly_detector.handler",
            code=lambda_.Code.from_asset("lambda/anomaly_detector"),
            timeout=Duration.seconds(60),
            memory_size=512,
            environment={
                "QUALITY_TABLE_NAME": self.quality_table.table_name,
                "Z_SCORE_THRESHOLD": "3",
                "EWMA_ALPHA": "0.2",
                "LOOKBACK_DAYS": "7",
                "MIN_HISTORICAL_POINTS": "3",
                "LOG_LEVEL": "INFO",
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
            tracing=lambda_.Tracing.ACTIVE,
        )

        # Grant permissions
        self.quality_table.grant_read_data(self.anomaly_detector)

        self.anomaly_detector.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["cloudwatch:PutMetricData"],
                resources=["*"]
            )
        )

        # ========================================
        # PHASE 6: ALERT ROUTER LAMBDA
        # ========================================
        # NOTE: Must be defined BEFORE Step Functions workflow

        self.alert_router = lambda_.Function(
            self, "AlertRouter",
            function_name="datapulse-alert-router",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="alert_router.handler",
            code=lambda_.Code.from_asset("lambda/alert_router"),
            timeout=Duration.seconds(30),
            memory_size=256,
            environment={
                "ALERT_TOPIC_ARN": self.alert_topic.topic_arn,
                "CRITICAL_ALERT_TOPIC_ARN": self.critical_alert_topic.topic_arn if hasattr(self, 'critical_alert_topic') else "",
                "LOG_LEVEL": "INFO",
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        # Grant SNS publish permissions
        self.alert_topic.grant_publish(self.alert_router)
        if hasattr(self, 'critical_alert_topic'):
            self.critical_alert_topic.grant_publish(self.alert_router)

        # ========================================
        # PHASE 4: STEP FUNCTIONS WORKFLOW
        # ========================================
        # NOTE: This must be defined BEFORE event_processor so we can reference the ARN

        # Fail state for schema validation errors
        schema_fail = sfn.Fail(
            self, "SchemaValidationFailed",
            cause="Schema validation failed",
            error="SchemaValidationError"
        )

        schema_validation_task = tasks.LambdaInvoke(
            self, "SchemaValidationTask",
            lambda_function=self.schema_validator,

            # Input from workflow execution
            payload=sfn.TaskInput.from_object({
                "metadata.$": "$.metadata",
            }),

            # Store result in workflow state
            result_path="$.schemaResult",

            # Output selector (extract Payload from Lambda response)
            output_path="$",

            # Retry configuration
            retry_on_service_exceptions=True,
        )

        # Add error handling
        schema_validation_task.add_retry(
            errors=['States.TaskFailed'],
            interval=Duration.seconds(2),
            max_attempts=3,
            backoff_rate=2.0
        )

        schema_validation_task.add_catch(
            schema_fail,
            errors=['States.ALL'],
            result_path="$.schemaError"
        )

        # Fail state for quality validation errors
        quality_fail = sfn.Fail(
            self, "QualityValidationFailed",
            cause="Quality validation failed",
            error="QualityValidationError"
        )

        quality_validation_task = tasks.LambdaInvoke(
            self, "QualityValidationTask",
            lambda_function=self.quality_validator,

            # Pass both metadata and schema result
            payload=sfn.TaskInput.from_object({
                "metadata.$": "$.metadata",
                "validation_result.$": "$.schemaResult.Payload.validation_result",
            }),

            result_path="$.qualityResult",
            output_path="$",

            retry_on_service_exceptions=True,
        )

        # Add error handling
        quality_validation_task.add_retry(
            errors=['States.TaskFailed'],
            interval=Duration.seconds(2),
            max_attempts=3,
            backoff_rate=2.0
        )

        quality_validation_task.add_catch(
            quality_fail,
            errors=['States.ALL'],
            result_path="$.qualityError"
        )

        anomaly_detection_task = tasks.LambdaInvoke(
            self, "AnomalyDetectionTask",
            lambda_function=self.anomaly_detector,
            payload=sfn.TaskInput.from_object({
                "metadata.$": "$.metadata",
                "schemaResult.$": "$.schemaResult",
                "qualityResult.$": "$.qualityResult",
            }),
            result_path="$.anomalyResult",
            retry_on_service_exceptions=True,
        )

        alert_routing_task = tasks.LambdaInvoke(
            self, "AlertRoutingTask",
            lambda_function=self.alert_router,
            payload=sfn.TaskInput.from_object({
                "metadata.$": "$.metadata",
                "schemaResult.$": "$.schemaResult",
                "qualityResult.$": "$.qualityResult",
                "anomalyResult.$": "$.anomalyResult",
            }),
            result_path="$.alertResult",
        )

        quality_check = sfn.Choice(
            self, "QualityCheckPassed",
            comment="Branch based on quality check results"
        )

        # Success path
        quality_passed = sfn.Succeed(
            self, "ValidationSucceeded",
            comment="All validation checks passed"
        )

        # Failure path (doesn't stop workflow, just marks status)
        quality_failed = sfn.Pass(
            self, "QualityFailed",
            comment="Quality checks failed but workflow continues",
            result=sfn.Result.from_object({
                "status": "QUALITY_FAILED",
                "message": "Data quality checks failed, alerts will be sent"
            }),
            result_path="$.workflowStatus"
        )

        # Continue to success after logging failure
        quality_failed.next(quality_passed)

        # Chain tasks: Schema → Quality → Check → Success/Failure
                # Update workflow chain
        definition = schema_validation_task \
            .next(quality_validation_task) \
            .next(anomaly_detection_task) \
            .next(alert_routing_task) \
            .next(quality_check
                .when(
                    sfn.Condition.boolean_equals("$.qualityResult.Payload.quality_report.passed", True),
                    quality_passed
                )
                .otherwise(quality_failed)
            )

        self.validation_workflow = sfn.StateMachine(
            self, "ValidationWorkflow",
            state_machine_name="datapulse-validation-workflow",

            # Workflow definition
            definition=definition,

            # Timeout (prevent infinite execution)
            timeout=Duration.minutes(5),

            # Enable X-Ray tracing
            tracing_enabled=True,

            # Logging configuration
            logs=sfn.LogOptions(
                destination=logs.LogGroup(
                    self, "WorkflowLogGroup",
                    log_group_name="/aws/vendedlogs/states/datapulse-workflow",
                    retention=logs.RetentionDays.ONE_WEEK,
                    removal_policy=RemovalPolicy.DESTROY
                ),
                level=sfn.LogLevel.ALL,
                include_execution_data=True
            ),
        )

        CfnOutput(
            self, "WorkflowArn",
            value=self.validation_workflow.state_machine_arn,
            description="Step Functions workflow ARN",
            export_name="DataPulse-WorkflowArn"
        )

        # Lambda function to process S3 events
        self.event_processor = lambda_.Function(
            self, "EventProcessor",
            function_name="datapulse-event-processor",
            
            runtime=lambda_.Runtime.PYTHON_3_11,
            
            handler="event_processor.handler",
            
            code=lambda_.Code.from_asset("lambda/event_processor"),
            
            timeout=Duration.seconds(60),
            
            memory_size=512,
            
            environment={
                "DATA_BUCKET": self.data_bucket.bucket_name,
                "STATE_MACHINE_ARN": self.validation_workflow.state_machine_arn,
                "LOG_LEVEL": "INFO",
                "POWERTOOLS_SERVICE_NAME": "datapulse-event-processor",
            },
            
            log_retention=logs.RetentionDays.ONE_WEEK,
            
            tracing=lambda_.Tracing.ACTIVE,
            
            # reserved_concurrent_executions=10,
        )
        
        self.validation_workflow.grant_start_execution(self.event_processor)

        # EventBridge rule to capture S3 object creation events
        s3_rule = events.Rule(
            self, "S3ObjectCreatedRule",
            rule_name="datapulse-s3-object-created",
            
            # Description for AWS Console
            description="Routes S3 object creation events to DataPulse event processor",
            
            # Event pattern matching
            event_pattern=events.EventPattern(
                # Source: AWS S3 service
                source=["aws.s3"],
                
                # Detail type: Object Created events (PUT, POST, COPY, CompleteMultipartUpload)
                detail_type=["Object Created"],
                
                # Filter events
                detail={
                    # Only from our specific bucket
                    "bucket": {
                        "name": [self.data_bucket.bucket_name]
                    },
                    
                    # Only files in 'raw/' prefix (data ingestion folder)
                    # Add more prefixes as needed: ["raw/", "staging/"]
                    "object": {
                        "key": [
                            {"prefix": "raw/"}
                        ]
                    }
                }
            ),
            
            # Enable rule immediately
            enabled=True,
        )
        
        # Add Lambda as target for matching events
        s3_rule.add_target(
            targets.LambdaFunction(
                self.event_processor,
                
                # Retry configuration for failed invocations
                retry_attempts=2,
                
                # Maximum event age (24 hours)
                max_event_age=Duration.hours(24),
                
                # Dead letter queue for failed events (optional)
                # dead_letter_queue=some_sqs_queue,
            )
        )
        
        # Enable EventBridge notifications on S3 bucket
        # This is REQUIRED for S3 to send events to EventBridge
        self.data_bucket.enable_event_bridge_notification()

        
        # Output EventBridge rule ARN
        CfnOutput(
            self, "EventBridgeRuleArn",
            value=s3_rule.rule_arn,
            description="EventBridge rule ARN for S3 events",
        )
        
        self.data_bucket.grant_read(self.event_processor)
        
        # Output bucket name for easy reference
        CfnOutput(
            self, "DataBucketName",
            value=self.data_bucket.bucket_name,
            description="S3 bucket for data ingestion",
            export_name="DataPulse-DataBucket"
        )
        

        # Output Lambda ARN
        CfnOutput(
            self, "EventProcessorArn",
            value=self.event_processor.function_arn,
            description="Event processor Lambda ARN",
        )

        dashboard = cloudwatch.Dashboard(
            self, "DataPulseDashboard",
            dashboard_name="DataPulse-Overview",
        )

        # Quality score widget
        quality_score_widget = cloudwatch.GraphWidget(
            title="Quality Score by Pipeline (Last 24h)",
            left=[
                cloudwatch.Metric(
                    namespace="DataPulse",
                    metric_name="QualityScore",
                    dimensions_map={"PipelineId": "customers"},
                    statistic="Average",
                    label="Customers",
                    color=cloudwatch.Color.BLUE,
                ),
                cloudwatch.Metric(
                    namespace="DataPulse",
                    metric_name="QualityScore",
                    dimensions_map={"PipelineId": "orders"},
                    statistic="Average",
                    label="Orders",
                    color=cloudwatch.Color.GREEN,
                ),
            ],
            width=12,
            height=6,
            left_y_axis=cloudwatch.YAxisProps(
                label="Quality Score (%)",
                min=0,
                max=100,
            ),
        )
        
        # Failed checks widget
        failed_checks_widget = cloudwatch.GraphWidget(
            title="Failed Quality Checks (Last 24h)",
            left=[
                cloudwatch.Metric(
                    namespace="DataPulse",
                    metric_name="FailedChecks",
                    statistic="Sum",
                    label="Total Failed Checks",
                    color=cloudwatch.Color.RED,
                ),
                cloudwatch.Metric(
                    namespace="DataPulse",
                    metric_name="PassedChecks",
                    statistic="Sum",
                    label="Total Passed Checks",
                    color=cloudwatch.Color.GREEN,
                ),
            ],
            width=12,
            height=6,
            stacked=False,
        )

        # Anomaly count widget
        anomaly_widget = cloudwatch.GraphWidget(
            title="Anomalies Detected (Last 7 days)",
            left=[
                cloudwatch.Metric(
                    namespace="DataPulse",
                    metric_name="AnomalyCount",
                    statistic="Sum",
                    label="Total Anomalies",
                    color=cloudwatch.Color.ORANGE,
                ),
                cloudwatch.Metric(
                    namespace="DataPulse",
                    metric_name="HasAnomalies",
                    statistic="Sum",
                    label="Files with Anomalies",
                    color=cloudwatch.Color.RED,
                ),
            ],
            width=12,
            height=6,
        )
        
        # Anomaly severity breakdown (single value widgets)
        anomaly_severity_widget = cloudwatch.Row(
            cloudwatch.SingleValueWidget(
                title="Critical Anomalies",
                metrics=[
                    cloudwatch.Metric(
                        namespace="DataPulse",
                        metric_name="AnomalyCount",
                        dimensions_map={"Severity": "CRITICAL"},
                        statistic="Sum",
                    )
                ],
                width=6,
                height=3,
            ),
            cloudwatch.SingleValueWidget(
                title="High Anomalies",
                metrics=[
                    cloudwatch.Metric(
                        namespace="DataPulse",
                        metric_name="AnomalyCount",
                        dimensions_map={"Severity": "HIGH"},
                        statistic="Sum",
                    )
                ],
                width=6,
                height=3,
            ),
            cloudwatch.SingleValueWidget(
                title="Medium Anomalies",
                metrics=[
                    cloudwatch.Metric(
                        namespace="DataPulse",
                        metric_name="AnomalyCount",
                        dimensions_map={"Severity": "MEDIUM"},
                        statistic="Sum",
                    )
                ],
                width=6,
                height=3,
            ),
            cloudwatch.SingleValueWidget(
                title="Low Anomalies",
                metrics=[
                    cloudwatch.Metric(
                        namespace="DataPulse",
                        metric_name="AnomalyCount",
                        dimensions_map={"Severity": "LOW"},
                        statistic="Sum",
                    )
                ],
                width=6,
                height=3,
            ),
        )

        # Lambda duration widget
        lambda_duration_widget = cloudwatch.GraphWidget(
            title="Lambda Execution Duration (ms)",
            left=[
                self.schema_validator.metric_duration(
                    label="Schema Validator",
                    statistic="Average",
                    color=cloudwatch.Color.BLUE,
                ),
                self.quality_validator.metric_duration(
                    label="Quality Validator",
                    statistic="Average",
                    color=cloudwatch.Color.GREEN,
                ),
                self.anomaly_detector.metric_duration(
                    label="Anomaly Detector",
                    statistic="Average",
                    color=cloudwatch.Color.ORANGE,
                ),
                self.alert_router.metric_duration(
                    label="Alert Router",
                    statistic="Average",
                    color=cloudwatch.Color.PURPLE,
                ),
            ],
            width=12,
            height=6,
        )
        
        # Lambda errors widget
        lambda_errors_widget = cloudwatch.GraphWidget(
            title="Lambda Errors",
            left=[
                self.schema_validator.metric_errors(
                    label="Schema Errors",
                    color=cloudwatch.Color.RED,
                ),
                self.quality_validator.metric_errors(
                    label="Quality Errors",
                    color=cloudwatch.Color.ORANGE,
                ),
                self.anomaly_detector.metric_errors(
                    label="Anomaly Errors",
                    color=cloudwatch.Color.PURPLE,
                ),
            ],
            width=12,
            height=6,
        )

        # Workflow executions widget
        workflow_executions_widget = cloudwatch.GraphWidget(
            title="Workflow Executions",
            left=[
                self.validation_workflow.metric_started(
                    label="Started",
                    color=cloudwatch.Color.BLUE,
                ),
                self.validation_workflow.metric_succeeded(
                    label="Succeeded",
                    color=cloudwatch.Color.GREEN,
                ),
                self.validation_workflow.metric_failed(
                    label="Failed",
                    color=cloudwatch.Color.RED,
                ),
                self.validation_workflow.metric_timed_out(
                    label="Timed Out",
                    color=cloudwatch.Color.ORANGE,
                ),
            ],
            width=12,
            height=6,
        )
        
        # Workflow duration widget
        workflow_duration_widget = cloudwatch.GraphWidget(
            title="Workflow Duration (seconds)",
            left=[
                cloudwatch.Metric(
                    namespace="AWS/States",
                    metric_name="ExecutionTime",
                    dimensions_map={
                        "StateMachineArn": self.validation_workflow.state_machine_arn
                    },
                    statistic="Average",
                    label="Average Duration",
                ),
            ],
            width=12,
            height=6,
        )

        # Lambda invocations (for cost tracking)
        lambda_invocations_widget = cloudwatch.GraphWidget(
            title="Lambda Invocations (Cost Indicator)",
            left=[
                self.schema_validator.metric_invocations(label="Schema Validator"),
                self.quality_validator.metric_invocations(label="Quality Validator"),
                self.anomaly_detector.metric_invocations(label="Anomaly Detector"),
                self.alert_router.metric_invocations(label="Alert Router"),
            ],
            width=12,
            height=6,
        )
        
        # DynamoDB operations (for cost tracking)
        dynamodb_operations_widget = cloudwatch.GraphWidget(
            title="DynamoDB Operations (Cost Indicator)",
            left=[
                cloudwatch.Metric(
                    namespace="AWS/DynamoDB",
                    metric_name="ConsumedReadCapacityUnits",
                    dimensions_map={"TableName": self.schema_table.table_name},
                    statistic="Sum",
                    label="Schema Table Reads",
                ),
                cloudwatch.Metric(
                    namespace="AWS/DynamoDB",
                    metric_name="ConsumedWriteCapacityUnits",
                    dimensions_map={"TableName": self.schema_table.table_name},
                    statistic="Sum",
                    label="Schema Table Writes",
                ),
                cloudwatch.Metric(
                    namespace="AWS/DynamoDB",
                    metric_name="ConsumedReadCapacityUnits",
                    dimensions_map={"TableName": self.quality_table.table_name},
                    statistic="Sum",
                    label="Quality Table Reads",
                ),
                cloudwatch.Metric(
                    namespace="AWS/DynamoDB",
                    metric_name="ConsumedWriteCapacityUnits",
                    dimensions_map={"TableName": self.quality_table.table_name},
                    statistic="Sum",
                    label="Quality Table Writes",
                ),
            ],
            width=12,
            height=6,
        )

        # Row 1: Quality metrics
        dashboard.add_widgets(quality_score_widget, failed_checks_widget)
        
        # Row 2: Anomaly detection
        dashboard.add_widgets(anomaly_widget)
        dashboard.add_widgets(anomaly_severity_widget)
        
        # Row 3: Lambda performance
        dashboard.add_widgets(lambda_duration_widget, lambda_errors_widget)
        
        # Row 4: Workflow metrics
        dashboard.add_widgets(workflow_executions_widget, workflow_duration_widget)
        
        # Row 5: Cost tracking
        dashboard.add_widgets(lambda_invocations_widget, dynamodb_operations_widget)

        # Alarm: Low quality score
        low_quality_alarm = cloudwatch.Alarm(
            self, "LowQualityScoreAlarm",
            alarm_name="datapulse-low-quality-score",
            alarm_description="Alert when quality score drops below 70%",
            metric=cloudwatch.Metric(
                namespace="DataPulse",
                metric_name="QualityScore",
                statistic="Average",
                period=Duration.minutes(5),
            ),
            threshold=70,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            evaluation_periods=2,
            datapoints_to_alarm=2,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        
        low_quality_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
        
        # Alarm: High anomaly count
        anomaly_alarm = cloudwatch.Alarm(
            self, "HighAnomalyCountAlarm",
            alarm_name="datapulse-high-anomaly-count",
            alarm_description="Alert when 3+ anomalies detected in 15 minutes",
            metric=cloudwatch.Metric(
                namespace="DataPulse",
                metric_name="AnomalyCount",
                statistic="Sum",
                period=Duration.minutes(15),
            ),
            threshold=3,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            evaluation_periods=1,
        )
        
        anomaly_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
        
        # Alarm: Lambda errors
        lambda_error_alarm = cloudwatch.Alarm(
            self, "LambdaErrorAlarm",
            alarm_name="datapulse-lambda-errors",
            alarm_description="Alert on any Lambda errors",
            metric=cloudwatch.Metric(
                namespace="AWS/Lambda",
                metric_name="Errors",
                dimensions_map={"FunctionName": self.quality_validator.function_name},
                statistic="Sum",
                period=Duration.minutes(5),
            ),
            threshold=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            evaluation_periods=1,
        )
        
        lambda_error_alarm.add_alarm_action(cw_actions.SnsAction(self.alert_topic))
        
        # Alarm: Workflow failures
        workflow_failure_alarm = cloudwatch.Alarm(
            self, "WorkflowFailureAlarm",
            alarm_name="datapulse-workflow-failures",
            alarm_description="Alert on Step Functions workflow failures",
            metric=self.validation_workflow.metric_failed(
                period=Duration.minutes(5),
            ),
            threshold=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            evaluation_periods=1,
        )
        
        workflow_failure_alarm.add_alarm_action(cw_actions.SnsAction(self.critical_alert_topic if hasattr(self, 'critical_alert_topic') else self.alert_topic))

        CfnOutput(
            self, "DashboardUrl",
            value=f"https://console.aws.amazon.com/cloudwatch/home?region={self.region}#dashboards:name=DataPulse-Overview",
            description="CloudWatch Dashboard URL",
        )