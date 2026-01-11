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
        definition = schema_validation_task \
            .next(quality_validation_task) \
            .next(quality_check
                .when(
                    sfn.Condition.boolean_equals(
                        "$.qualityResult.Payload.quality_report.passed",
                        True
                    ),
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