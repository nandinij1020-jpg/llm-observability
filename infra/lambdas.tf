# Lambda code is staged into ../build by `make build` (scripts/build_lambdas.sh)
data "archive_file" "processor" {
  type        = "zip"
  source_dir  = "${local.build_dir}/processor"
  output_path = "${local.build_dir}/processor.zip"
}

data "archive_file" "canary" {
  type        = "zip"
  source_dir  = "${local.build_dir}/canary"
  output_path = "${local.build_dir}/canary.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# =========================================================== Processor
resource "aws_iam_role" "processor" {
  name               = "${local.name}-processor"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "processor_basic" {
  role       = aws_iam_role.processor.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "processor_xray" {
  role       = aws_iam_role.processor.name
  policy_arn = "arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess"
}

resource "aws_iam_role_policy" "processor" {
  name = "processor-access"
  role = aws_iam_role.processor.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
        "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:ListStreams"]
        Resource = aws_kinesis_stream.traces.arn
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:UpdateItem", "dynamodb:GetItem", "dynamodb:PutItem"]
        Resource = aws_dynamodb_table.usage.arn
      },
      {
        Effect    = "Allow"
        Action    = "cloudwatch:PutMetricData"
        Resource  = "*"
        Condition = { StringEquals = { "cloudwatch:namespace" = local.namespace } }
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      },
      {
        Effect   = "Allow"
        Action   = ["es:ESHttpPost", "es:ESHttpPut", "es:ESHttpGet"]
        Resource = "${aws_opensearch_domain.traces.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.processor_dlq.arn
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "processor" {
  name              = "/aws/lambda/${local.name}-processor"
  retention_in_days = var.log_retention_days
}

resource "aws_sqs_queue" "processor_dlq" {
  name                      = "${local.name}-processor-dlq"
  message_retention_seconds = 1209600
}

resource "aws_lambda_function" "processor" {
  function_name    = "${local.name}-processor"
  role             = aws_iam_role.processor.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.processor.output_path
  source_code_hash = data.archive_file.processor.output_base64sha256
  timeout          = 60
  memory_size      = 512
  architectures    = ["arm64"]

  tracing_config {
    mode = "Active" # AWS X-Ray
  }

  environment {
    variables = {
      ENVIRONMENT             = var.environment
      METRIC_NAMESPACE        = local.namespace
      USAGE_TABLE             = aws_dynamodb_table.usage.name
      OPENSEARCH_ENDPOINT     = aws_opensearch_domain.traces.endpoint
      OPENSEARCH_INDEX_PREFIX = "llm-traces"
      ALERT_TOPIC_ARN         = aws_sns_topic.alerts.arn
      ANOMALY_Z_THRESHOLD     = tostring(var.anomaly_z_threshold)
    }
  }

  depends_on = [aws_cloudwatch_log_group.processor, aws_iam_role_policy.processor]
}

resource "aws_lambda_event_source_mapping" "kinesis" {
  event_source_arn                   = aws_kinesis_stream.traces.arn
  function_name                      = aws_lambda_function.processor.arn
  starting_position                  = "LATEST"
  batch_size                         = 200
  maximum_batching_window_in_seconds = 1
  parallelization_factor             = 1 # keeps per-shard ordering for the EWMA detector
  maximum_retry_attempts             = 5
  bisect_batch_on_function_error     = true
  maximum_record_age_in_seconds      = 3600

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.processor_dlq.arn
    }
  }
}

# =========================================================== Canary
resource "aws_iam_role" "canary" {
  name               = "${local.name}-canary"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "canary_basic" {
  role       = aws_iam_role.canary.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "canary_xray" {
  role       = aws_iam_role.canary.name
  policy_arn = "arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess"
}

resource "aws_iam_role_policy" "canary" {
  name = "canary-access"
  role = aws_iam_role.canary.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["kinesis:PutRecords", "kinesis:PutRecord"], Resource = aws_kinesis_stream.traces.arn },
      { Effect = "Allow", Action = ["dynamodb:GetItem"], Resource = aws_dynamodb_table.usage.arn },
      { Effect = "Allow", Action = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"], Resource = "*" },
      {
        Effect    = "Allow"
        Action    = "cloudwatch:PutMetricData"
        Resource  = "*"
        Condition = { StringEquals = { "cloudwatch:namespace" = local.namespace } }
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "canary" {
  name              = "/aws/lambda/${local.name}-canary"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "canary" {
  function_name    = "${local.name}-canary"
  role             = aws_iam_role.canary.arn
  runtime          = "python3.12"
  handler          = "handler.lambda_handler"
  filename         = data.archive_file.canary.output_path
  source_code_hash = data.archive_file.canary.output_base64sha256
  timeout          = 90
  memory_size      = 256
  architectures    = ["arm64"]

  tracing_config {
    mode = "Active"
  }

  environment {
    variables = {
      ENVIRONMENT         = var.environment
      METRIC_NAMESPACE    = local.namespace
      TRACE_STREAM_NAME   = aws_kinesis_stream.traces.name
      USAGE_TABLE         = aws_dynamodb_table.usage.name
      CANARY_MODEL_ID     = var.canary_model_id
      CANARY_MOCK_BEDROCK = tostring(var.canary_mock_bedrock)
    }
  }

  depends_on = [aws_cloudwatch_log_group.canary, aws_iam_role_policy.canary]
}

resource "aws_cloudwatch_event_rule" "canary" {
  name                = "${local.name}-canary"
  schedule_expression = var.canary_schedule
  state               = var.canary_enabled ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "canary" {
  rule = aws_cloudwatch_event_rule.canary.name
  arn  = aws_lambda_function.canary.arn
}

resource "aws_lambda_permission" "canary_events" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.canary.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.canary.arn
}

# =========================================================== Producer policy
# Attach this managed policy to whatever runs YOUR agents (EC2 role, ECS task
# role, another Lambda, or the IAM user running scripts/demo.py).
resource "aws_iam_policy" "producer" {
  name        = "${local.name}-trace-producer"
  description = "Lets an instrumented agent call Bedrock and ship spans to Kinesis"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["kinesis:PutRecords", "kinesis:PutRecord"], Resource = aws_kinesis_stream.traces.arn },
      { Effect = "Allow", Action = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"], Resource = "*" }
    ]
  })
}
