# ---------------------------------------------------------------- Kinesis
resource "aws_kinesis_stream" "traces" {
  name             = "${local.name}-traces"
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours
  encryption_type  = "KMS"
  kms_key_id       = "alias/aws/kinesis"

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

# ---------------------------------------------------------------- DynamoDB
# Single-table design:
#   pk = "user#<id>" | "agent#<id>" | "session#<id>" | "model#<id>" | "anomaly#<agent>#<metric>"
#   sk = "day#YYYY-MM-DD" | "total" | "ewma"
resource "aws_dynamodb_table" "usage" {
  name         = "${local.name}-usage"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = var.environment == "prod"
  }
  server_side_encryption {
    enabled = true
  }
}

# ---------------------------------------------------------------- OpenSearch
resource "aws_opensearch_domain" "traces" {
  domain_name    = "${var.project}-${var.environment}" # max 28 chars
  engine_version = var.opensearch_engine_version

  cluster_config {
    instance_type          = var.opensearch_instance_type
    instance_count         = var.opensearch_instance_count
    zone_awareness_enabled = var.opensearch_instance_count > 1
  }

  ebs_options {
    ebs_enabled = true
    volume_type = "gp3"
    volume_size = var.opensearch_volume_gb
  }

  encrypt_at_rest {
    enabled = var.opensearch_encrypt_at_rest
  }
  node_to_node_encryption {
    enabled = true
  }
  domain_endpoint_options {
    enforce_https       = true
    tls_security_policy = "Policy-Min-TLS-1-2-2019-07"
  }
}

data "aws_iam_policy_document" "opensearch_access" {
  # The processor Lambda (SigV4-signed requests) can read and write
  statement {
    sid       = "ProcessorWrite"
    actions   = ["es:ESHttp*"]
    resources = ["${aws_opensearch_domain.traces.arn}/*"]
    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.processor.arn]
    }
  }

  # Browser access to OpenSearch Dashboards from your own IP only
  dynamic "statement" {
    for_each = length(var.dashboard_allowed_cidrs) > 0 ? [1] : []
    content {
      sid       = "DashboardsFromAllowedIPs"
      actions   = ["es:ESHttpGet", "es:ESHttpPost", "es:ESHttpPut", "es:ESHttpHead", "es:ESHttpDelete"]
      resources = ["${aws_opensearch_domain.traces.arn}/*"]
      principals {
        type        = "AWS"
        identifiers = ["*"]
      }
      condition {
        test     = "IpAddress"
        variable = "aws:SourceIp"
        values   = var.dashboard_allowed_cidrs
      }
    }
  }
}

resource "aws_opensearch_domain_policy" "traces" {
  domain_name     = aws_opensearch_domain.traces.domain_name
  access_policies = data.aws_iam_policy_document.opensearch_access.json
}

# ---------------------------------------------------------------- SNS
# Note: no KMS here on purpose — CloudWatch alarms cannot publish to a topic
# encrypted with the AWS-managed SNS key (it would need a customer-managed key).
resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowCloudWatchAlarms"
      Effect    = "Allow"
      Principal = { Service = "cloudwatch.amazonaws.com" }
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.alerts.arn
      Condition = { StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id } }
    }]
  })
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}
