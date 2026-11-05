# All alarms notify the same SNS topic (OK transitions too, so you see recovery).
locals {
  alarm_actions = [aws_sns_topic.alerts.arn]
  all_agents    = { AgentId = "ALL" }
}

# ------------------------------------------------------------ static thresholds
resource "aws_cloudwatch_metric_alarm" "error_rate" {
  alarm_name          = "${local.name}-agent-error-rate"
  alarm_description   = "Agent run error rate above ${var.error_rate_threshold_pct}% (3 of 5 minutes)"
  comparison_operator = "GreaterThanThreshold"
  threshold           = var.error_rate_threshold_pct
  evaluation_periods  = 5
  datapoints_to_alarm = 3
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions

  metric_query {
    id          = "rate"
    expression  = "IF(req > 0, 100 * err / req, 0)"
    label       = "Error rate %"
    return_data = true
  }
  metric_query {
    id = "err"
    metric {
      namespace   = local.namespace
      metric_name = "Errors"
      dimensions  = local.all_agents
      period      = 60
      stat        = "Sum"
    }
  }
  metric_query {
    id = "req"
    metric {
      namespace   = local.namespace
      metric_name = "Requests"
      dimensions  = local.all_agents
      period      = 60
      stat        = "Sum"
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "p90_latency" {
  alarm_name          = "${local.name}-agent-p90-latency"
  alarm_description   = "p90 agent run latency above ${var.p90_latency_threshold_ms} ms"
  namespace           = local.namespace
  metric_name         = "AgentRunLatencyMs"
  dimensions          = local.all_agents
  extended_statistic  = "p90"
  period              = 300
  evaluation_periods  = 2
  threshold           = var.p90_latency_threshold_ms
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "hourly_cost" {
  alarm_name          = "${local.name}-hourly-cost"
  alarm_description   = "LLM spend above USD ${var.hourly_cost_threshold_usd} in one hour"
  namespace           = local.namespace
  metric_name         = "CostUSD"
  dimensions          = local.all_agents
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = var.hourly_cost_threshold_usd
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

# ------------------------------------------------- ML anomaly detection bands
# CloudWatch trains a model on the metric's history (handles daily/weekly
# seasonality). Needs some history before the band becomes meaningful.
resource "aws_cloudwatch_metric_alarm" "latency_anomaly" {
  alarm_name          = "${local.name}-latency-anomaly"
  alarm_description   = "p90 agent latency outside the expected band (CloudWatch anomaly detection)"
  comparison_operator = "GreaterThanUpperThreshold"
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  threshold_metric_id = "band"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  metric_query {
    id          = "band"
    expression  = "ANOMALY_DETECTION_BAND(m1, ${var.anomaly_band_width})"
    label       = "Latency expected range"
    return_data = true
  }
  metric_query {
    id          = "m1"
    return_data = true
    metric {
      namespace   = local.namespace
      metric_name = "AgentRunLatencyMs"
      dimensions  = local.all_agents
      period      = 300
      stat        = "p90"
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "cost_anomaly" {
  alarm_name          = "${local.name}-cost-anomaly"
  alarm_description   = "Spend per 5 minutes outside the expected band (CloudWatch anomaly detection)"
  comparison_operator = "GreaterThanUpperThreshold"
  evaluation_periods  = 3
  datapoints_to_alarm = 2
  threshold_metric_id = "band"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions

  metric_query {
    id          = "band"
    expression  = "ANOMALY_DETECTION_BAND(m1, ${var.anomaly_band_width})"
    label       = "Cost expected range"
    return_data = true
  }
  metric_query {
    id          = "m1"
    return_data = true
    metric {
      namespace   = local.namespace
      metric_name = "CostUSD"
      dimensions  = local.all_agents
      period      = 300
      stat        = "Sum"
    }
  }
}

# ------------------------------------------------------------ pipeline health
resource "aws_cloudwatch_metric_alarm" "canary" {
  alarm_name          = "${local.name}-canary-failing"
  alarm_description   = "Synthetic canary could not get a trace through the pipeline end-to-end"
  namespace           = local.namespace
  metric_name         = "CanarySuccess"
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 2
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = var.canary_enabled ? "breaching" : "notBreaching"
  alarm_actions       = local.alarm_actions
  ok_actions          = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "processor_errors" {
  alarm_name          = "${local.name}-processor-errors"
  alarm_description   = "Trace processor Lambda is failing"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.processor.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "${local.name}-processing-lag"
  alarm_description   = "Traces waiting in Kinesis for more than 60 s (processor falling behind)"
  namespace           = "AWS/Lambda"
  metric_name         = "IteratorAge"
  dimensions          = { FunctionName = aws_lambda_function.processor.function_name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 60000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

resource "aws_cloudwatch_metric_alarm" "dlq" {
  alarm_name          = "${local.name}-dropped-batches"
  alarm_description   = "Kinesis batches gave up retrying and went to the DLQ"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.processor_dlq.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

# ------------------------------------------------------------ dashboard as code
resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${local.name}-overview"
  dashboard_body = templatefile("${path.module}/../dashboards/cloudwatch-overview.json.tftpl", {
    namespace          = local.namespace
    region             = var.region
    processor_function = aws_lambda_function.processor.function_name
    canary_function    = aws_lambda_function.canary.function_name
    alarm_arns = jsonencode([
      aws_cloudwatch_metric_alarm.error_rate.arn,
      aws_cloudwatch_metric_alarm.p90_latency.arn,
      aws_cloudwatch_metric_alarm.hourly_cost.arn,
      aws_cloudwatch_metric_alarm.latency_anomaly.arn,
      aws_cloudwatch_metric_alarm.cost_anomaly.arn,
      aws_cloudwatch_metric_alarm.canary.arn,
      aws_cloudwatch_metric_alarm.processor_errors.arn,
      aws_cloudwatch_metric_alarm.iterator_age.arn,
      aws_cloudwatch_metric_alarm.dlq.arn,
    ])
  })
}
