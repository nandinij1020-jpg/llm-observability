output "trace_stream_name" {
  description = "Kinesis stream your agents send spans to (LLM_OBS_STREAM)"
  value       = aws_kinesis_stream.traces.name
}

output "usage_table" {
  value = aws_dynamodb_table.usage.name
}

output "opensearch_endpoint" {
  value = aws_opensearch_domain.traces.endpoint
}

output "opensearch_dashboards_url" {
  description = "Open in a browser (only works from dashboard_allowed_cidrs)"
  value       = "https://${aws_opensearch_domain.traces.dashboard_endpoint}"
}

output "alert_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "cloudwatch_dashboard_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=${aws_cloudwatch_dashboard.main.dashboard_name}"
}

output "xray_traces_url" {
  value = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#xray:service-map/map"
}

output "producer_policy_arn" {
  description = "Attach to whatever runs your agents"
  value       = aws_iam_policy.producer.arn
}

output "metric_namespace" {
  value = local.namespace
}
