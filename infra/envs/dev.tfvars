environment = "dev"
region      = "us-east-1"

# CHANGE THESE
alert_email             = "you@example.com"
dashboard_allowed_cidrs = ["203.0.113.7/32"] # your public IP: curl https://checkip.amazonaws.com

opensearch_instance_type  = "t3.small.search"
opensearch_instance_count = 1
kinesis_shard_count       = 1
canary_schedule           = "rate(5 minutes)"
canary_mock_bedrock       = false

# Low thresholds so the demo can trigger alerts
error_rate_threshold_pct  = 10
p90_latency_threshold_ms  = 15000
hourly_cost_threshold_usd = 1
