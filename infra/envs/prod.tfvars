environment = "prod"
region      = "us-east-1"

alert_email             = "oncall@example.com"
dashboard_allowed_cidrs = []

opensearch_instance_type   = "m6g.large.search"
opensearch_instance_count  = 2
opensearch_volume_gb       = 50
opensearch_encrypt_at_rest = true
kinesis_shard_count        = 2
kinesis_retention_hours    = 72
log_retention_days         = 90
canary_schedule            = "rate(1 minute)"

error_rate_threshold_pct  = 5
p90_latency_threshold_ms  = 10000
hourly_cost_threshold_usd = 50
