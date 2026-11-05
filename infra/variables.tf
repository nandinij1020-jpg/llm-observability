variable "project" {
  description = "Prefix for all resource names"
  type        = string
  default     = "llm-obs"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod) — every resource name includes it, so environments never collide"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod."
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "alert_email" {
  description = "Email address that receives SNS alerts (confirm the subscription email AWS sends)"
  type        = string
}

variable "dashboard_allowed_cidrs" {
  description = "Public IP ranges allowed to open OpenSearch Dashboards in a browser, e.g. [\"203.0.113.7/32\"]"
  type        = list(string)
  default     = []
}

variable "opensearch_instance_type" {
  type    = string
  default = "t3.small.search"
}

variable "opensearch_instance_count" {
  type    = number
  default = 1
}

variable "opensearch_volume_gb" {
  type    = number
  default = 10
}

variable "opensearch_engine_version" {
  type    = string
  default = "OpenSearch_2.13"
}

variable "opensearch_encrypt_at_rest" {
  description = "Encryption at rest (turn on for prod; some small instance types do not support it)"
  type        = bool
  default     = false
}

variable "kinesis_shard_count" {
  description = "Shards for PROVISIONED mode. Each shard: 1 MB/s or 1,000 records/s in."
  type        = number
  default     = 1
}

variable "kinesis_retention_hours" {
  type    = number
  default = 24
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "canary_schedule" {
  type    = string
  default = "rate(5 minutes)"
}

variable "canary_enabled" {
  type    = bool
  default = true
}

variable "canary_model_id" {
  type    = string
  default = "amazon.nova-micro-v1:0"
}

variable "canary_mock_bedrock" {
  description = "true = canary skips the real Bedrock call (no model cost) but still tests the pipeline"
  type        = bool
  default     = false
}

# ---- alert thresholds -------------------------------------------------------
variable "error_rate_threshold_pct" {
  type    = number
  default = 10
}

variable "p90_latency_threshold_ms" {
  type    = number
  default = 15000
}

variable "hourly_cost_threshold_usd" {
  type    = number
  default = 5
}

variable "anomaly_band_width" {
  description = "Standard deviations for the CloudWatch anomaly detection band"
  type        = number
  default     = 2
}

variable "anomaly_z_threshold" {
  description = "z-score that triggers the processor's real-time EWMA anomaly alert"
  type        = number
  default     = 3
}
