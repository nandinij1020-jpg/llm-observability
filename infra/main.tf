terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }

  # Remote state per environment. Uncomment and run:
  #   terraform init -backend-config=envs/dev.backend.hcl
  # backend "s3" {}
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      Project     = "llm-observability"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  name      = "${var.project}-${var.environment}"
  namespace = "LLMObservability/${var.environment}"
  build_dir = "${path.module}/../build"
}
