# Remote state (optional). Create the bucket and lock table once, then uncomment
# the `backend "s3" {}` block in main.tf and run:
#   terraform init -backend-config=envs/dev.backend.hcl
bucket         = "CHANGE-ME-llm-obs-tfstate"
key            = "llm-obs/dev/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "CHANGE-ME-tf-locks"
encrypt        = true
