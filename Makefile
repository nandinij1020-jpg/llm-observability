ENV ?= dev
TF  = terraform -chdir=infra

.PHONY: install test lint build init plan deploy destroy demo trace dashboards outputs

install:            ## local dev dependencies
	pip install -r requirements-dev.txt
	pip install -e sdk

test:               ## unit tests (mock Bedrock, no AWS needed)
	pytest -q

lint:
	ruff check sdk lambdas scripts tests
	$(TF) fmt -recursive

build:              ## stage Lambda code into build/
	bash scripts/build_lambdas.sh

init:
	$(TF) init

plan: build
	$(TF) plan -var-file=envs/$(ENV).tfvars

deploy: build       ## make deploy ENV=dev
	$(TF) apply -var-file=envs/$(ENV).tfvars -auto-approve

destroy:            ## tear everything down (OpenSearch bills hourly!)
	$(TF) destroy -var-file=envs/$(ENV).tfvars -auto-approve

outputs:
	$(TF) output

demo:               ## generate demo traffic through Bedrock
	LLM_OBS_STREAM=$$($(TF) output -raw trace_stream_name) python scripts/demo.py

dashboards:         ## create OpenSearch Dashboards index pattern + saved searches
	LLM_OBS_OPENSEARCH=$$($(TF) output -raw opensearch_endpoint) python scripts/setup_opensearch_dashboards.py

trace:              ## show the latest trace as a tree
	LLM_OBS_OPENSEARCH=$$($(TF) output -raw opensearch_endpoint) python scripts/show_trace.py
