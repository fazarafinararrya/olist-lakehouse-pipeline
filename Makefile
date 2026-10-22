.PHONY: help install test lint up down run run-local tf-init tf-plan tf-apply tf-destroy clean

help:        ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/ -/'

install:     ## local dev install (needs Java 17+)
	pip install -e ".[dev]"

test:        ## unit + end-to-end tests on local Spark
	pytest

lint:        ## static checks
	ruff check src tests flows

up:          ## start MinIO + Prefect server
	docker compose up -d minio minio-init prefect-server

down:        ## stop everything
	docker compose down

run:         ## run the Prefect flow in Docker against MinIO
	docker compose run --rm pipeline python flows/olist_flow.py

run-local:   ## run raw -> silver -> quality on local disk (no Docker)
	python -m olist_pipeline.cli all

tf-init:     ## terraform init
	terraform -chdir=terraform init

tf-plan:     ## terraform plan
	terraform -chdir=terraform plan

tf-apply:    ## create AWS resources (S3, Glue DBs, Athena workgroup, IAM, budget)
	terraform -chdir=terraform apply

tf-destroy:  ## remove AWS resources (do this after practice sessions)
	terraform -chdir=terraform destroy

clean:       ## remove local lake output and reports
	rm -rf data/lake/raw data/lake/silver data/lake/gold data/reports/*.json
