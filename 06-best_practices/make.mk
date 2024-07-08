.PHONY: integration-tests

S3_ENDPOINT_URL ?= http://localhost:4566

integration-tests:
    python integration_test.py
