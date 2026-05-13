.PHONY: smoke-test smoke

smoke-test:
	python3 backend/tests/smoke_test.py

smoke: smoke-test
