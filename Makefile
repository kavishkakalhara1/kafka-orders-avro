PYTHON ?= .venv/bin/python
ARGS ?=

.PHONY: install up ui dashboard topics down produce consume retry dlq test integration lint
install:
	python3 -m venv .venv
	$(PYTHON) -m pip install -r requirements.txt
up:
	docker compose up -d --wait kafka schema-registry
	$(MAKE) topics
ui:
	docker compose --profile ui up -d --wait
dashboard:
	$(PYTHON) -m dashboard.server
topics:
	bash docker/setup-topics.sh
down:
	docker compose --profile ui down
produce:
	$(PYTHON) -m producer.main $(ARGS)
consume:
	$(PYTHON) -m consumer.main $(ARGS)
retry:
	$(PYTHON) -m consumer.retry_worker $(ARGS)
dlq:
	$(PYTHON) -m consumer.dlq_inspector $(ARGS)
test:
	$(PYTHON) -m pytest -m "not integration"
integration:
	NO_PROXY=localhost,127.0.0.1,$$NO_PROXY RUN_INTEGRATION=1 $(PYTHON) -m pytest -m integration -v
lint:
	$(PYTHON) -m ruff check .