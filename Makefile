# Data Engineering study stack - build & run helpers.
# Windows without `make`? Every target maps 1:1 to a docker command - see README.md.

COMPOSE = docker compose

.PHONY: build up down ps logs seed reset notebooks health shell

## Build images in dependency order (base first, then the services that FROM it)
build:
	docker build -t de-labs-spark-base images/spark-base
	$(COMPOSE) build jupyter streamlit

## Start the whole stack detached (pre-creates the lake dirs the bind mount needs)
up:
	$(COMPOSE) run --rm --no-deps data-generator sh -c "mkdir -p /data/spark-events /data/spark-work /data/checkpoints /data/warehouse"
	$(COMPOSE) up -d

## Stop everything (keeps the lake in ./data)
down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

## Write the batch bronze layer (10k customers / 500 products / 60k orders)
seed:
	$(COMPOSE) run --rm --no-deps data-generator python3 /repo/scripts/generate_bronze.py

## Execute every notebook headless, in order, storing outputs back into the files
notebooks:
	@for nb in notebooks/nb*.ipynb; do \
		echo "=== $$nb"; \
		$(COMPOSE) exec -T jupyter jupyter nbconvert --to notebook --execute --inplace \
			--ExecutePreprocessor.timeout=900 "$$(basename $$nb)" || exit 1; \
	done

health:
	$(COMPOSE) ps

## Shell into the Jupyter container
shell:
	$(COMPOSE) exec jupyter bash

## Destructive: stop the stack and wipe the lake (data, checkpoints, event logs)
reset:
	$(COMPOSE) down
	rm -rf data/bronze data/silver data/gold data/checkpoints data/warehouse data/spark-events data/spark-work
