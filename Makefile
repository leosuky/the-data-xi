.PHONY: up down bash spark-ui bronze

up:
	@docker compose -f .devcontainer/docker-compose.yml up -d --build

down:
	@docker compose -f .devcontainer/docker-compose.yml down

bash:
	@docker compose -f .devcontainer/docker-compose.yml exec app bash

spark-ui:
	@echo "Spark UI -> http://localhost:4040 (when a job is running)"

bronze:
	@docker compose -f .devcontainer/docker-compose.yml exec app \
	  python -m src.pipelines.bronze_ingest
