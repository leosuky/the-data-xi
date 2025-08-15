up:
	docker compose up -d --build

down:
	docker compose down

bash:
	docker compose exec app bash

logs:
	docker compose logs -f app

bronze:
	docker compose exec app python -m src.pipelines.bronze_ingest
