.PHONY: down up dev

down:
	docker compose down

up: down
	docker compose up -d --build
dev: down
	docker compose up --watch --build