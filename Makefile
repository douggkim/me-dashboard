.PHONY: down up dev clean

# Project name for filtering
PROJECT_NAME=me-dashboard

down:
	docker compose down

dev-down:
	docker compose --profile dev down

# Clean only resources related to this project
clean:
	docker image prune -f --filter=label=project=${PROJECT_NAME}
	docker container prune -f --filter=label=project=${PROJECT_NAME}
	docker network prune -f --filter=label=project=${PROJECT_NAME}

up: down clean
	docker compose up -d --build

dev: dev-down clean
	docker compose --profile dev up -d --build

# Full system clean (use cautiously)
clean-all:
	docker image prune -f -a
	docker container prune -f
	docker network prune -f