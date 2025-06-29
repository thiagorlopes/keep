.PHONY: down start

down:
	docker-compose down

start: down
	docker-compose up --build
