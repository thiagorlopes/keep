.PHONY: down start

stop:
	docker-compose down

start: down
	docker-compose up --build
