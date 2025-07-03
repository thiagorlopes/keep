.PHONY: stop start test

stop:
	docker-compose down

start: stop
	docker-compose up --build

test:
	docker-compose run --build --rm test
