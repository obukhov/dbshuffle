build:
	go build -o bin/dbshuffle ./cmd

run-server:
	go run ./cmd server

status:
	go run ./cmd status

assign:
	go run ./cmd assign $(TEMPLATE) $(DB)

extend:
	go run ./cmd extend $(TEMPLATE) $(DB)

clean:
	go run ./cmd clean

refill:
	go run ./cmd refill

tidy:
	go mod tidy

db-up:
	docker compose up -d mysql

db-down:
	docker compose stop mysql

db-shell:
	docker compose exec mysql mysql -uroot -psecret

.PHONY: build run-server status assign extend clean refill tidy db-up db-down db-shell
