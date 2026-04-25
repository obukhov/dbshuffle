build:
	go build -o bin/dbshuffle ./cmd

run-server:
	go run ./cmd server

status:
	go run ./cmd status

assign:
	go run ./cmd assign $(TEMPLATE) $(DB)

reset:
	go run ./cmd reset $(TEMPLATE) $(DB)

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

tag:
	@test -n "$(VERSION)" || (echo "Usage: make tag VERSION=v1.2.3"; exit 1)
	git tag -a $(VERSION) -m "Release $(VERSION)"
	git push origin $(VERSION)

tag-patch:
	$(eval CURRENT := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"))
	$(eval NEXT := $(shell echo $(CURRENT) | awk -F. '{print $$1"."$$2"."$$3+1}'))
	$(MAKE) tag VERSION=$(NEXT)

tag-minor:
	$(eval CURRENT := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"))
	$(eval NEXT := $(shell echo $(CURRENT) | awk -F. '{print $$1"."$$2+1".0"}'))
	$(MAKE) tag VERSION=$(NEXT)

tag-major:
	$(eval CURRENT := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0"))
	$(eval NEXT := $(shell echo $(CURRENT) | awk -F. '{gsub(/v/,"",$1); print "v"$$1+1".0.0"}'))
	$(MAKE) tag VERSION=$(NEXT)

.PHONY: build run-server status assign reset extend clean refill tidy db-up db-down db-shell tag tag-patch tag-minor tag-major
