# Common tasks. Everything here is plain `go` underneath.

.PHONY: all build test race sim fuzz bench lint docker-up docker-down

all: lint test

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

# Needs cgo and a C compiler.
race:
	go test -race -count=1 ./...

sim:
	go run ./cmd/raftsim -seeds 5000 -nodes 3
	go run ./cmd/raftsim -seeds 5000 -nodes 5

fuzz:
	go test ./kv -run '^$$' -fuzz '^FuzzDecode$$' -fuzztime 60s
	go test ./kv -run '^$$' -fuzz '^FuzzRestore$$' -fuzztime 60s
	go test ./storage -run '^$$' -fuzz '^FuzzOpen$$' -fuzztime 60s

bench:
	go run ./cmd/raftkv bench -local

lint:
	test -z "$$(gofmt -l .)"
	go vet ./...

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down
