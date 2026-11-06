.PHONY: build demo test test-race test-all fmt vet clean

build:
	go build -o bin/idemq-server ./cmd/idemq-server
	go build -o bin/idemq-worker-demo ./cmd/idemq-worker-demo

demo: build
	./scripts/run-demo.sh

test:
	go test ./internal/... -count=1

test-race:
	go test ./internal/... -race -count=1

test-all:
	go test ./... -count=1 -timeout 120s

fmt:
	gofmt -l .

vet:
	go vet ./...

clean:
	rm -rf bin/ data/
