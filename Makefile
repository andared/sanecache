GOLANGCI_LINT_VERSION := v2.13.2

.PHONY: test
test:
	go test -race -count=2 ./...

.PHONY: lint
lint:
	golangci-lint run ./...
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

.PHONY: bench
bench:
	go test -run '^$$' -bench . -benchmem ./...

.PHONY: bench-compare
bench-compare:
	cd benchmarks && go test -run '^$$' -bench 'Get$$|Set$$|Mixed$$' -benchmem ./...
	cd benchmarks && go test -run '^$$' -bench HitRate -benchtime=300000x ./...

.PHONY: cover
cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: tools
tools:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
