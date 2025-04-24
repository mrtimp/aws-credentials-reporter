BINARY_NAME=aws-credentials-reporter

VERSION := $(shell git rev-parse --short HEAD)
BUILDTIME := $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

GOLDFLAGS += -s -w
GOLDFLAGS += -X main.Version=$(VERSION)
GOLDFLAGS += -X main.Buildtime=$(BUILDTIME)
GOFLAGS = -ldflags "$(GOLDFLAGS)"

build:
	GOARCH=arm64 GOOS=darwin go build $(GOFLAGS) -o ${BINARY_NAME}-darwin *.go
	GOARCH=arm64 GOOS=linux go build $(GOFLAGS) -o ${BINARY_NAME}-linux *.go

optimize:
	if [ -x /usr/bin/upx ] || [ -x /usr/local/bin/upx ]; then upx --brute ${BINARY_NAME}-*; fi

test:
	go test -v

clean:
	go clean
	rm -f ${BINARY_NAME}-darwin
	rm -f ${BINARY_NAME}-linux

dep:
	go mod download
