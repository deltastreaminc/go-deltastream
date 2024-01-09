MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

SHELL := /bin/bash
GOOS := $(shell go env GOOS)
GOPATH := $(shell go env GOPATH)

CGO_ENABLED := 0 
GOARCH := amd64
GO111MODULE := on
GOBIN := $(GOPATH)/bin
export CGO_ENABLED
export GOARCH
export GO111MODULE
export GOBIN

OAIGEN := $(GOBIN)/oapi-codegen

all: apiv2/zz_generated.api.go
	go test ./...

apiv2/zz_generated.api.go: apiv2/api-server-v2.yaml apiv2/api-server-v2-config.yaml | $(OAIGEN)
	$(OAIGEN) --config apiv2/api-server-v2-config.yaml apiv2/api-server-v2.yaml > $@

$(OAIGEN):
	go install github.com/deepmap/oapi-codegen/v2/cmd/oapi-codegen@v2.0.0
	$(GOBIN)/oapi-codegen -version

clean:
	rm apiv2/zz_generated.api.go
