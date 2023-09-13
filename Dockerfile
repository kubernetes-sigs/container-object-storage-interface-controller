#
# BUILDER
#
FROM docker.io/library/golang:1.21.1 AS builder

WORKDIR /buildroot

# Cache deps before building and copying source, so that we don't need to re-download
# as much and so that source changes don't invalidate our downloaded layer.
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY cmd/ cmd/
COPY pkg/ pkg/

ENV CGO_ENABLED=0

RUN go build -o artifacts/controller-manager cmd/controller-manager/*.go


#
# FINAL IMAGE
#
FROM gcr.io/distroless/static:latest

LABEL maintainers="Kubernetes Authors"
LABEL description="COSI Controller"

LABEL org.opencontainers.image.title="COSI Controller"
LABEL org.opencontainers.image.description="Container Object Storage Interface (COSI) Controller"
LABEL org.opencontainers.image.source="https://github.com/kubernetes-sigs/container-object-storage-interface-controller"
LABEL org.opencontainers.image.licenses="APACHE-2.0"

COPY --from=builder /buildroot/artifacts/controller-manager .
ENTRYPOINT ["/controller-manager"]
