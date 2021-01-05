FROM gcr.io/distroless/static:latest
LABEL maintainers="Kubernetes Authors"
LABEL description="COSI Controller"

COPY ./bin/controller-manager controller-manager
ENTRYPOINT ["/controller-manager"]
