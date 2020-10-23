FROM gcr.io/distroless/static:latest
LABEL maintainers="Kubernetes Authors"
LABEL description="COSI Controller"

COPY ./bin/cosi-controller-manager cosi-controller-manager
ENTRYPOINT ["/cosi-controller-manager"]
