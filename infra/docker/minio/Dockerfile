FROM golang:1.24.8-alpine3.22 AS build

ARG MINIO_VERSION=RELEASE.2025-09-07T16-13-09Z
ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z

ENV CGO_ENABLED=0
ENV GOBIN=/out

RUN --mount=type=cache,target=/go/pkg/mod \
    go install -trimpath -ldflags="-s -w" github.com/minio/minio@${MINIO_VERSION} \
    && go install -trimpath -ldflags="-s -w" github.com/minio/mc@${MC_VERSION}

FROM alpine:3.22

ARG MINIO_VERSION=RELEASE.2025-09-07T16-13-09Z
ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z

RUN apk add --no-cache ca-certificates curl

COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /out/mc /usr/local/bin/mc

LABEL org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.version="${MINIO_VERSION}" \
      io.flashflood.mc.version="${MC_VERSION}"

EXPOSE 9000 9001
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/minio"]
CMD ["server", "/data"]
