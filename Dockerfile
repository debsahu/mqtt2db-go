# Stage 1: build static binary on the latest stable Go.
# Build the base image once via:
#   docker build -t golang:1.26-alpine deploy/golang-1.26-alpine/
FROM golang:1.26-alpine AS build

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0 GOFLAGS=-trimpath
# BuildKit cache mounts (--mount=type=cache) speed up rebuilds dramatically
# but require the docker/dockerfile:1.7 frontend image. We keep the
# Dockerfile portable to the legacy builder for offline / locked-down
# environments; bring the cache mounts back if/when your build host can
# pull that image and you want the speedup.
RUN go build -ldflags "-s -w \
        -X 'github.com/debsahu/mqtt2db-go/internal/version.Version=${VERSION}' \
        -X 'github.com/debsahu/mqtt2db-go/internal/version.Commit=${COMMIT}' \
        -X 'github.com/debsahu/mqtt2db-go/internal/version.BuildDate=${BUILD_DATE}'" \
        -o /out/mqtt2db-go ./cmd/mqtt2db-go

# Stage 2: minimal alpine runtime. Production-grade alternative is
# gcr.io/distroless/static-debian12:nonroot — swap when your build host
# can reach gcr.io. Alpine works for offline/locked-down builds and
# matches the alpine major used by the build stage (3.23).
FROM alpine:3.23

LABEL org.opencontainers.image.title="mqtt2db-go"
LABEL org.opencontainers.image.source="https://github.com/debsahu/mqtt2db-go"
LABEL org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S -g 65532 nonroot && \
    adduser -S -D -u 65532 -G nonroot nonroot

USER nonroot:nonroot
COPY --from=build /out/mqtt2db-go /usr/local/bin/mqtt2db-go

ENTRYPOINT ["/usr/local/bin/mqtt2db-go"]
CMD ["--config=/etc/mqtt2db-go/config.yaml"]
