# Build Comqtt from upstream source. wind-c/comqtt does not publish a
# pre-built image to Docker Hub, so we pin a release tag and compile here.
# Bump COMQTT_REF when adopting a new upstream release.
#
# Build context: this directory only (no parent files needed).

ARG COMQTT_REF=v2.6.2
# golang:1.26-alpine pinned via the upstream docker-library Dockerfile in
# deploy/golang-1.26-alpine/. Build that image first if it isn't in your
# local Docker daemon yet:
#   docker build -t golang:1.26-alpine deploy/golang-1.26-alpine/
ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS build

ARG COMQTT_REF
RUN apk add --no-cache git
WORKDIR /src
RUN git clone --depth=1 --branch ${COMQTT_REF} https://github.com/wind-c/comqtt.git .

ENV CGO_ENABLED=0 GOFLAGS=-trimpath
RUN go build -ldflags="-s -w" -o /out/comqtt ./cmd/single

FROM alpine:3.23
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/comqtt /usr/local/bin/comqtt

# Defaults match cmd/single/main.go: TCP 1883, WS 1882, dashboard 8080.
EXPOSE 1883 1882 8080

# No config file by default — Comqtt's single-node main parses CLI flags
# only, with anonymous auth and in-memory storage as defaults. Operators
# override via flags or by mounting their own config and passing
# `--conf=/etc/comqtt/comqtt.yaml`.
ENTRYPOINT ["/usr/local/bin/comqtt"]
CMD ["--tcp=:1883", "--ws=:1882", "--http=:8080"]
