# 0004. Build Comqtt From Source for the Dev/Test Broker

Date: 2026-05-01

## Status

Accepted

## Context

Comqtt is the production MQTT broker. **Comqtt deployment and
configuration are owned by the operator** and are not part of this
codebase. We still need a broker in `deploy/docker-compose.yml` and in
integration tests so the subscriber package can be exercised end-to-end
without mocks.

Comqtt does not publish a pre-built image to Docker Hub. The first cut
of the Compose file pointed at `wind2009/comqtt:latest`, which fails to
pull. There were three options worth weighing:

1. Build Comqtt from upstream source via a small Dockerfile in this repo.
2. Substitute eclipse-mosquitto v2 (an MQTT 5 + shared-subscription
   broker with an official image).
3. Embed mochi-mqtt (the engine Comqtt itself wraps) for tests.

(2) and (3) would diverge dev/tests from the production broker. (1)
keeps fidelity at the cost of a build step. The build is a single Go
binary, finishes in under a minute, and gives us identical behavior to
production for the protocol surface this service touches (MQTT 5, QoS 1,
manual ack, shared subscriptions).

## Decision

Build Comqtt from upstream source in `deploy/comqtt/Dockerfile`, pinned
to `v2.6.2` via the `COMQTT_REF` build arg.

- `docker compose -f deploy/docker-compose.yml build mqtt` produces
  `comqtt:v2.6.2` locally.
- The Compose `mqtt` service uses that image.
- `test/integration/subscriber_integration_test.go` builds from the same
  Dockerfile via testcontainers-go's `FromDockerfile`, so the broker
  under test is byte-identical to the dev-stack broker.
- Bumping Comqtt is a single-line change in
  `deploy/comqtt/Dockerfile` (`ARG COMQTT_REF=...`).

The Dockerfile compiles `cmd/single` (the standalone single-node entry
point), passes `--tcp=:1883 --ws=:1882 --http=:8080` by default, and
runs anonymous-auth in-memory because that matches the `single` mode
expected for dev. Production deployments do their own configuration on
top of the operator's preferred image.

## Alternatives Considered

- **Substitute eclipse-mosquitto v2.** Cheaper and pulls without a build
  step. Rejected because dev/tests should run the same broker
  implementation as production; substituting a different broker means
  passing dev tests do not prove production parity.
- **Embed mochi-mqtt for tests.** Comqtt is built on mochi, so the
  protocol surface is the same — but the test-broker / dev-broker /
  prod-broker triplet would still be three different images, which
  defeats the goal.
- **Use a third-party comqtt mirror image.** No official one exists; an
  unofficial mirror is a supply-chain risk we don't need.

## Consequences

**Easier**:
- Dev stack and integration tests run real Comqtt, so any broker-side
  surprises (subscribe option restrictions, MQTT 5 reason codes, shared
  subscription distribution behavior) surface immediately.
- The version we test is the version we recommend in deployment docs.

**Harder**:
- First `docker compose up` and first `make test-integration` run pay
  the cost of building Comqtt (~30–60s on a developer laptop, cached
  for subsequent runs).
- We need to track upstream Comqtt releases and bump `COMQTT_REF`
  deliberately rather than letting `:latest` move under us.
- Any breakage in the upstream `cmd/single` entry point breaks our
  build. Mitigation: pinned tag.

## References

- wind-c/comqtt: https://github.com/wind-c/comqtt
- Comqtt v2.6.2 release: https://github.com/wind-c/comqtt/releases/tag/v2.6.2
- ADR 0003 (RustFS swap): same pattern of preferring an explicit, pinned
  build over a substitute when the protocol surface matters.
