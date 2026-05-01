# 0003. RustFS for Dev Object Storage; AWS SDK v2 for S3 Client

Date: 2026-05-01

## Status

Accepted

## Context

The project's original mandatory-dependencies list specified MinIO as the
local-dev S3-compatible object store and `github.com/minio/minio-go/v7` as
the S3 client. The dead-letter sink writes failed messages to S3 (RustFS in
dev, AWS S3 or any S3-compatible service in prod).

Two pressures led us to revisit those defaults:

1. **MinIO licensing trajectory.** MinIO's AGPLv3 license and its enterprise
   pivot mean the open-source server is no longer a comfortable long-term
   default for client-deliverable infrastructure. Even when used only for
   local dev, MinIO's branding and license terms can complicate redistribution.

2. **`minio-go` is broker-coupled.** The `minio-go` SDK is maintained
   primarily for MinIO and lags AWS SDK v2 on features (smithy middleware,
   IRSA/IMDSv2 support, EventStream, retries, observability hooks). Picking
   it ties our client choice to a specific broker we may not even use in
   production.

## Decision

1. Replace MinIO with **RustFS** as the local-development S3-compatible
   object store. The Compose stack runs `rustfs/rustfs:latest` on port 9000
   (S3 API) and 9001 (admin console), with credentials `rustfsadmin /
   rustfsadmin` and a pre-created `mqtt2db-go-dlq` bucket.

2. Replace `github.com/minio/minio-go/v7` with the official **AWS SDK for Go
   v2** S3 client (`github.com/aws/aws-sdk-go-v2/service/s3`). We talk to
   any S3-compatible endpoint (RustFS, AWS S3, R2, B2) by overriding
   `BaseEndpoint` and enabling `UsePathStyle` when needed.

The default dead-letter bucket name changes from `mqtt-ingest-dlq` to
`mqtt2db-go-dlq` to match the renamed binary.

## Alternatives Considered

- **Keep MinIO.** Battle-tested, fast, well-documented. Rejected because
  AGPLv3 introduces ambiguity for client deliverables and the project's
  recent enterprise gating reduces our willingness to depend on it long-term.

- **SeaweedFS.** Strong S3 compatibility, broader feature set than RustFS.
  Heavier image, more knobs. We do not need its filer/replication features
  for a dev dependency, and the simpler model wins.

- **LocalStack S3.** Useful for AWS API parity testing, but it's a much
  larger surface for what is fundamentally a single-bucket dead-letter sink
  in dev. Overkill.

- **`minio-go` with a non-MinIO server.** Works, but doubles down on the
  decision we are explicitly trying to undo and lags AWS SDK v2 on features
  we will eventually want.

## Consequences

**Easier**:
- Production deployments to AWS S3 use the same client code and AWS-blessed
  credential chain (IRSA, IMDSv2, env, profile) without a separate adapter.
- License risk for client deliverables drops.
- We pick up smithy middleware, retries, and observability hooks for free.

**Harder**:
- AWS SDK v2's S3 client is more verbose than `minio-go`'s helpers; we will
  write small wrappers in `internal/deadletter` for `PutObject` and
  `HeadBucket` to keep call sites tidy.
- RustFS is younger than MinIO. We pin a specific version in Compose once
  Milestone 8 lands and bump it explicitly. If RustFS regresses S3
  compatibility, the dev story degrades — but production is unaffected
  because prod uses real S3.
- Operators running their own S3-compatible store (MinIO, Ceph) need to
  set `endpoint` and `use_path_style: true` in config. This is documented
  in `docs/CONFIGURATION.md`.

## References

- RustFS docs: https://docs.rustfs.com/installation/docker/
- RustFS image: https://hub.docker.com/r/rustfs/rustfs
- AWS SDK for Go v2: https://aws.github.io/aws-sdk-go-v2/docs/
- Configuration reference: `../CONFIGURATION.md` (`dead_letter` section)
