# ADR 0005: S3-compatible connector implementation

Status: accepted

## Context

The MVP backup destination uses an S3-compatible object contract. Uploads must
be verified before recovery points become durable records. Connectors are
owned by exactly one environment and never inherit from project or platform
scope. Cloudflare R2 is the explicit MVP target. The live R2 gate below is
mandatory implementation and release evidence before C16 can be declared
complete; it does not defer or reverse the accepted product target.

## Decision

Use AWS SDK for Go v2 inside the `s3-compatible` connector package, pinned to
`github.com/aws/aws-sdk-go-v2` v1.43.7 and
`github.com/aws/aws-sdk-go-v2/service/s3` v1.107.3. The adapter constructs
`aws.Config` directly. It does not use the SDK `config`, `credentials`, or
`feature/s3/manager` packages. Endpoint, region, bucket, prefix, addressing
behavior, and assignment-scoped static credentials come only from the typed
Connector execution input. Core backup logic depends on the connector
interface, never AWS SDK types.

The S3 client sets `BaseEndpoint` from the Connector endpoint and
`UsePathStyle` from its required `path_style` decision. The SDK request
checksum calculation and response checksum validation modes are both
`WHEN_REQUIRED`. Every PutObject and UploadPart also carries an explicit
base64-encoded `Content-MD5` over its exact body. The connector supplies its own
HTTP client: proxy discovery is disabled, redirects are rejected, SDK request
and response logging is disabled, and no Connector option can add a custom CA.
HTTPS uses the host's standard trust roots.

The canonical Connector endpoint is valid UTF-8 and at most 2,048 bytes. The
region is exactly 1 through 64 bytes and every byte is in ASCII range
`0x21..0x7e`. These bounds are validated before constructing SDK or HTTP
state.

An object has an exact maximum size of 5 TiB (`5 * 1024^4` bytes). A known-size
object of at most 100 MiB uses one conditional PutObject. A larger object uses
a sequential multipart upload with

```text
part_size = max(100 MiB, ceil(size / 10_000 / 1 MiB) * 1 MiB)
```

and consecutive part numbers beginning at one. This yields at most 10,000
parts through the 5 TiB maximum; only the final part may be shorter than
`part_size`. PutObject and CompleteMultipartUpload use the provider's
no-overwrite conditional request. DeleteObject is conditional on the exact
verified object evidence. A failed precondition is not retryable: an object
whose immutable metadata or version evidence differs is never overwritten or
deleted.

CreateMultipartUpload, CompleteMultipartUpload, and DeleteObject each use one
SDK attempt per call. A higher-level retry may issue another one-attempt call
only after explicit read reconciliation. Every other S3 call uses a
connector-owned retryer configured for exactly three total attempts and a
maximum 20-second backoff. Neither policy consults environment variables,
shared AWS configuration, nor SDK defaults.

Mutation reconciliation is closed:

- after an ambiguous CreateMultipartUpload, fully paginate multipart uploads,
  abort every upload whose key equals the intended key, then fully paginate
  again and require no exact-key upload before creating a replacement;
- after a transport failure or 5xx ambiguity from CompleteMultipartUpload,
  HeadObject the exact key and accept only exact size, immutable metadata, and
  version evidence. If authoritative reconciliation proves the object absent
  and the upload id still exists, another one-attempt Complete is allowed. An
  explicit Complete 409, or a 404 caused by concurrent deletion of the
  multipart upload, is never retried against the same upload id: reconcile the
  final key, abort and clean any remaining exact-key multipart state, and begin
  a fresh upload only after authoritative absence. Otherwise fail closed;
- after an ambiguous DeleteObject, read the exact key and version. Absence is
  success; the same verified object permits another conditional one-attempt
  Delete, and different evidence fails closed.

Multipart enumeration and abort cleanup are allowed only beneath a namespace
reserved exclusively for Groundplane objects. Enumeration is fully paginated,
matches the complete object key rather than a prefix, and never aborts an
operator- or provider-owned upload outside that namespace.

The adapter preserves the provider's version identifier exactly. VersionId
presence is determined independently from its value. A present VersionId may
be empty or the literal value `null`; absent, present-empty, and present-literal
`null` are three distinct states. A present VersionId must be valid UTF-8 and at
most 1,024 bytes, is the immutable discriminator, and is recorded and sent
literally on later version-targeted reads and deletes. Only when VersionId is
absent is the verified ETag the immutable discriminator; an ETag must be valid
UTF-8 and from 1 through 1,024 bytes. DeleteObject targets the exact unversioned
key with `If-Match` set to that ETag. Invalid provider-returned discriminator
evidence is a fail-closed provider-contract rejection and never falls back to
the other discriminator kind.
Recovery-point commit still follows upload, HeadObject verification of the
exact size, immutable metadata, and applicable discriminator, durable
recovery-point record, then retention pruning.

The Connector credential requires object-prefix permissions for `s3:PutObject`,
`s3:GetObject`, `s3:DeleteObject`, `s3:AbortMultipartUpload`, and
`s3:ListMultipartUploadParts`, plus bucket permission
`s3:ListBucketMultipartUploads`. It also requires `s3:ListBucket`, restricted
by the `s3:prefix` condition to the protected Groundplane prefix, so exact-key
absence can be established authoritatively. A provider that returns version
identifiers also requires `s3:GetObjectVersion` and `s3:DeleteObjectVersion`
for the same object prefix. HeadObject is authorized by the applicable
GetObject permission.

Provider-neutral acceptance requires wire tests for endpoint/addressing
selection, conditional headers, explicit MD5, checksum modes, exact retry
limits, redirect rejection, and pagination, plus a MinIO integration gate for
multipart reconciliation, authoritative absence, metadata, VersionId/ETag
handling, and mismatch safety.

Cloudflare R2 remains the explicit accepted MVP target. A separate live R2
gate must prove conditional PutObject, CompleteMultipartUpload, and
DeleteObject, exact metadata round trips, VersionId-present and
VersionId-absent behavior, and conflict reconciliation. Provider-neutral wire
and MinIO evidence cannot substitute for this test. C16 is incomplete and not
release-accepted until the live R2 gate passes.

The [Backup contract](../features/backups.md) preserves this SDK and provider contract and binds
its 2,048-byte canonical endpoint and exact 1..64-byte `0x21..0x7e` region
bounds into the sole Backup schema-1 authority. Their acceptance is not the
live R2 gate required for C16 completion.

## Consequences

- R2 uses the provider-neutral implementation without a provider switch in
  core; MinIO is a conformance fixture rather than a substitute product target.
- SDK configuration and error translation stay quarantined in infrastructure.
- A successful upload without HeadObject verification is not a successful
  backup.
- Ambiguous mutations become explicit reconciliation states rather than blind
  SDK retries.
