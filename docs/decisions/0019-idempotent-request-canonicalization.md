# ADR 0019: Canonicalize mutating HTTP intent before idempotency claims

- Status: Accepted
- Date: 2026-08-20

## Context

ADR 0013 and `docs/api-cli.md` require every human-API `POST`, `PUT`,
`PATCH`, and `DELETE` to carry one `Idempotency-Key`. The accepted behavior is
also clear at the durable boundary: a key identifies one user intent, an exact
completed or failed duplicate replays its result, an active duplicate returns
`idempotency.in_progress`, and reuse for another canonical request returns
`idempotency.mismatch`.

The accepted text does not yet define the canonical request precisely enough
to implement its SHA-256 digest. It names method, route, owner or scope, path,
query, body, and intent-affecting headers, but leaves these observable choices
open:

- whether a path is the raw escaped URL, decoded URL, route template, or a
  reconstruction from validated path parameters;
- whether query order, repeated keys, omitted defaults, and explicit default
  values are equivalent;
- which headers affect intent and how media-type parameters are normalized;
- whether JSON object order, insignificant whitespace, duplicate members, and
  alternative number spellings are equivalent;
- how the Blueprint multipart body excludes its random boundary and transport
  part framing without buffering the complete raw request; and
- how a future canonicalization change avoids silently changing the meaning of
  a marker retained for 90 days.

Choosing any of these in middleware would create a public replay contract by
accident. A raw-body digest would also make an otherwise identical Blueprint
retry mismatch whenever its multipart boundary changes. A generic middleware
cannot know route defaults or distinguish a validated Blueprint manifest from
untrusted transport metadata.

An additional security boundary applies when a canonical body contains a
password, token, private key, or other low-entropy secret. A stored, unkeyed
SHA-256 digest of that body is an offline guessing oracle: an etcd snapshot
holder could hash candidates without the Controller key and test them against
the marker. The intent digest must therefore be treated as sensitive material
and receive the same Controller-root age protection as the secret value that
contributed to it.

This ADR defines the canonicalization contract. The owner accepted all seven
choices on 2026-08-20. The durable marker repository and replay middleware
remain separately gated on their complete key and value schema.

## Accepted constraints

Any eventual decision must preserve the existing contracts:

- every mutating human-API method requires exactly one key of 16 through 128
  ASCII characters matching `[A-Za-z0-9._:-]+`;
- Console and CLI create one key per user intent and reuse it only for
  transport retry;
- all ordinary JSON request bodies are bounded to 1 MiB and are buffered by
  the accepted HTTP lifecycle before decoding;
- Blueprint multipart transport is bounded to 2 MiB while remaining streamed;
  the decoded closed bundle is bounded to 64 files, 256 KiB per file, and
  768 KiB in aggregate;
- synchronous transport, schema, reference, and domain validation finishes
  before a durable idempotency marker is claimed;
- marker claim and desired-state mutation or Task creation occur in one etcd
  transaction;
- replay stores the exact original public result rather than regenerating a
  response;
- the idempotency key itself scopes marker lookup but does not become part of
  the request-intent digest being compared;
- secret values and submitted Blueprint bytes never appear in an error, log,
  metric label, trace attribute, marker key, or digest input diagnostic; and
- an intent digest is sensitive transient material, is never stored or
  rendered in plaintext, and is cleared by every owner after use; and
- this ADR does not choose the durable marker key/value layout, garbage
  collector mechanics, or repository interface.

## Decision

### 1. Canonicalize validated intent, not an `http.Request`

The route handler first performs all accepted transport and synchronous
validation. It then constructs a typed `CanonicalIntentV1` from validated
values and passes that value to a small, pure canonical encoder. The encoder
does not accept `*http.Request`, `io.Reader`, `http.Header`, arbitrary maps, or
an idempotency key. This makes it impossible for the digest module to drain a
body, bypass a size limit, trust unvalidated multipart metadata, or
accidentally include a transport-only header.

The logical envelope is:

```text
CanonicalIntentV1 {
  version:      1
  method:       registered uppercase method
  route:        registered API route template
  scope_kind:   canonical owner kind or "platform"
  scope_id:     stable owner id or empty for platform
  path:         ordered validated path bindings
  query:        route-specific effective query value
  content_kind: "none" | "json" | "blueprint-bundle-v1"
  body:         route-specific canonical body value
}
```

`route` is the exact registered pattern below `/api/v1`, for example
`/services/{id}/deploy`; it is not `RequestURI`, `URL.Path`, or a cleaned URL.
`path` is an ordered list whose names follow their appearance in that pattern
and whose values are the decoded, validated canonical values. Stable ids are
included exactly in their canonical kind-prefixed form. A handler never
reconstructs these fields from a raw URL after validation.

`scope_kind` and `scope_id` use the durable owner of the mutation after every
id or slug has been resolved. The only id-less owner is the literal
`scope_kind = "platform"` with an empty `scope_id`. The route template, scope,
and actual path bindings are included even when some values appear again in
the body. That duplication is deliberate domain separation and prevents two
routes or owners with structurally equal bodies from sharing an intent digest.

### 2. Use one versioned, length-framed binary encoding

The digest input begins with the ASCII domain separator
`groundplane-http-intent`, a zero byte, and the unsigned byte `1`. Every
subsequent scalar is encoded as a one-byte type tag, an unsigned 64-bit
big-endian byte length, then exactly that many bytes. Lists encode their item
count followed by items. Objects encode their member count followed by
UTF-8-byte-ascending member names and values. Strings are valid UTF-8 and are
not Unicode-normalized. Integers use their shortest base-10 spelling with no
leading zero; booleans and null have dedicated tags. No floating-point value
is accepted by the canonical encoder.

The envelope fields are emitted in the order shown above. Path bindings retain
route order. All other objects use UTF-8 byte order. Length framing, rather
than delimiter concatenation, prevents ambiguous tuples. The encoder returns
canonicalization version `1` and a caller-owned, fixed 32-byte SHA-256 digest
over the complete framed value. The digest is sensitive binary material. It
has no public string or hexadecimal form, never implements text or JSON
marshaling, and is never written to a log, problem response, metric label,
trace attribute, Task event, marker key, or diagnostic. The caller clears its
digest buffer immediately after sealing or comparison; the encoder clears all
mutable intermediate buffers before returning.

Version 1 fixes the one-byte tag table permanently:

| Tag byte | Value |
| --- | --- |
| `0x01` | null |
| `0x02` | false |
| `0x03` | true |
| `0x04` | integer |
| `0x05` | UTF-8 string |
| `0x06` | list |
| `0x07` | object |
| `0x08` | fixed 32-byte verified Blueprint file SHA-256 |

Version 1 is immutable for the lifetime of `/api/v1` markers. A future
algorithm uses a new API version or a separately approved marker migration; it
never silently reinterprets an existing version-1 marker.

This narrow encoder is internal and implemented directly with the standard
library. It is not a general CBOR, JSON Canonicalization Scheme, or protobuf
surface, and no third-party canonicalization dependency is added.

### 3. Canonicalize query from the validated effective value

Each mutating route declares one typed query-input value. Decoding rejects an
unknown query key, a key repeated when the schema is scalar, a malformed
escape, and any value outside the route schema before marker lookup.

The canonical query is produced from the validated effective value, after
documented defaults are applied. Therefore omitted and explicitly supplied
default values have one digest. Optional values without a default preserve
the distinction between absent, empty, and non-empty. Lists preserve their
declared semantic order; a route whose list is defined as a set must sort and
deduplicate it during validation before constructing the intent. The digest
never depends on raw query-pair order, `+` versus `%20`, or percent-hex case.

A route with no query input uses an empty object. Handlers cannot pass raw
`url.Values` to the encoder.

### 4. Close the intent-affecting header set

For the MVP, the only intent-affecting request header is `Content-Type`, and it
is represented by `content_kind` rather than copied as text:

- no body is `none`;
- accepted JSON is `json`; and
- the accepted closed Blueprint multipart form is `blueprint-bundle-v1`.

The JSON boundary accepts only `application/json` with no parameter or exactly
`charset=utf-8`, compared case-insensitively; both forms canonicalize to
`json`. The Blueprint boundary accepts `multipart/form-data` with exactly one
valid `boundary` parameter; the boundary is transport framing and is excluded
from the canonical value. Every other media type or parameter is rejected
before marker lookup.

`Idempotency-Key`, `Accept`, `Authorization`, `Host`, `Content-Length`,
`Transfer-Encoding`, `User-Agent`, tracing headers, forwarding headers,
connection headers, and retry metadata are excluded. Authorization identity
belongs in the authenticated owner/scope when authentication is introduced;
bearer material is never hashed. The API currently defines no conditional
mutation header. Adding one requires a contract change that gives it a typed,
canonical field; copying arbitrary headers into the digest is prohibited.

### 5. Canonicalize JSON after strict decode and validation

JSON decoding rejects duplicate object member names, invalid UTF-8, a second
JSON document, trailing non-whitespace bytes, and a number that cannot be
represented by the route's declared integer or exact decimal-string type.
Routes do not accept untyped floating-point input. Unknown-member behavior
remains the route schema's responsibility and occurs before digest creation.

The route-specific canonical body value is constructed from the validated API
input, preserving semantically meaningful absent-versus-null distinctions and
applying documented defaults exactly as the use case does. JSON member order,
insignificant whitespace, escape spelling, and valid alternative integer
spelling therefore do not affect the digest. A bodyless mutation uses the
dedicated null body with `content_kind = "none"`; it is not treated as an
empty JSON object.

Secret-bearing fields enter the hash through the framed encoder but are never
retained separately. The implementation owns the minimum byte slices needed
for the digest, clears mutable secret buffers at the established ownership
boundary, and exposes the fixed digest only to the immediate caller as
sensitive, caller-owned bytes.

### 6. Canonicalize Blueprint from its verified closed-bundle manifest

Blueprint apply remains streaming at the raw HTTP layer. The multipart parser
first enforces ADR 0012's exact part identities and limits, streams every file
through SHA-256, and verifies its declared size and digest. Only a fully
validated bundle may construct an intent.

The canonical Blueprint body is the typed manifest value containing its
format version, normalized root path, ordered Compose source paths, explicit
interpolation mapping, and path-sorted file entries
`{path, part, size, sha256}`. SHA-256 values are decoded to their fixed 32-byte
form before framing. The manifest's file digests commit to the submitted bytes,
so file bytes are not copied into a second aggregate buffer or hashed again by
the intent encoder.

Multipart boundary text, MIME header spelling and order, transport part order,
and Content-Disposition quoting never enter the intent. They are either
validated transport framing or rejected. Two valid submissions of the same
closed bundle therefore produce one digest even when their multipart
boundaries differ. A changed file byte, manifest value, Compose-source order,
or interpolation value produces another digest.

### 7. Keep marker and replay behavior outside this module

The canonical module returns only `(version, 32-byte sensitive digest)` or a
closed internal error for a programming-invalid typed value. It does not claim
a marker, inspect prior state, choose HTTP status, cache a response, read time,
generate a key, or retry storage. Invalid external input has already been
rejected at the transport or typed route boundary and never reaches it.

The later durable idempotency repository will combine the separately
validated `Idempotency-Key`, method, route, and owner/scope with its approved
key layout. Before any marker crosses the etcd boundary, the repository
serializes only the canonicalization version and fixed 32 digest bytes and
seals that plaintext with the existing `internal/controller/secretvalue`
`Protector`, backed by the Controller's platform-wide root age key. The durable
marker contains the secret-value envelope metadata and ciphertext, never the
plaintext version or digest. The envelope's own SHA-256 covers ciphertext only
and is therefore safe non-secret integrity metadata; it must never be replaced
with a plaintext digest.

For comparison, the repository restores and opens the envelope, validates the
plaintext's exact version and length, and compares the stored and candidate
32-byte digests in constant time inside the `Protector.Open` callback. It
clears the candidate digest and every decrypted or serialized plaintext buffer
on success and every failure path. Envelope corruption, decryption failure,
unsupported canonicalization version, or malformed plaintext is durable
internal corruption and returns the closed generic internal error. It never
becomes `idempotency.mismatch`, because mismatch is reserved for two valid
digests, and no secret-bearing error text is retained.

The repository owns the accepted pending, replay, mismatch, unknown-outcome,
retention, and atomic-claim semantics. Its complete durable marker schema
remains separately gated. No middleware may return a replay until that
repository and the same-transaction resource mutation exist.

This security requirement corrects ADR 0013's stale recommendation to persist
an unprotected SHA-256 request hash. ADR 0013 still owns marker lifecycle and
atomicity, but its eventual marker schema must reference this ADR's protected
version-and-digest envelope rather than plaintext digest bytes or hex.

## Consequences

- Semantically equal validated retries have one digest despite JSON layout,
  query encoding, or multipart-boundary differences.
- Different routes, stable resources, or durable owners cannot collide at the
  intent layer even when their bodies match.
- The digest is deterministic across processes and upgrades, while etcd
  retains only its root-age-protected envelope rather than raw request, secret
  material, or a plaintext guessing oracle.
- JSON mutation handlers must preserve absent/null/default semantics in typed
  inputs and reject duplicate members.
- Blueprint parsing computes content hashes while streaming and reuses its
  already verified manifest rather than buffering the raw multipart request.
- A new intent-affecting header, floating-point input, or body content kind is
  a contract change rather than an accidental change in a generic serializer.
- The durable marker layout and repository remain separately gated; accepting
  this ADR would permit only the pure canonical module and its focused tests.

## Alternatives considered

### Hash raw method, request URI, headers, and body bytes

Rejected because equivalent query encodings and JSON layout would mismatch,
multipart boundaries would make Blueprint retries mismatch, raw headers expose
transport noise, and hashing a streaming body in generic middleware risks
consuming it or bypassing established limits.

### Canonicalize arbitrary `http.Request` values in middleware

Rejected because middleware cannot know route defaults, owner identity,
absent/null semantics, ordered versus set-valued inputs, or whether a
Blueprint manifest and its file hashes have passed domain validation.

### Use RFC 8785 JSON as the universal envelope

Rejected as the universal representation because Blueprint content is not a
JSON body, API inputs include absent/default semantics outside raw JSON, and
IEEE-754 number canonicalization is an unnecessary cross-runtime constraint
for a server-internal digest. A small typed framed encoder has a narrower
surface and forbids floats outright.

### Hash only the normalized desired-state result

Rejected because two distinct operator requests can converge on the same
desired record while differing in action, owner, path, operational parameter,
or requested Task. Idempotency protects the submitted intent, not only its
eventual state.

## Acceptance and implementation gate

The owner explicitly approved:

1. validated semantic intent rather than raw HTTP bytes;
2. the immutable version-1 framed encoding and fixed 32-byte sensitive
   SHA-256 output with caller-cleared ownership;
3. default-aware typed query normalization and rejection of unknown or
   duplicate query inputs;
4. the closed MVP header/content-kind set;
5. strict typed JSON canonicalization with no floating-point values; and
6. verified Blueprint-manifest canonicalization independent of multipart
   framing; and
7. root-age envelope protection for durable version-and-digest bytes,
   constant-time comparison, and internal classification of corrupt durable
   state.

After acceptance, implementation starts with focused race-safe unit tests for
field framing, route/scope separation, query equivalence and mismatch,
JSON absent/null/default behavior, secret-safe failures, and Blueprint boundary
independence. The implementation must also prove that oversized and malformed
bodies are rejected by the existing HTTP lifecycle before intent creation and
that the pure encoder never reads a stream. Tests must prove that no raw digest
is logged, formatted, marshaled, returned in a problem, used in a marker key,
or persisted; every owned digest and decrypted plaintext buffer is cleared;
valid digests compare in constant time; and corrupt envelope metadata,
ciphertext, canonicalization version, or plaintext length fails as generic
internal state rather than mismatch without leaking digest or secret bytes.
Durable markers, replay responses, and etcd keys remain blocked until their
separate layout is accepted.
