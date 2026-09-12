# ADR 0060: Dual-platform MVP release authority

- Status: Accepted
- Date: 2026-08-29
- Accepted: 2026-08-30

## Context

Groundplane must run on ordinary x86-64 hosts and ARM64 hosts. Earlier
proposals described managed PostgreSQL and Valkey releases as ARM64-only.
That restriction conflicts with the product and cannot become MVP authority.

The MVP still has one host, one Controller, and one Controller-managed Agent.
Dual-platform support means that the same Groundplane release can install on
either supported host architecture. It does not add multi-host scheduling or
mixed-architecture orchestration.

## Decision

### Support exactly two host platforms

The MVP supports these host platforms:

```text
linux/amd64
linux/arm64
```

Deployment maps `x86_64` to `amd64`. It maps `aarch64` and `arm64` to
`arm64`. Deployment rejects every other operating system or architecture
before it changes the target host.

Groundplane builds Go binaries with `GOAMD64=v1` for `linux/amd64` and
`GOARM64=v8.0` for `linux/arm64`. A release publishes the Controller, Agent,
CLI, helper binaries, and managed images for both platforms. A platform may
not be marked current until both variants pass their platform-specific release
checks.

### Bind one release to both supported image children

A Groundplane-published and controlled managed image index uses an OCI index
digest, not a mutable tag and not a platform child digest. It contains exactly
two runnable image manifests: one for each supported platform. This remains
true whether Groundplane built those children or publishes unchanged vendor
images. A missing, duplicate, ambiguous, or additional runnable child is a
release error. Other non-runnable descriptors do not count as runnable
children and never gain execution authority. Registry readback supplies every
digest. Source documentation never predicts a post-push digest.

Every managed-image release record contains this ordered array:

```text
platforms: [
  {
    os: "linux",
    architecture: "amd64",
    variant: null,
    child_digest: OCI_SHA256,
    config_digest: OCI_SHA256,
    added_layer_digests: [OCI_SHA256]
  },
  {
    os: "linux",
    architecture: "arm64",
    variant: "v8" | null,
    child_digest: OCI_SHA256,
    config_digest: OCI_SHA256,
    added_layer_digests: [OCI_SHA256]
  }
]
```

The authenticated AMD64 descriptor has no variant. The authenticated ARM64
descriptor decides whether its variant is `"v8"` or `null`. Every other
variant is rejected. Groundplane does not invent a variant. The release record
also binds the managed repository and the managed index digest. Its existing
domain-separated release digest covers the complete two-item array.

An AdapterRevision or another executable release identity binds the complete
multi-platform release. It is not different per host architecture. At runtime,
the Agent selects the entry that matches its compiled host architecture and
verifies the selected child and config before execution.

Compiled third-party images use the same two-platform execution authority but
do not claim that Groundplane controls the contents of the vendor's pinned
upstream index. Only that vendor-owned index may contain descriptors for
unsupported platforms and non-runnable artifacts such as attestations without
violating Groundplane's two-runnable-child publication rule. Those descriptors
are ignored for selection. They do not authorize execution, become fallback
candidates, or add platform support.

For each compiled third-party image, the catalog binds exactly one
`linux/amd64` child with no variant and exactly one `linux/arm64` child whose
authenticated variant is either `"v8"` or absent, selected from the
hash-verified upstream index. All other variants are rejected. Each binding
contains the authenticated descriptor's variant and child digest. Registry
readback then obtains the child manifest and the exact config blob it
references, verifies the config blob bytes against that digest, and binds the
verified config digest in the catalog. The config's operating system and
architecture must agree with both the authenticated index descriptor and the
catalog entry. The same child digest or config digest cannot satisfy both
supported architectures.

The immutable artifact reference remains the registry-readback index digest;
the selected child manifest and its config blob are separate immutable
identities.

Selection fails closed when a supported platform descriptor is missing,
duplicated, or ambiguous, or when the compiled repository, index digest,
platform, variant, child digest, config digest, config bytes, or config
operating system and architecture differs from registry readback. A platform
mismatch, unsupported variant, or incompatible variant fails closed.
Unsupported descriptors are never accepted as substitutes. There is no
platform fallback or emulation.

### Keep one-host authority

The Controller manages only its local Agent in the MVP. The Controller does
not choose a remote architecture. The installer detects the local host,
installs the matching binaries, and Docker resolves the matching child from
the pinned OCI index.

No platform fallback exists. An `amd64` host cannot run the `arm64` child, and
an `arm64` host cannot run the `amd64` child. Emulation is outside the MVP.

### Prove both platforms

A release must prove the same operator journey on minimal Ubuntu 24.04 for
both `linux/amd64` and `linux/arm64`. Platform-specific image checks must prove
the child descriptor, config, helper ELF identity, file metadata, inherited
runtime configuration, and runtime probes. The product capability is not
accepted from evidence for only one architecture.

## Adoption

[rejected adapter registry](../features/backing-services.md) is rejected for the MVP. Its ARM64-only release model, compiled
history and revocation registry, and reverse-reference audit do not govern the
accepted C10 implementation.

The [Backup contract](../features/backups.md) was accepted on 2026-08-30 with this
dual-platform authority, without a dependency on the rejected registry, and with one
schema with no compatibility reader.

## Consequences

- Operators can install one Groundplane release on either supported host.
- Managed images keep one immutable release identity across both platforms.
- Release automation must build, push, read back, and test two image children.
- Compiled third-party catalogs must bind and verify the two supported children
  and their config digests without treating other upstream descriptors as
  executable authority.
- The MVP does not gain multi-host scheduling, emulation, or additional
  architectures.
- The required compiled-catalog correction remains pending. This decision does
  not claim that the correction is deployed or that dual-platform runtime
  support has passed its required proof.
