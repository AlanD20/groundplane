# ADR 0060: Dual-platform MVP release authority

- Status: Accepted
- Date: 2026-08-29

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

### Bind one release to both image children

A Groundplane-managed image reference uses an OCI index digest, not a mutable
tag and not a platform child digest. The index contains exactly one runnable
image manifest for each supported platform. Registry readback supplies every
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

The upstream authenticated descriptor decides whether the ARM64 variant is
`"v8"` or `null`. Groundplane does not invent a variant. The release record
also binds the managed repository and the managed index digest. Its existing
domain-separated release digest covers the complete two-item array.

An AdapterRevision or another executable release identity binds the complete
multi-platform release. It is not different per host architecture. At runtime,
the Agent selects the entry that matches its compiled host architecture and
verifies the selected child before execution. A missing, duplicate, or extra
runnable platform child is a release error.

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

ADR 0054 is rejected for the MVP. Its ARM64-only release model, compiled
history and revocation registry, and reverse-reference audit do not govern the
accepted C10 implementation.

ADRs 0047 and 0048 remain proposals. Before either can be accepted, they must
remove their ADR 0054 dependency and replace every ARM64-only image, helper,
wire, and acceptance clause with this dual-platform authority. The replacement
must keep one schema and must not add a compatibility reader.

## Consequences

- Operators can install one Groundplane release on either supported host.
- Managed images keep one immutable release identity across both platforms.
- Release automation must build, push, read back, and test two image children.
- The MVP does not gain multi-host scheduling, emulation, or additional
  architectures.
