# ADR 0012: Closed Blueprint bundles for Compose inputs

- Status: Accepted
- Date: 2026-08-20

## Context

An environment Blueprint is a Compose-compatible desired-state document. The
accepted Blueprint contract preserves native Compose fields and accepts
multiple Compose sources, `include`, file-based `extends`, `env_file`,
`label_file`, native configs and secrets, and Controller-controlled variable
interpolation. The Controller resolves those inputs into one canonical Compose
project before Agent execution.

The current parser cannot implement that contract. It accepts only one YAML
byte slice, with no ordered source list, companion files, or interpolation
mapping. Passing those bytes to the default `compose-go` loader would also let
relative references read the Controller filesystem. Supplying the Controller
process environment would make interpolation depend on deployment state. Both
behaviors violate the Blueprint requirement that desired state be reproducible
from explicit inputs.

This ADR must define a closed input boundary without narrowing the accepted
Compose grammar. It must not replace Compose with a partial parser or reject
native features that `blueprint.md` already includes in the MVP.

## Accepted constraints

The eventual decision must preserve these existing contracts:

- Standard Compose keys retain their standard meaning and valid native fields
  are not silently dropped.
- Ordered multiple Compose sources, `include`, and file-based `extends` are
  resolved into one canonical project before dispatch.
- Bundle-relative `env_file`, `label_file`, configs, secrets, and other native
  file references remain part of the Compose model where the final decision
  permits their content class.
- Interpolation uses only Controller-controlled inputs. Secret values are never
  interpolation inputs in persisted desired state.
- The Controller parses and validates with `compose-go/v2`; it does not maintain
  a handwritten parallel Compose grammar.
- The Agent receives canonical Compose plus explicit materializations and typed
  execution steps. It never resolves the authored bundle independently.

## Decision

Replace the byte-only environment parser with a logical `BlueprintBundle`
input. The bundle contains, independently of its eventual API encoding:

- one root Blueprint path whose document contains the Groundplane envelope;
- an ordered list of Compose source paths, with the stripped root Compose body
  as the primary source;
- a closed set of normalized relative paths and their bytes for every submitted
  source and companion file; and
- an explicit non-secret interpolation mapping.

The Controller loads the bundle in two stages:

1. Parse the root envelope and resolve its tenant, project, and environment to
   stable ids.
2. Load the ordered Compose sources with `compose-go`, using only the bundle's
   file namespace, explicit interpolation mapping, and Controller-derived
   identity inputs.

All local references resolved during parsing must stay within the bundle.
Paths are normalized, relative, unique, and traversal-free. The loader receives
no implicit `.env`, process environment, Controller or Agent host path, symlink,
network resource, or remote fallback. Bundle-relative `include`, `extends`,
`env_file`, `label_file`, and permitted config or secret references are loaded
through this closed namespace rather than disabled.

After Groundplane extensions are validated and compiled, reconciliation reads
the normalized desired records and regenerates one canonical Compose project
plus the required materializations. The submitted bundle remains durable only
for audit and deterministic reparse verification; it is not an alternate
desired-state authority, and ordinary reconciliation never reparses it or
depends on files from the machine that submitted the Blueprint. Groundplane
extension fields remain typed Controller input and never become an executable
escape hatch.

The clean implementation replaces `ParseEnvironmentDocument([]byte)` and its
untyped Compose map with a context-aware bundle parser that returns the typed
`compose-go` project plus typed Groundplane extensions. There is no byte-only
compatibility path and no fallback to ambient files or environment variables.

## Approved boundary choices

The owner approved these choices on 2026-08-20:

1. **Transport encoding.** The human API uses `multipart/form-data` with one
   JSON manifest part and one binary part for each path named by the manifest.
   The manifest carries a format version, root path, ordered Compose source
   paths, explicit interpolation mapping, path list, and SHA-256 for every file.
   Archives, base64-in-JSON, implicit directory reads, and unlisted parts are
   rejected.
2. **Resource limits.** A decoded bundle contains at most 64 files, each file is
   at most 256 KiB, all file bytes total at most 768 KiB, and each normalized
   relative path is at most 240 bytes. Transport framing does not count toward
   these limits. Because the accepted transport is not compressed, there is no
   separate expanded-size limit.
3. **Bind policy.** A native bind source is accepted only when it names bundled
   non-secret content, is read-only, and is materialized below the stable
   environment directory. Writable binds, arbitrary host paths, sockets,
   devices, and paths outside that managed directory are rejected.
4. **Secret files.** Native Compose secret grants are preserved in rendered
   Compose, but authored secret content and secret file inputs must use a
   Groundplane secret reference through `x-gp-entry`. Secret bytes are never a
   bundle file, interpolation value, canonical Compose value, or persisted
   desired-state value.

## Owner-approved resolutions

The owner approved these final resolutions on 2026-08-20:

1. **Durable form.** The initially approved direction to persist canonical
   Compose and materializations conflicts with the accepted architecture,
   which makes normalized desired records the persistence unit and classifies
   render plans as ephemeral. The resolution is to persist the
   immutable submitted closed bundle under an environment generation for
   audit and deterministic reparse verification only; it is not an alternate
   desired-state authority. Normalized resource records remain the sole
   reconciliation source of truth. Canonical Compose, generated
   materializations, and all secret-bearing output are regenerated and never
   persisted.
2. **The 1:1 submission action.** The locked CLI tree has no Blueprint action,
   and the Console has no corresponding bundle submission action. The contract
   must choose whether bundle submission is part of environment create/edit or
   a new explicit environment action, then define its CLI flags and Console
   interaction without adding a parity exception. **Resolution:**
   add `environment apply <slug> --bundle-dir DIR --root RELATIVE_PATH`,
   `PUT /environments/{id}/blueprint` with the multipart bundle and a
   `202 {task_id}` response, and an Environment Desired State “Apply
   Blueprint” drawer using a directory picker plus explicit root selection.
   The action validates and commits desired state atomically before scheduling
   reconciliation; it is not a local CLI-only parser and not a parity
   exception.
3. **Parser complexity.** The byte limits bound input size but do not by
   themselves bound YAML aliases or the fully resolved Compose object graph.
   The contract must select exact alias, object-count, and resolution-depth
   limits, including whether exceeding any one of them uses
   `validation.failed` or a dedicated stable error code. **Resolution:** allow
   at most 100,000 aggregate YAML nodes, 1,000 alias
   references, a maximum alias/include/extends resolution depth of 16, and 512
   aggregate resolved Compose resources across services, networks, volumes,
   configs, and secrets. Exceeding any limit fails side-effect-free with
   `validation.failed` and HTTP 422.

The exact etcd transaction and deletion mechanics are owned by ADR 0013 and
must be accepted before durable repository implementation. The parser must use
the choices above explicitly rather than selecting loader defaults.

## Owner-approved submission details

The owner approved these exact submission details on 2026-08-20:

1. **Source ordering and interpolation UX.** `--bundle-dir` and `--root` do not
   describe ordered additional Compose sources or explicit interpolation
   values. **Resolution:** add repeatable `--compose-file
   RELATIVE_PATH` flags in layer order and repeatable `--var KEY=VALUE` flags;
   the root Compose body is always first. The Console drawer mirrors these as
   a reorderable source list and non-secret key/value table. The selected
   directory supplies the closed file namespace and symlinks are rejected.
2. **Multipart part identity.** A path cannot safely double as an arbitrary
   multipart field name. **Resolution:** the manifest `files`
   array contains `{path, part, size, sha256}` in path order, where `part` is
   exactly `file-000001`, `file-000002`, and so on. The request contains one
   `application/json` part named `manifest` and one
   `application/octet-stream` part with each declared field name; filenames,
   undeclared parts, missing parts, reordered identifiers, and duplicate parts
   are rejected.

CLI, Console, and transport implementations must use these details exactly.

## Alternatives considered

### One hermetic YAML document

Rejecting `include`, `extends`, `env_file`, `label_file`, and native companion
files would be simple, but it contradicts the accepted MVP Compose surface.
This alternative is rejected unless the authoritative Blueprint contract is
explicitly changed first.

### Controller filesystem and process environment

Resolving relative references against the Controller working directory and
using its process environment would require no bundle contract, but identical
submissions could produce different desired state or read unrelated host data.
This alternative is rejected.

### A Groundplane-only partial Compose model

Parsing only the fields currently used by the Console would avoid external
inputs, but it would silently discard valid native Compose behavior and violate
ADR 0003. This alternative is rejected.

## Consequences

- Identical bundles, explicit interpolation inputs, and referenced Groundplane
  records produce identical normalized desired state.
- Accepted Compose source layering and native fields remain available without
  granting parser access to ambient host state.
- API, CLI, Console, persistence, and parser work must ship as one contract
  change; a local parser-only implementation is insufficient.
- Bundle validation is side-effect free and completes before desired state is
  persisted or a task is scheduled.
- Future bundle features extend this explicit boundary rather than adding
  compatibility fallbacks to ambient files, environment variables, or network
  resources.
