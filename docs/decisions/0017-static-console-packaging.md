# ADR 0017: Embed an untracked Console build reproducibly

- Status: Accepted
- Date: 2026-08-20

## Context

The accepted product contract requires the native Controller binary to serve
both the REST API and the production React/Vite Console. Vite emits
`console/dist/`, those files must be embedded with `go:embed`, Node must not be
needed on the production host, and `console/dist/` must remain untracked.

Those requirements do not currently form a reproducible build contract:

- `go:embed` may embed only files below the Go package containing the
  directive, so a package under `internal/` cannot directly embed
  `console/dist/`;
- an unconditional embed directive makes `go build ./...` and `go test ./...`
  fail on a clean checkout because the ignored directory does not exist;
- `make controller`, `make build`, and `make ci` currently run Go without first
  running the Console build;
- CI does not install an exact Node and npm toolchain or build the Console
  before the Go gate; and
- the contract does not choose a generated Go blob, a copied staging tree, or
  a build-tagged direct embed.

The static-serving behavior is also incomplete. The Controller needs an exact
rule for API precedence, client-side route fallback, cache headers, content
types, invalid paths, and a binary built without production assets. Choosing
these rules in code would create a packaging and HTTP contract by accident.

This ADR records that contract. The packaging, dispatcher, and exact toolchain
are implemented together so that the release behavior does not depend on a
developer workstation or adjacent files.

## Accepted constraints

Any eventual decision must preserve these existing requirements:

- the Controller is the one HTTP backend for the API and Console;
- the production Console is the Vite output from `console/`;
- the production Controller is one self-contained binary and needs no Node
  runtime or adjacent asset directory;
- generated `console/dist/` files remain ignored and are never committed;
- API routes cannot fall through to the SPA;
- no disk-serving or legacy-path compatibility fallback is introduced; and
- C22 is not Accepted until the Console uses the generated API client in
  addition to being built, embedded, and served.

## Decision

### 1. Direct, build-tagged embedding

Add `console/embed.go`, guarded by `//go:build groundplane_console`. It is the
only Go file in a small stdlib-only package beside the Vite application. The
directive is exactly `//go:embed all:dist`: `all:` makes the embedded tree the
exact Vite output even if a future build emits internal dot- or
underscore-prefixed metadata. The package exposes an `fs.FS` rooted at
`dist/`; it does not expose a host path.

Hidden build metadata is not public content. The static handler rejects a URL
with any path element beginning with `.` or `_`, even when that file exists in
the embedded filesystem. Keeping the directive beside the Vite output avoids
copying bytes into a second generated tree and avoids a generated Go source
blob.

The application wiring has two named compile-time implementations under
`internal/app`:

- a `groundplane_console` file imports the `console` Go package and injects its
  rooted `fs.FS` into the Controller; and
- a `!groundplane_console` file does not import that package and injects no
  Console filesystem.

The split is required because the `console` Go package has no buildable files
without the tag. `cmd/controller` remains thin and imports only `internal/app`.
The Controller and static handler depend only on `fs.FS`; neither imports
`embed` nor knows the asset source.

`architecture.md` and `standards.md` record the
stdlib-only `console` asset package and its allowed import direction. The
golines gate must include `./console/`; otherwise the new Go file falls outside
the repository's enforced Go rules.

### 2. One authoritative build order and exact toolchain

Use `npm ci`, never `npm install`, and use `console/package-lock.json` as the
setup-node cache dependency. Reproducible means that a clean checkout uses the
same exact Node release, exact npm release, lockfile, Vite configuration, and
Go version declaration. Pinning only a Node major does not satisfy this
contract.

The exact toolchain is:

1. Node `24.19.0` LTS; and
2. npm `11.17.0`, the npm release shipped by that Node line.

The pins come from the official Node 24 archive and release record:
<https://nodejs.org/en/download/archive/v24> and
<https://nodejs.org/en/blog/release/v24.19.0>. They are release contract
values, not floating "latest LTS" inputs.

Record the Node version in a checked-in version file and the
CI setup-node input. Record the npm version in `package.json`'s exact
`packageManager` field, and make CI fail before installation when
`node --version` or `npm --version` differs. Pin the setup actions by immutable
commit rather than a mutable major tag.

The production build graph is:

1. verify the approved exact Node and npm versions;
2. run `npm ci` in `console/`;
3. run `npm run build` in `console/`;
4. verify the emitted asset and fingerprint contract;
5. run `go build -tags groundplane_console` for `cmd/controller`; and
6. package only the resulting binary, without `console/dist/` or
   `node_modules/`.

`make controller` and `make build` enforce that order. The untagged command is
development-only and must be named and documented as such; release and package
automation never invokes raw `go build ./cmd/controller`.

`make ci` builds the Console first, then runs tagged `go vet`, tagged race
tests, and a tagged Controller build so a missing embed input is a CI failure.
It also runs golines over `./console/`. CLI and Agent builds do not depend on
Node. `make clean` removes the ignored Vite output. The build creates `bin/`
through an explicit Make prerequisite, writes the Controller there, and
`.gitignore` ignores `/bin/`. `delivery.md` and `standards.md` must make
`make ci` the one full local/CI gate rather than separately rebuilding the
Console afterward.

### 3. Routing and path validation

Registration order is not a precedence mechanism in `http.ServeMux`. The
outer Controller request handler owns dispatch before calling a mux:

- it validates the original decoded path and `URL.EscapedPath()` before
  `ServeMux` can clean or redirect it;
- it rejects dot segments, empty interior segments, repeated separators,
  encoded dot or separator variants, NUL, backslash, and any path that cannot
  map directly to one canonical `io/fs.ValidPath`;
- it never applies `path.Clean` and then accepts the cleaned result;
- exact `/api` and the `/api/` subtree are reserved for the API router;
- API and operational routes are dispatched before the static surface; and
- only a canonical path outside those namespaces can reach the static
  handler.

An unknown API path returns the existing `errs.KindRequestNotFound` RFC 7807
problem. A known API path with an unsupported method returns the existing
`errs.KindRequestMethodNotAllowed` problem. Exact `/api`, every `/api/` path,
and every method remain API-owned and can never receive SPA HTML or a plain
`net/http` error response.

### 4. Static HTTP behavior

The static handler operates only on the injected `fs.FS`. It does not call
`http.Dir`, `os.Open`, `http.FileServer`, or `http.ServeFile`. Its behavior is:

- only `GET` and `HEAD` are supported; other methods return
  `errs.KindRequestMethodNotAllowed`;
- only regular embedded files are served; directories are never listed;
- an exact regular file wins;
- a missing canonical path falls back to `index.html` only when the method is
  `GET` or `HEAD` and at least one syntactically valid `Accept` member is
  exactly `text/html` with a quality value greater than zero;
- absent `Accept`, wildcard-only Accept, malformed members, and
  `text/html;q=0` do not authorize SPA fallback;
- `/assets` and every path below `/assets/` are asset space and never receive
  SPA fallback;
- a missing path, invalid path, directory, or disallowed hidden path returns
  `errs.KindRequestNotFound`;
- `HEAD` returns the same status, content type, content length, cache policy,
  and variation headers as `GET`, without response bytes;
- every exact static response and SPA fallback sets
  `X-Content-Type-Options: nosniff`;
- every response whose representation depends on `Accept`, including a
  fallback 200 or resulting 404, sets `Vary: Accept`;
- `index.html` and other non-fingerprinted exact files use
  `Cache-Control: no-cache`;
- handler-generated 404 and 405 problems use `Cache-Control: no-store`; and
- content length is the embedded file length, not a host filesystem value.

Files under `/assets/` use
`Cache-Control: public, max-age=31536000, immutable` only when their emitted
filename satisfies the locked Vite fingerprint pattern. Vite configuration
must explicitly emit entry, chunk, and asset names with an eight-character
content hash, and CI must reject any regular `/assets/` output that lacks that
hash. `console/public/assets/` is prohibited because Vite would copy those
names without fingerprinting. A future change to either rule must update this
ADR before it can weaken cache correctness.

The closed, case-sensitive extension map is:

| Extension | Content-Type |
| --- | --- |
| `.avif` | `image/avif` |
| `.css` | `text/css; charset=utf-8` |
| `.gif` | `image/gif` |
| `.html` | `text/html; charset=utf-8` |
| `.ico` | `image/x-icon` |
| `.jpeg`, `.jpg` | `image/jpeg` |
| `.js`, `.mjs` | `text/javascript; charset=utf-8` |
| `.json`, `.map` | `application/json` |
| `.png` | `image/png` |
| `.svg` | `image/svg+xml` |
| `.txt` | `text/plain; charset=utf-8` |
| `.webp` | `image/webp` |
| `.woff` | `font/woff` |
| `.woff2` | `font/woff2` |

An unlisted extension uses `application/octet-stream`; the handler never asks
the host MIME database and never sniffs content.

### 5. Failure and release behavior

An untagged Controller continues to serve API routes for ordinary
clean-checkout Go development. Every otherwise valid Console request returns
an RFC 7807 problem built from the existing `errs.KindRequestUnavailable`,
with HTTP 503 and `Cache-Control: no-store`, explaining that the development
binary has no embedded Console build. Invalid paths and unsupported methods
retain their 404 and 405 precedence.

An untagged binary is not a production artifact. The compiler cannot prevent a
collaborator from running raw untagged `go build`; the supported release
pipeline prevents an assetless release by using only the tagged Make target
and by smoke-testing that artifact. A tagged build cannot compile when
`console/dist/` is absent.

The release smoke test builds the tagged binary from a fresh checkout, removes
or makes `console/dist/` unavailable after compilation, starts the resulting
binary with its hermetic test dependencies, and proves that it serves the
Console and API from the one executable. The Controller must not silently use
a disk directory, a development server, fixture HTML, a previously copied
tree, or a previously generated output tree.

## Alternatives considered

### Commit `console/dist/`

Rejected because generated Vite output is explicitly untracked and committed
bundles create review noise and stale-source risk.

### Copy the Vite output below `internal/` before every Go build

Rejected because it creates a second generated asset tree, needs another
ignore/cleanup contract, and can embed stale copied bytes independently of
`console/dist/`.

### Generate a Go source blob containing every asset

Rejected because it adds a custom generator and large generated source while
providing no benefit over direct embedding. If committed, it also recreates
the stale generated-artifact problem; if ignored, it still requires a build
tag or placeholder to keep clean-checkout Go tooling usable.

### Require Console assets for every Go command

Rejected because unrelated CLI, Agent, domain, and package tests would require
Node and a populated ignored directory. Only production Controller builds and
the full delivery gate need the tagged embed.

## Consequences

- the production Controller contains the exact Vite output and has no runtime
  asset dependency;
- clean-checkout Go work remains possible without Node;
- release and CI commands become the enforceable boundary between development
  and production Controller builds;
- the build tag is part of the delivery contract and cannot be renamed or
  omitted by downstream packaging; and
- accepting this ADR closes the packaging decisions needed to implement the
  static-serving portion of C22, but does not by itself complete C22.

Acceptance requires synchronized follow-on changes, not documentation-only
approval:

- add the tagged Console asset package plus tagged and untagged application
  wiring without changing the thin Controller entry point;
- add the outer canonical-path and API-namespace dispatcher before
  `ServeMux` normalization;
- implement the reusable `fs.FS` static handler and its exact RFC 7807,
  negotiation, MIME, cache, and security-header behavior;
- pin the owner-approved Node and npm versions and immutable CI actions;
- lock and verify Vite's fingerprinted asset naming and prohibit
  `console/public/assets/`;
- update the Makefile, CI, `.gitignore`, `architecture.md`, `standards.md`,
  `delivery.md`, and agent setup instructions as defined above; and
- keep `console/dist/` ignored and absent from commits.

## Acceptance evidence

After acceptance and implementation, focused tests must prove:

- exact `/api`, unknown `/api/` paths, known API paths with wrong methods, and
  operational routes never receive SPA HTML and retain RFC 7807 responses;
- validation before `ServeMux` returns 404 for decoded and encoded dot
  segments, separators, repeated separators, hidden elements, backslashes,
  NUL, and non-canonical paths without redirecting or cleaning them into a
  valid asset;
- exact regular files, rejected directories, client-side fallback, and
  missing `/assets` paths behave as specified;
- `GET` and `HEAD` agree on status and headers, `HEAD` emits no body, and
  unsupported methods use `errs.KindRequestMethodNotAllowed`;
- absent, wildcard, malformed, zero-quality, and positive `text/html` Accept
  cases produce the exact fallback and `Vary: Accept` behavior;
- every closed MIME-map entry and the octet-stream fallback are deterministic
  and every static response has `X-Content-Type-Options: nosniff`;
- index, non-fingerprinted files, fingerprinted assets, 404, 405, and untagged
  503 responses carry their exact cache policies;
- CI rejects an unhashed `/assets/` output and any `console/public/assets/`
  input;
- untagged Go development does not import the excluded Console package and
  returns `errs.KindRequestUnavailable` for valid Console requests;
- the approved exact Node and npm versions are checked before `npm ci`;
- `make ci` builds and tests the tagged Controller from a fresh checkout,
  formats and checks the Console Go package, and leaves no untracked build
  output; and
- the final tagged Controller binary serves the Console after
  `console/dist/` is made unavailable, proving that no disk fallback exists.

## Accepted implementation choices

The accepted implementation uses the `groundplane_console` release build tag,
Node `24.19.0`, npm `11.17.0`, and the checked-in, content-addressed `npm ci`
path as the production Console build contract.
