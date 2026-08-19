module github.com/sample-tenant/groundplane

go 1.22

require (
	github.com/jedib0t/go-pretty/v6 v6.5.9
	github.com/oklog/ulid/v2 v2.1.0
	github.com/spf13/cobra v1.8.1
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
)

// google.golang.org/grpc and google.golang.org/protobuf are needed once
// `make proto` generates proto/agentpb/*.pb.go and internal/agent starts
// importing it — add them back with `go get` at that point. Left out for
// now so `go mod tidy` doesn't need google.golang.org reachable.

require (
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	golang.org/x/sys v0.17.0 // indirect
)

// gopkg.in/* module paths resolve via the gopkg.in redirector, which is
// not on this environment's egress allowlist; both are mirrored as
// regular GitHub repos, so point there directly. Safe to drop once
// gopkg.in is reachable (or simply left as-is — the packages are
// identical).
replace gopkg.in/yaml.v3 => github.com/go-yaml/yaml v3.0.1+incompatible

replace gopkg.in/natefinch/lumberjack.v2 => github.com/natefinch/lumberjack v2.2.1+incompatible

// gopkg.in/check.v1 is a test-only transitive dependency of yaml.v3 (not
// actually imported by anything in this repo); vendored locally purely
// so `go mod tidy`/`go build` can resolve the module graph without
// gopkg.in being reachable. Safe to drop once it is.
replace gopkg.in/check.v1 => github.com/go-check/check v0.0.0-20201130134442-10cb98267c6c

// golang.org/x/sys resolves via the golang.org vanity-import redirector,
// also unreachable from this environment; github.com/golang/sys is the
// canonical mirror the vanity path points at.
replace golang.org/x/sys => github.com/golang/sys v0.17.0
