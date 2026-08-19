module github.com/AlanD20/groundplane

go 1.26

require (
	github.com/danielgtaylor/huma/v2 v2.39.1
	github.com/jedib0t/go-pretty/v6 v6.5.9
	github.com/oklog/ulid/v2 v2.1.0
	github.com/spf13/cobra v1.10.2
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/hpke v0.4.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
)

// google.golang.org/grpc and google.golang.org/protobuf are needed once
// `make proto` generates proto/agentpb/*.pb.go and internal/agent starts
// importing it — add them back with `go get` at that point. Left out for
// now so `go mod tidy` doesn't need google.golang.org reachable.

require (
	filippo.io/age v1.3.1
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.24 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	golang.org/x/sys v0.47.0 // indirect
)
