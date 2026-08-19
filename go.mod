module github.com/AlanD20/groundplane

go 1.26

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
