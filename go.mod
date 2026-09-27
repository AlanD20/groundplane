module github.com/AlanD20/groundplane

go 1.26

replace github.com/AlanD20/groundplane-component-sdk => ./component-sdk

replace github.com/AlanD20/groundplane-registered-components => ./registered-components

require (
	github.com/AlanD20/groundplane-component-sdk v0.0.0
	github.com/AlanD20/groundplane-registered-components v0.0.0
	github.com/aws/aws-sdk-go-v2 v1.43.7
	github.com/aws/aws-sdk-go-v2/service/s3 v1.107.3
	github.com/containerd/errdefs v1.0.0
	github.com/coredns/caddy v1.1.1
	github.com/danielgtaylor/huma/v2 v2.39.1
	github.com/distribution/reference v0.6.0
	github.com/gowebpki/jcs v1.0.1
	github.com/jedib0t/go-pretty/v6 v6.5.9
	github.com/moby/moby/api v1.55.0
	github.com/moby/moby/client v0.5.1
	github.com/oapi-codegen/runtime v1.2.0
	github.com/oklog/ulid/v2 v2.1.0
	github.com/opencontainers/go-digest v1.0.0
	github.com/opencontainers/image-spec v1.1.1
	github.com/spf13/cobra v1.10.2
	go.etcd.io/etcd/api/v3 v3.6.13
	go.etcd.io/etcd/client/v3 v3.6.13
	golang.org/x/net v0.57.0
	google.golang.org/grpc v1.79.3
	google.golang.org/protobuf v1.36.11
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/hpke v0.4.0 // indirect
	github.com/BurntSushi/toml v1.4.1-0.20240526193622-a339e1f7089c // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20231202071711-9a357b53e9c9 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.18 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.38 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.38 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.39 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.31 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.38 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.39 // indirect
	github.com/aws/smithy-go v1.27.8 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/dave/dst v0.27.3 // indirect
	github.com/docker/go-connections v0.7.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dprotaso/go-yit v0.0.0-20220510233725-9ba8df137936 // indirect
	github.com/fatih/structtag v1.2.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/getkin/kin-openapi v0.142.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.23.1 // indirect
	github.com/go-openapi/swag/jsonname v0.26.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.3 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.23 // indirect
	github.com/mattn/go-shellwords v1.0.12 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/oapi-codegen/oapi-codegen/v2 v2.8.0 // indirect
	github.com/oasdiff/yaml v0.1.1 // indirect
	github.com/oasdiff/yaml3 v0.0.14 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	github.com/segmentio/golines v0.12.2 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/speakeasy-api/jsonpath v0.6.3 // indirect
	github.com/speakeasy-api/openapi v1.24.0 // indirect
	github.com/vmware-labs/yaml-jsonpath v0.3.2 // indirect
	github.com/x-cray/logrus-prefixed-formatter v0.5.2 // indirect
	github.com/xhit/go-str2duration/v2 v2.1.0 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.6.13 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0 // indirect
	go.opentelemetry.io/otel v1.39.0 // indirect
	go.opentelemetry.io/otel/metric v1.39.0 // indirect
	go.opentelemetry.io/otel/trace v1.39.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	go.yaml.in/yaml/v4 v4.0.0-rc.4 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/term v0.45.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.5.1 // indirect
	gopkg.in/alecthomas/kingpin.v2 v2.2.6 // indirect
	honnef.co/go/tools v0.7.0 // indirect
)

require (
	filippo.io/age v1.3.1
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/compose-spec/compose-go/v2 v2.14.0
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mattn/go-runewidth v0.0.24 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	golang.org/x/sys v0.47.0
)

tool (
	github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen
	github.com/segmentio/golines
	google.golang.org/grpc/cmd/protoc-gen-go-grpc
	google.golang.org/protobuf/cmd/protoc-gen-go
	honnef.co/go/tools/cmd/staticcheck
)
