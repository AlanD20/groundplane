package backingpostgresruntime

import (
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// The database and its backup authority advance in the same verified terminal
// transaction. Ordinary Services do not consume the Backing record.
type RuntimeReader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func PrepareAcknowledgement(ctx context.Context, storage RuntimeReader,
	runtimeValue []byte, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	runtime, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](
		runtimeValue,
		"service-acknowledged-runtime",
	)
	if err != nil || serviceruntimerecord.Validate(runtime) != nil {
		return nil, nil, releases.CorruptReleaseRecord()
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(runtime.Runtime.CurrentArtifact, artifact) != nil {
		return nil, nil, releases.CorruptReleaseRecord()
	}
	managed := false
	for _, workload := range artifact.Services {
		managed = managed || workload.PostgresToolsImage != ""
	}
	if !managed {
		return nil, nil, nil
	}
	key := Key(runtime.EnvironmentID, runtime.Runtime.ServiceID)
	read, err := storage.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, releases.CorruptReleaseRecord()
	}
	defer etcdstore.ClearValues(read.Values)
	prior, err := Decode(read.Values[0].Value)
	if err != nil {
		return nil, nil, err
	}
	next, err := FromDeployment(prior, runtime)
	if err != nil {
		return nil, nil, err
	}
	encoded, err := Encode(next)
	if err != nil {
		return nil, nil, err
	}
	return []etcdstore.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: encoded}}, nil
}
