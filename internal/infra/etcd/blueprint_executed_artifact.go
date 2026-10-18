package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *TaskRepository) blueprintAcknowledgedArtifact(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) ([]byte, etcdstore.Condition, error) {
	key := releases.ReleasePublicationKey(task.Params[releaserender.TaskReleasePublicationParam])
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, etcdstore.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, etcdstore.Condition{}, releases.CorruptReleaseRecord()
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](
		read.Values[0].Value,
		"release-publication",
	)
	if err != nil || marker.OperationID != task.OperationID ||
		marker.PublicationID != task.Params[releaserender.TaskReleasePublicationParam] ||
		marker.CandidateReleaseDescriptor.PlanID != task.PlanID ||
		hex.EncodeToString(marker.CandidateReleaseDescriptor.PlanHash) != task.PlanHash {
		return nil, etcdstore.Condition{}, releases.CorruptReleaseRecord()
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(marker.ExecutedComposeArtifact, artifact); err != nil ||
		artifact.GetArtifactId() != task.Params[taskjournal.TaskComposeArtifactParam] || artifact.GetOwnerId() != task.Owner.EnvironmentID ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
		return nil, etcdstore.Condition{}, releases.CorruptReleaseRecord()
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, marker.ExecutedComposeArtifact) {
		return nil, etcdstore.Condition{}, releases.CorruptReleaseRecord()
	}
	return canonical, etcdstore.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
