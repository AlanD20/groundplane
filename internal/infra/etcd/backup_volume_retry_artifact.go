package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *BackupRuntimeRepository) backupRetryVolumeArtifacts(ctx context.Context,
	source backupRunRetrySource, expected etcdstore.Condition,
) ([]*agentpb.ComposeArtifact, error) {
	read, err := repository.ReadFixedKeys(ctx, []string{expected.Key}, source.task.ReadRevision)
	if err != nil {
		return nil, err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].ModRevision != expected.ModRevision {
		return nil, errs.New(errs.KindStateConflict, "backup retry source procedure changed")
	}
	plan, err := backupruntime.DecodeBackupExecutionPlan(read.Values[0].Value)
	if err != nil || plan.PlanId != source.task.Record.PlanID ||
		hex.EncodeToString(plan.PlanHash) != source.task.Record.PlanHash {
		return nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	artifacts := make([]*agentpb.ComposeArtifact, len(plan.Artifacts))
	for index, artifact := range plan.Artifacts {
		artifacts[index] = proto.CloneOf(artifact)
	}
	return artifacts, nil
}
