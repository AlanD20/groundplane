package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) releaseRecoveryProcedureAtRevision(
	ctx context.Context, task TaskRecord, revision int64,
) (*agentpb.CandidateReleaseProcedure, []keyvalue.Condition, error) {
	if authoredBlueprintServiceRemovalTask(task) {
		procedure, _, conditions, err := repository.authoredServiceRecoveryProcedureAtRevision(ctx, task, revision)
		return procedure, conditions, err
	}
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, revision)
	return procedure, nil, err
}
