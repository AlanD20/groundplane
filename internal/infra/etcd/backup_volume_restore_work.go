package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PublishWork consumes the captured resource admission once and publishes native
// execution in the same transaction. Retry resumes work; it is not republication.
func (publication *PreparedVolumeRestorePublication) PublishWork(ctx context.Context,
	task workexecution.Task, item workexecution.Item, input *agentpb.BackupWorkInput,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if publication == nil || publication.state == nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Restore publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	if state.repository == nil {
		state.mu.Unlock()
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Restore publication is consumed or is not a new action",
		)
	}
	repository := state.repository
	captured := restoreWorkPublication{hostID: state.hostID, owner: state.owner, restore: state.restore,
		scope: state.scope, authority: state.authority, conditions: state.conditions, mutations: state.mutations, artifacts: state.artifacts}
	state.repository, state.scope, state.authority = nil, nil, nil
	state.conditions, state.mutations = nil, nil
	state.artifacts = nil
	state.mu.Unlock()
	return repository.publishRestoreWork(ctx, captured, task, item, input, marker)
}
