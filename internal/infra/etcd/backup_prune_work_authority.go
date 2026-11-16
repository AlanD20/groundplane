package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (publication *PreparedBackupPrunePublication) PrepareWorkAuthority(ctx context.Context,
	task workexecution.Task, item workexecution.Item, input *agentpb.BackupWorkInput,
) (workqueue.Change, error) {
	if publication == nil || publication.state == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Point removal publication is not prepared")
	}
	state := publication.state
	state.mu.Lock()
	if state.repository == nil {
		state.mu.Unlock()
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Point removal publication was consumed")
	}
	plan, scope, owner, dispatch, hostID := state.plan, state.scope, state.owner, state.dispatch, state.hostID
	state.repository, state.plan, state.scope = nil, backupPruneTransactionPlan{}, nil
	state.mu.Unlock()
	defer plan.clear()
	if !backupwork.PruneSettlementFits(input) {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Point removal exceeds its completion budget")
	}
	return preparePruneWorkAuthority(plan, scope, owner, dispatch, hostID, task, item, input)
}
