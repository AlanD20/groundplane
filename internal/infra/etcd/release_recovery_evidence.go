package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (ledger *ReleaseLedger) RecoveryEffectConditions(
	ctx context.Context,
	view workqueue.ResumeView,
	input releaseexecution.RecoveryInput,
	previous releasework.RecoverySnapshot,
	report releaseexecution.RecoveryReport,
) (workqueue.Change, error) {
	conditions, err := releasework.RecoveryEffectCompletionConditions(ctx, ledger.store, view, input, previous, report)
	return workqueue.Change{Conditions: conditions}, err
}
