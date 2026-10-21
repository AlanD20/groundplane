package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// ReadReleaseIntentMatch proves the same current-intent boundary used by
// forward admission. Its comparisons must join either the grant or revocation;
// changed intent never authorizes releasing an active attempt's effect claims.
func (ledger *ReleaseLedger) ReadReleaseIntentMatch(ctx context.Context,
	view workqueue.View,
) (bool, []keyvalue.Condition, error) {
	task := view.Task.Task
	if task.Action == workexecution.ActionDeploy || task.Action == workexecution.ActionRollback {
		captured, _, err := ledger.ReadDirectReleasePreparation(ctx, view)
		if err != nil {
			return false, nil, err
		}
		return ledger.MatchDirectReleaseSource(ctx, view, captured)
	}
	units, err := blueprintunits.NewRepository(ledger.store)
	if err != nil {
		return false, nil, err
	}
	return units.ReadWorkIntentMatch(ctx, view)
}
