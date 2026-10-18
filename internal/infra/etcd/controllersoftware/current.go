package controllersoftware

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) PrepareCurrent(ctx context.Context, capture keyvalue.Versioned[Capture], revision int64, started bool,
	outcome workexecution.Outcome, settled bool,
) (workqueue.Change, error) {
	current, found, err := repository.Current(ctx, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: LatestKey, ModRevision: current.Revision}}}
	if !found || current.Record.Capture.WorkItemID != capture.Record.WorkItemID {
		return change, nil
	}
	reference, err := capture.Record.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	stored, err := current.Record.Capture.Reference()
	if err != nil || stored != reference {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Controller software projection changed captured input")
	}
	if outcome == "" {
		outcome = current.Record.Outcome
	}
	value := Current{Capture: capture.Record, Started: started || current.Record.Started, Outcome: outcome, Settled: settled}
	raw, err := EncodeCurrent(value)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: LatestKey, Value: raw})
	return change, nil
}
