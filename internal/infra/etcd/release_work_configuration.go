package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareReleaseWorkConfiguration stages immutable file sources without
// acknowledging them or authorizing file writes. The Release owner includes
// the returned capture in its complete input and compares the prior head during
// publication and admission while holding the native Environment claims.
func (ledger *ReleaseLedger) PrepareReleaseWorkConfiguration(ctx context.Context,
	work workqueue.View, configurationID string, generation uint64, changes []taskmaterialization.Record,
) (runtimeconfiguration.WorkCapture, error) {
	if ledger == nil || ledger.store == nil || work.Item.Item.Operation != workexecution.Release {
		return runtimeconfiguration.WorkCapture{}, errs.New(errs.KindValidationFailed, "Release configuration requires native Release work")
	}
	sources, err := runtimeconfiguration.New(ledger.store)
	if err != nil {
		return runtimeconfiguration.WorkCapture{}, err
	}
	return sources.CaptureWork(ctx, work.Task.Task, work.Item.Item, work.ReadRevision, configurationID, generation, changes)
}

// ReleaseWorkConfigurationConditions joins source and predecessor compares to
// native admission. Secret pin compares remain owned by the Release input owner.
func (ledger *ReleaseLedger) ReleaseWorkConfigurationConditions(ctx context.Context,
	work workqueue.View, capture runtimeconfiguration.WorkCapture,
) ([]keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, err
	}
	if ledger == nil || ledger.store == nil || work.Item.Item.Operation != workexecution.Release {
		return nil, errs.New(errs.KindValidationFailed, "configuration admission requires Release work")
	}
	if err := capture.Matches(work.Task.Task, work.Item.Item); err != nil {
		return nil, err
	}
	sources, err := runtimeconfiguration.New(ledger.store)
	if err != nil {
		return nil, err
	}
	captured, err := sources.WorkConditions(ctx, capture, work.ReadRevision)
	if err != nil {
		return nil, err
	}
	conditions := make([]keyvalue.Condition, len(captured))
	for index, condition := range captured {
		conditions[index] = keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision}
	}
	return conditions, nil
}
