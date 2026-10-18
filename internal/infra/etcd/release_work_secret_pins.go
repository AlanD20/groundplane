package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareReleaseWorkSecretPins retains only the candidate and prior file sources
// touched by this work. The caller binds Prepared.Binding into its complete input
// and joins Activation to publication. It does not create a second Task or select
// newer Secret values when a captured value is unavailable.
func (ledger *ReleaseLedger) PrepareReleaseWorkSecretPins(ctx context.Context,
	work workqueue.View, operationID string, capture runtimeconfiguration.WorkCapture,
) (tasksecretpins.Prepared, error) {
	if ledger == nil || work.Item.Item.Operation != workexecution.Release {
		return tasksecretpins.Prepared{}, errs.New(errs.KindValidationFailed, "Secret retention requires native Release work")
	}
	return prepareConfigurationWorkSecretPins(ctx, ledger.store, work, operationID, capture)
}

// AbandonReleaseWorkSecretPins discards only unactivated preparation. Its root
// fence preserves pins if final publication succeeded but its response was lost.
func (ledger *ReleaseLedger) AbandonReleaseWorkSecretPins(ctx context.Context,
	work workqueue.View, prepared tasksecretpins.Prepared,
) error {
	if prepared.IsZero() {
		return nil
	}
	if ctx == nil || ledger == nil || ledger.store == nil ||
		work.Task.Task.Validate() != nil || work.Item.Item.Validate() != nil ||
		work.Item.Item.TaskID != work.Task.Task.ID || work.Item.Item.Operation != workexecution.Release ||
		prepared.TaskID() != work.Task.Task.ID || prepared.WorkItemID() != work.Item.Item.ID {
		return errs.New(errs.KindValidationFailed, "Secret preparation cleanup belongs to another Release work item")
	}
	repository, err := tasksecretpins.NewEtcdRepository(ledger.store, work.Task.Task.Scope.ProjectID)
	if err != nil {
		return err
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return repository.Abandon(cleanup, prepared.OperationID())
}
