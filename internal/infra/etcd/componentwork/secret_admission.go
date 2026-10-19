package componentwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// ExecutionSecretConditions protects both forward execution and restoration.
// Reading retained failure evidence does not require live pins; granting a new
// effect or delivering captured Secret-backed files does.
func (repository *InputRepository) ExecutionSecretConditions(
	ctx context.Context,
	view workqueue.View,
) ([]keyvalue.Condition, error) {
	_, capture, guards, err := repository.Load(ctx, view)
	if err != nil {
		return nil, err
	}
	if capture.Secrets == nil {
		return guards, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, view.Task.Task.Scope.ProjectID)
	if err != nil {
		return nil, err
	}
	root, found, err := pins.LoadActive(ctx, capture.Secrets.OperationID, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, conflict("Component execution no longer holds its captured Secret values")
	}
	condition, err := root.AdmissionCondition(*capture.Secrets)
	if err != nil {
		return nil, err
	}
	return append(guards, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision}), nil
}
