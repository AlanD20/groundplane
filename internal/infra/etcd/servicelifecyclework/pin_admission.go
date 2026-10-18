package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

func (repository *Repository) hookPinGuard(ctx context.Context, input Input, revision int64) ([]keyvalue.Condition, error) {
	if input.HookInputs == nil || input.HookInputs.SecretPins == nil {
		return nil, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, input.Scope.ProjectID)
	if err != nil {
		return nil, err
	}
	root, found, err := pins.LoadActive(ctx, input.HookInputs.OperationID, revision)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, conflict("Service lifecycle no longer holds its captured Secret values")
	}
	guard, err := root.AdmissionCondition(*input.HookInputs.SecretPins)
	if err != nil {
		return nil, err
	}
	return []keyvalue.Condition{{Key: guard.Key, ModRevision: guard.ModRevision}}, nil
}
