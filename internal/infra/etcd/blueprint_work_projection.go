package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintWorkProjectionConditions binds the actual effective input as well as
// its authored root. The projection reader's Revision identifies the root, not
// the effective projection, which may have been published later by dependencies.
func (ledger *ReleaseLedger) BlueprintWorkProjectionConditions(ctx context.Context,
	work workqueue.View, expected environmentprojection.EnvironmentComposeProjection,
) ([]keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, err
	}
	task, item := work.Task.Task, work.Item.Item
	if ledger == nil || ledger.store == nil || task.Validate() != nil || item.Validate() != nil ||
		task.Action != workexecution.ActionApply || item.TaskID != task.ID ||
		item.Operation != workexecution.Release || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		work.ReadRevision < work.Task.Revision || work.ReadRevision < work.Item.Revision ||
		expected.EnvironmentID != task.Scope.EnvironmentID || expected.RevisionID != task.ID {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint projection requires its captured Release work")
	}
	encoded, err := environmentprojection.EncodeEnvironmentComposeProjectionStorage(expected)
	if err != nil {
		return nil, err
	}
	defer clear(encoded)
	keys := []string{
		blueprints.EnvironmentBlueprintRootKey(expected.EnvironmentID, task.ID),
		blueprints.EnvironmentBlueprintEffectiveProjectionKey(expected.EnvironmentID, task.ID),
	}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: work.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != len(keys) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint runtime input snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		value := read.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint runtime input is not available at the work snapshot")
		}
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: value.ModRevision}
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[0].Value)
	if err != nil || seal.EnvironmentID != expected.EnvironmentID || seal.RevisionID != task.ID ||
		seal.RenderGeneration != expected.RenderGeneration || !bytes.Equal(encoded, read.Values[1].Value) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint runtime input differs from the captured work snapshot")
	}
	return conditions, nil
}
