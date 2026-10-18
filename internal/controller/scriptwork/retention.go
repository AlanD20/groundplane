package scriptwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	if view.Item.Item.Input == nil {
		return service.RetireUnpreparedWork(ctx, view, at)
	}
	ready, guards, err := service.repository.RetireStandaloneScriptSources(ctx, view, at)
	if err != nil || !ready || !final {
		return ready, guards, err
	}
	input, err := service.repository.LoadScriptWork(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer input.Execution.Clear()
	guards.Conditions = append(guards.Conditions, input.Conditions...)
	key := scriptexecutions.ScriptExecutionKey(input.Execution.ID)
	guards.Mutations = append(guards.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: scriptexecutions.WorkKey(view.Item.Item.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: scriptexecutions.UnstartedKey(view.Item.Item.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key + "/checkpoints/", Prefix: true},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key + "/interruptions/", Prefix: true},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key + "/recoveries/", Prefix: true},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: scriptexecutions.ScriptRunnerSnapshotKey(input.Execution.SnapshotID)},
	)
	return true, guards, nil
}

func (service *Service) RetireUnpreparedWork(ctx context.Context, view workqueue.View, at time.Time) (bool, workqueue.Change, error) {
	return service.repository.RetireUnpreparedGroupPreflight(ctx, view, at)
}

// Script checkpoint evidence stays with its input until the last attempt is
// collected. The shared collector removes each attempt's cursor/diagnostics.
func (service *Service) PrepareAttemptRetirement(view workqueue.View, attempt workqueue.AttemptSnapshot) (workqueue.Change, error) {
	if view.Item.Item.Operation != workexecution.Script {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script retention received another operation")
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, attempt)
	keyvalue.ClearMutationValues(prepared.Mutations)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{}, nil
}
