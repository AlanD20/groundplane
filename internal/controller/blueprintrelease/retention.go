package blueprintrelease

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (service *Service) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	if view.Item.Item.Input == nil {
		retired, err := service.ledger.RetireUnpublishedHookStage(ctx, view)
		if err != nil || !retired {
			return false, workqueue.Change{}, err
		}
		retired, err = service.ledger.RetireUnpublishedRollout(ctx, view)
		if err != nil || !retired {
			return false, workqueue.Change{}, err
		}
		change, err := service.ledger.PrepareUnpublishedReleaseRetirement(ctx, view, at)
		return err == nil, change, err
	}
	sources, ready, guards, err := service.retireCapturedSources(ctx, view, at)
	if err != nil || !ready {
		return false, workqueue.Change{}, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	if !final {
		return true, guards, nil
	}
	// Keep the immutable input until the last attempt is collected. These keys
	// belong to execution, unlike the published Release and applied runtime.
	guards.Mutations = append(guards.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: etcd.ReleaseWorkInputKey(view.Item.Item.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: releaserender.ServiceRolloutKey(view.Task.Task.Scope.EnvironmentID, view.Task.Task.ID, view.Item.Item.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: etcd.BlueprintProxyServiceRolloutKey(view.Task.Task.Scope.EnvironmentID, view.Task.Task.ID, view.Item.Item.ID)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: releasework.UnstartedKey(view.Item.Item.ID) + "/", Prefix: true},
	)
	for _, hook := range sources.Rollout.Record.Render.Hooks {
		key := scriptexecutions.ScriptExecutionKey(hook.ScriptExecutionID)
		guards.Mutations = append(guards.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key + "/checkpoints/", Prefix: true},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: scriptexecutions.ScriptRunnerSnapshotKey(hook.RunnerSnapshotID)},
		)
	}
	return true, guards, nil
}

func (service *Service) PrepareAttemptRetirement(view workqueue.View, attempt workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return releasework.PrepareAttemptRetirement(view, attempt)
}
