package etcd

import (
	"context"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type groupRestorationState struct {
	runtime    serviceruntimerecord.Record
	projection domain.ServiceProjection
	conditions []keyvalue.Condition
}

// Group restoration owns the successfully applied member, not the predecessor
// revision used by failed-forward cleanup. Its resource heads must still name
// the exact source attempt; a matching image or Service name is insufficient.
func (ledger *ReleaseLedger) groupRestorationState(ctx context.Context, sources GroupRestorationSources, revision int64) (groupRestorationState, error) {
	selected := sources.Restoration.Input
	serviceID := selected.Preparation.ServiceID
	environmentID := sources.Release.Rollout.Record.Scope.EnvironmentID
	keys := []string{serviceruntimerecord.Key(serviceID), releases.ReleaseProjectionKey(serviceID), runtimeconfiguration.HeadKey(environmentID)}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return groupRestorationState{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return groupRestorationState{}, errs.New(errs.KindStateConflict, "Group restoration applied-state read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision {
			return groupRestorationState{}, errs.New(errs.KindStateConflict, "Group restoration lost its acknowledged source runtime or configuration")
		}
	}
	runtime, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](read.Values[0].Value, "service-acknowledged-runtime")
	if err != nil || serviceruntimerecord.Validate(runtime) != nil {
		return groupRestorationState{}, errs.New(errs.KindStateConflict, "Group restoration source runtime is invalid")
	}
	projection, err := decodeReleaseProjection(read.Values[1], environmentID, serviceID)
	if err != nil {
		return groupRestorationState{}, err
	}
	configuration, err := runtimeconfiguration.DecodeReference(read.Values[2].Value)
	if err != nil {
		return groupRestorationState{}, err
	}
	source, candidate := selected.Source, sources.Release.Rollout.Record.Render.ReleaseID
	ack := runtime.Source
	if runtime.EnvironmentID != environmentID || runtime.Runtime.ServiceID != serviceID || runtime.Runtime.ReleaseID != candidate ||
		ack.TaskID != source.TaskID || ack.WorkItemID != source.WorkItemID || ack.AttemptID != source.ID || ack.Executor != source.Executor ||
		ack.Input != source.Input || source.Result == nil || ack.Checkpoint.Evidence != *source.Result ||
		ack.RenderGeneration != sources.Release.Rollout.Record.Render.Projection.RenderGeneration ||
		runtime.Configuration == nil || *runtime.Configuration != sources.Release.Input.Configuration.Current ||
		configuration != sources.Release.Input.Configuration.Current || projection.ServingReleaseID != candidate ||
		projection.CurrentSuccessfulReleaseID != candidate || projection.ActiveOperationID != "" {
		return groupRestorationState{}, errs.New(errs.KindStateConflict, "Group restoration source is no longer the exact acknowledged member")
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision}
	}
	return groupRestorationState{runtime: runtime, projection: projection, conditions: conditions}, nil
}

func (ledger *ReleaseLedger) GroupRestorationConditions(ctx context.Context, view workqueue.View) ([]keyvalue.Condition, error) {
	sources, err := ledger.LoadGroupRestorationSources(ctx, view)
	if err != nil {
		return nil, err
	}
	defer sources.Clear()
	state, err := ledger.groupRestorationState(ctx, sources, view.ReadRevision)
	if err != nil {
		return nil, err
	}
	return resourcePublicationConditions(sources.Conditions, state.conditions)
}
