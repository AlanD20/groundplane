package etcd

import (
	"context"
	"net/netip"
	"time"

	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconcileResolverListener publishes an ordinary sealed resolver Task when
// machine configuration differs from the last acknowledged listener. It neither
// edits desired Component settings nor rewrites a running Task's inputs.
func (repository *TaskRepository) ReconcileResolverListener(ctx context.Context, listener string) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if listener != "" {
		address, err := netip.ParseAddr(listener)
		if err != nil || !address.Is4() || !address.IsPrivate() || address.String() != listener {
			return errs.New(errs.KindValidationFailed, "resolver listener must be canonical private IPv4")
		}
	}
	projectionRead, err := repository.store.Get(ctx, resolutionrecord.StorageKey)
	if err != nil {
		return err
	}
	if projectionRead == nil || projectionRead.Entry == nil {
		return nil // Clean-start publication owns the initial resolver Task.
	}
	resolver, err := repository.platformResolverAtRevision(ctx, projectionRead.ReadRevision)
	if err != nil {
		return err
	}
	if !resolver.Record.Desired.Enabled {
		return nil
	}
	componentID := resolver.Record.Desired.ID
	requestKey := "/v1/indexes/platform-resolver-listener-attempt/" + componentID
	keys := []string{
		platformComponentTaskActiveKey(componentID),
		platformcomponents.ComponentObservationKey(componentID),
		requestKey,
	}
	state, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: keys, Revision: projectionRead.ReadRevision},
	)
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != len(keys) {
		return errs.New(errs.KindInternal, "resolver listener reconciliation read is incomplete")
	}
	if state.Values[0] != nil || state.Values[1] == nil {
		return nil // Wait for active work; failed bootstrap requires explicit Retry.
	}
	observation, err := platformcomponents.DecodeComponentObservation(state.Values[1].Value)
	if err != nil {
		return err
	}
	if observation.ComponentID != componentID {
		return errs.New(errs.KindInternal, "resolver listener observation belongs to another Component")
	}
	acknowledged, err := repository.GetTask(ctx, observation.TaskID)
	if err != nil {
		return err
	}
	input, err := platformcomponents.ReadAcknowledgedInput(
		ctx,
		repository.store,
		observation,
		acknowledged.Record.PlanID,
		state.ReadRevision,
	)
	if err != nil {
		return err
	}
	conditions := []etcdstore.Condition{
		{Key: componentrecord.RecordKey(componentID), ModRevision: resolver.Revision},
		{Key: resolutionrecord.StorageKey, ModRevision: projectionRead.Entry.ModRevision},
		{Key: keys[0]}, {Key: keys[1], ModRevision: state.Values[1].ModRevision},
		{Key: requestKey, ModRevision: etcdstore.RevisionOf(state.Values[2])},
		{Key: taskjournal.TaskStorageKey(observation.TaskID), ModRevision: acknowledged.Revision},
		{Key: platformcomponents.PlatformComponentTaskRenderInputKey(input.Record.PlanID), ModRevision: input.Revision},
	}
	if input.Record.PrivateListener == listener {
		if state.Values[2] == nil {
			return nil
		}
		// A return to applied configuration resets a previous failed request.
		result, err := repository.store.Transact(
			ctx,
			conditions,
			[]etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: requestKey}},
		)
		return resolverListenerTransaction(result, err)
	}
	// One automatic attempt per requested listener and acknowledged predecessor.
	// A failed Task stays available for explicit Retry instead of looping forever.
	request := listener + "\n" + observation.TaskID
	if state.Values[2] != nil && string(state.Values[2].Value) == request {
		return nil
	}
	projection, err := resolutionrecord.DecodeHostResolutionProjectionRecord(projectionRead.Entry.Value)
	if err != nil {
		return err
	}
	task := newPlatformDNSResolverTask(componentID, time.Now().UTC())
	change, err := repository.preparePlatformDNSResolverTaskContribution(
		ctx,
		resolver,
		projection,
		task,
		nil,
		nil,
		nil,
		conditions,
	)
	if err != nil {
		return err
	}
	defer clearHostResolutionReconciliationChange(change)
	if !change.applies {
		return errs.New(errs.KindStateConflict, "resolver listener change produced no Task")
	}
	change.mutations = append(
		change.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: requestKey, Value: []byte(request)},
	)
	result, err := repository.store.Transact(ctx, change.conditions, change.mutations)
	return resolverListenerTransaction(result, err)
}

func resolverListenerTransaction(result etcdstore.TransactionResult, err error) error {
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "resolver listener state changed during publication")
	}
	return nil
}
