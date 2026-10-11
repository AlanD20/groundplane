package etcd

import (
	"context"
	"net/netip"
	"slices"
	"time"

	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconcileResolverConfiguration uses the existing sealed resolver Task path for
// listener and Environment DNS changes. It never edits an in-flight Task.
func (repository *TaskRepository) ReconcileResolverConfiguration(
	ctx context.Context,
	listener string,
	zones platformcomponents.ZoneDNSListenerSelection,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := zones.Validate(); err != nil {
		return err
	}
	if listener != "" {
		address, err := netip.ParseAddr(listener)
		if err != nil || !address.Is4() || !address.IsPrivate() || address.String() != listener {
			return errs.New(errs.KindValidationFailed, "resolver listener must be canonical private IPv4")
		}
	}
	projectionState, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{resolutionrecord.StorageKey}, Revision: zones.ReadRevision,
	})
	if err != nil {
		return err
	}
	if projectionState == nil || projectionState.ReadRevision != zones.ReadRevision ||
		len(projectionState.Values) != 1 {
		return errs.New(errs.KindInternal, "resolver configuration projection read is incomplete")
	}
	projectionRead := etcdstore.GetResult{Entry: projectionState.Values[0], ReadRevision: projectionState.ReadRevision}
	if projectionRead.Entry == nil {
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
	requestKey := platformcomponents.ResolverConfigurationAttemptKey(componentID)
	keys := []string{
		platformcomponents.ActiveTaskKey(componentID),
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
	if acknowledged.Record.Status != taskjournal.TaskStatusCompleted ||
		acknowledged.Record.Target != componentID || !observation.Enabled || !observation.Healthy {
		return nil // Only a successful, healthy acknowledgement can prove withdrawal.
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
	ownerConditions, err := resolverInputTaskOwnerConditions(
		ctx,
		repository.store,
		acknowledged.Record,
		input.Record,
		state.ReadRevision,
	)
	if err != nil {
		return err
	}
	if acknowledged.Record.PlanHash != input.Record.ExecutionPlanSHA256 ||
		!slices.Equal(input.Record.EnvironmentDNS, observation.EnvironmentDNS) ||
		!sameResolverDNSHosts(input.Record.Hosts, observation.ResolvedDNSHosts) {
		return errs.New(errs.KindStateConflict, "DNS acknowledgement differs from its sealed configuration")
	}
	selection, dnsConditions, err := dnsrecords.NewReader(repository.store).
		ResolveEnvironmentDNS(ctx, state.ReadRevision)
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
	conditions = append(conditions, ownerConditions...)
	for _, condition := range append(dnsConditions, zones.Conditions...) {
		conditions = appendHostResolutionCondition(conditions, condition)
	}
	if input.Record.PrivateListener == listener &&
		slices.Equal(input.Record.ZoneListeners, zones.Addresses) &&
		platformcomponents.SameEnvironmentDNSBindings(input.Record.EnvironmentDNS, selection.Captures) {
		retirementConditions, mutations, err := dnsrecords.NewReader(repository.store).
			PrepareObsoleteHostnameRetirement(
				ctx, selection.Captures, observation.ResolvedDNSHosts, state.ReadRevision,
			)
		if err != nil {
			return err
		}
		conditions = append(conditions, retirementConditions...)
		if len(mutations) == 0 {
			receipt, err := repository.prepareResolverDNSOmission(ctx, acknowledged.Record,
				observation, state.Values[1].ModRevision, state.ReadRevision)
			if err != nil {
				return err
			}
			defer receipt.Clear()
			conditions = append(conditions, receipt.Conditions()...)
			mutations = append(mutations, receipt.Mutations()...)
		}
		if state.Values[2] != nil {
			// A return to applied configuration resets a previous failed request.
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: requestKey})
		}
		if len(mutations) == 0 {
			return nil
		}
		result, err := repository.store.Transact(ctx, conditions, mutations)
		return resolverConfigurationTransaction(result, err)
	}
	// One automatic attempt per captured request and acknowledged predecessor.
	// A failed Task stays available for explicit Retry instead of looping forever.
	fingerprint, err := platformcomponents.ResolverConfigurationFingerprint(
		listener,
		selection.Captures,
		zones.Addresses,
	)
	if err != nil {
		return err
	}
	if state.Values[2] != nil {
		previous, err := platformcomponents.DecodeResolverConfigurationAttempt(state.Values[2].Value)
		if err != nil {
			return err
		}
		if previous.Fingerprint == fingerprint && previous.PredecessorTaskID == observation.TaskID {
			matches, err := repository.resolverAttemptMatchesRemovalFences(ctx, previous, state.ReadRevision)
			if err != nil {
				return err
			}
			if matches {
				return nil
			}
		}
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
	result, err := repository.store.Transact(ctx, change.conditions, change.mutations)
	return resolverConfigurationTransaction(result, err)
}

func (repository *TaskRepository) resolverAttemptMatchesRemovalFences(ctx context.Context,
	attempt platformcomponents.ResolverConfigurationAttempt, revision int64,
) (bool, error) {
	taskKey := taskjournal.TaskStorageKey(attempt.TaskID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{taskKey}, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return false, errs.New(errs.KindStateConflict, "previous CoreDNS request Task is unavailable")
	}
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return false, err
	}
	key := platformcomponents.PlatformComponentTaskRenderInputKey(task.PlanID)
	inputRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return false, err
	}
	if inputRead == nil || inputRead.ReadRevision != revision || len(inputRead.Values) != 1 ||
		inputRead.Values[0] == nil {
		return false, errs.New(errs.KindStateConflict, "previous CoreDNS request input is unavailable")
	}
	input, err := platformcomponents.DecodePlatformComponentTaskRenderInput(inputRead.Values[0].Value)
	if err != nil {
		return false, err
	}
	if task.ID != attempt.TaskID || input.PredecessorTaskID != attempt.PredecessorTaskID {
		return false, errs.New(errs.KindStateConflict, "previous CoreDNS request owner changed")
	}
	if _, err := resolverInputTaskOwnerConditions(ctx, repository.store, task, input, revision); err != nil {
		return false, err
	}
	_, matches, err := matchResolverRemovalFences(ctx, repository.store, input.ReferenceConditions, revision)
	return matches, err
}

func resolverConfigurationTransaction(result etcdstore.TransactionResult, err error) error {
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "resolver configuration changed during publication")
	}
	return nil
}
