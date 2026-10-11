package etcd

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type resolverAcknowledgement struct {
	observation         platformcomponents.ComponentObservationRecord
	observationRevision int64
	input               platformcomponents.PlatformComponentTaskRenderInput
	attempt             *platformcomponents.ResolverConfigurationAttempt
	conditions          []keyvalue.Condition
	pendingTaskID       string
}

type resolverAcknowledgementPurpose uint8

const (
	resolverServingAcknowledgement resolverAcknowledgementPurpose = iota
	resolverWithdrawalBeforeEffects
	resolverWithdrawalFinalization
)

// A successful Agent acknowledgement, its sealed input and the current resolver
// fence are read together. Prepared input alone is never proof of DNS activation.
func readResolverAcknowledgement(
	ctx context.Context, store hierarchyStore, revision int64,
) (resolverAcknowledgement, bool, error) {
	return readResolverStateAcknowledgement(ctx, store, revision, resolverServingAcknowledgement)
}

func readResolverStateAcknowledgement(
	ctx context.Context, store hierarchyStore, revision int64, purpose resolverAcknowledgementPurpose,
) (resolverAcknowledgement, bool, error) {
	var ack resolverAcknowledgement
	indexKey := platformcomponents.PlatformComponentKindKey(core.ComponentKindCoreDNS)
	index, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{indexKey}, Revision: revision})
	if err != nil {
		return ack, false, err
	}
	if index == nil || index.ReadRevision != revision || len(index.Values) != 1 {
		return ack, false, errs.New(errs.KindInternal, "DNS resolver index read is incomplete")
	}
	if index.Values[0] == nil {
		if purpose != resolverServingAcknowledgement {
			ack.conditions = []keyvalue.Condition{{Key: indexKey}}
			return ack, true, nil
		}
		return ack, false, errs.New(
			errs.KindValidationFailed,
			"Environment DNS requires an enabled platform CoreDNS resolver",
		)
	}
	componentID := string(index.Values[0].Value)
	keys := []string{components.RecordKey(componentID), platformcomponents.ComponentObservationKey(componentID),
		platformcomponents.ActiveTaskKey(componentID), platformcomponents.ResolverConfigurationAttemptKey(componentID),
		platformcomponents.PlatformComponentBootstrapKey(componentID)}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return ack, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil {
		return ack, false, errs.New(errs.KindInternal, "DNS resolver evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	component, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return ack, false, err
	}
	if component.Desired.ID != componentID || component.Desired.Kind != core.ComponentKindCoreDNS ||
		component.Desired.Owner != core.ComponentOwnerPlatform || component.Desired.OwnerID != "" {
		return ack, false, errs.New(errs.KindInternal, "DNS resolver identity changed")
	}
	if purpose == resolverServingAcknowledgement && !component.Desired.Enabled {
		return ack, false, errs.New(
			errs.KindValidationFailed,
			"Environment DNS requires an enabled platform CoreDNS resolver",
		)
	}
	ack.conditions = []keyvalue.Condition{{Key: indexKey, ModRevision: index.Values[0].ModRevision}}
	for index, key := range keys {
		ack.conditions = append(
			ack.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	if read.Values[2] != nil {
		ack.pendingTaskID = string(read.Values[2].Value)
	}
	if read.Values[2] != nil && purpose != resolverWithdrawalFinalization {
		return ack, false, nil
	}
	if read.Values[1] == nil {
		// The bootstrap marker is consumed atomically with the first host Task.
		// Its positive provenance proves no execution was published; empty
		// runtime metadata alone cannot prove absence after failed bootstrap.
		absent := purpose != resolverServingAcknowledgement && read.Values[2] == nil &&
			!component.Desired.Enabled && !component.Runtime.Healthy && len(component.Runtime.GeneratedServices) == 0 &&
			read.Values[4] != nil && string(read.Values[4].Value) == componentID
		return ack, absent, nil
	}
	ack.observation, err = platformcomponents.DecodeComponentObservation(read.Values[1].Value)
	if err != nil {
		return ack, false, err
	}
	ack.observationRevision = read.Values[1].ModRevision
	if ack.observation.ComponentID != componentID {
		return ack, false, errs.New(errs.KindInternal, "DNS observation belongs to another resolver")
	}
	if ack.observation.Enabled && !ack.observation.Healthy ||
		purpose == resolverServingAcknowledgement && !ack.observation.Enabled {
		return ack, false, nil
	}
	taskKey := taskjournal.TaskStorageKey(ack.observation.TaskID)
	taskRead, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{taskKey}, Revision: revision})
	if err != nil {
		return ack, false, err
	}
	if taskRead == nil || taskRead.ReadRevision != revision || len(taskRead.Values) != 1 || taskRead.Values[0] == nil {
		return ack, false, errs.New(errs.KindStateConflict, "DNS acknowledged Task is unavailable")
	}
	defer keyvalue.ClearValues(taskRead.Values)
	task, err := DecodeTaskRecord(taskRead.Values[0].Value)
	if err != nil {
		return ack, false, err
	}
	if task.ID != ack.observation.TaskID || task.Target != componentID ||
		task.Status != taskjournal.TaskStatusCompleted {
		return ack, false, errs.New(errs.KindStateConflict, "DNS requires a completed resolver acknowledgement")
	}
	input, err := platformcomponents.ReadAcknowledgedInput(ctx, store, ack.observation, task.PlanID, revision)
	if err != nil {
		return ack, false, err
	}
	ownerConditions, err := resolverInputTaskOwnerConditions(ctx, store, task, input.Record, revision)
	if err != nil {
		return ack, false, err
	}
	if task.PlanHash != input.Record.ExecutionPlanSHA256 || ack.observation.Enabled &&
		(!slices.Equal(input.Record.EnvironmentDNS, ack.observation.EnvironmentDNS) ||
			!sameResolverDNSHosts(input.Record.Hosts, ack.observation.ResolvedDNSHosts)) {
		return ack, false, errs.New(errs.KindStateConflict, "DNS acknowledgement differs from its sealed configuration")
	}
	ack.input = input.Record
	ack.conditions = append(
		ack.conditions,
		keyvalue.Condition{Key: taskKey, ModRevision: taskRead.Values[0].ModRevision},
		keyvalue.Condition{
			Key:         platformcomponents.PlatformComponentTaskRenderInputKey(task.PlanID),
			ModRevision: input.Revision,
		},
	)
	ack.conditions = append(ack.conditions, ownerConditions...)
	if read.Values[3] != nil {
		attempt, err := platformcomponents.DecodeResolverConfigurationAttempt(read.Values[3].Value)
		if err != nil {
			return ack, false, err
		}
		ack.attempt = &attempt
	}
	return ack, true, nil
}

func sameResolverDNSHosts(left, right []platformcomponents.PlatformDNSHost) bool {
	return slices.EqualFunc(left, right, func(a, b platformcomponents.PlatformDNSHost) bool {
		return a.Address == b.Address && slices.Equal(a.Hostnames, b.Hostnames)
	})
}

func failedResolverConfigurationRequest(ctx context.Context, store hierarchyStore,
	revision int64, ack resolverAcknowledgement,
) (string, []keyvalue.Condition, error) {
	if ack.attempt == nil || ack.attempt.PredecessorTaskID != ack.observation.TaskID {
		return "", nil, nil
	}
	// A failure from an older authored head must not fail a newer Apply.
	global, conditions, err := dnsrecords.NewReader(store).ResolveEnvironmentDNS(ctx, revision)
	if err != nil {
		return "", nil, err
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskjournal.TaskStorageKey(ack.attempt.TaskID)}, Revision: revision,
	})
	if err != nil {
		return "", nil, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return "", nil, errs.New(errs.KindStateConflict, "DNS update Task is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return "", nil, err
	}
	inputKey := platformcomponents.PlatformComponentTaskRenderInputKey(task.PlanID)
	inputRead, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{inputKey}, Revision: revision})
	if err != nil {
		return "", nil, err
	}
	if inputRead == nil || inputRead.ReadRevision != revision || len(inputRead.Values) != 1 ||
		inputRead.Values[0] == nil {
		return "", nil, errs.New(errs.KindStateConflict, "DNS update captured input is unavailable")
	}
	defer keyvalue.ClearValues(inputRead.Values)
	input, err := platformcomponents.DecodePlatformComponentTaskRenderInput(inputRead.Values[0].Value)
	if err != nil {
		return "", nil, err
	}
	if input.PlanID != task.PlanID || input.ComponentID != task.Target ||
		input.ExecutionPlanSHA256 != task.PlanHash || input.PredecessorTaskID != ack.attempt.PredecessorTaskID {
		return "", nil, errs.New(errs.KindStateConflict, "DNS update captured input differs from its Task")
	}
	ownerConditions, err := resolverInputTaskOwnerConditions(ctx, store, task, input, revision)
	if err != nil {
		return "", nil, err
	}
	removalConditions, matches, err := matchResolverRemovalFences(ctx, store, input.ReferenceConditions, revision)
	if err != nil || !matches {
		return "", nil, err
	}
	generation, matches, err := dnsrecords.NewReader(store).
		MatchResolverDNSGeneration(ctx, input.ReferenceConditions, revision)
	if err != nil || !matches {
		return "", nil, err
	}
	fingerprint, err := platformcomponents.ResolverConfigurationFingerprint(
		input.PrivateListener,
		global.Captures,
		input.ZoneListeners,
	)
	if err != nil {
		return "", nil, err
	}
	if fingerprint != ack.attempt.Fingerprint {
		return "", nil, nil
	}
	conditions = append(conditions, removalConditions...)
	conditions = append(conditions, ownerConditions...)
	conditions = appendHostResolutionCondition(conditions, generation)
	conditions = appendHostResolutionCondition(
		conditions,
		keyvalue.Condition{Key: inputKey, ModRevision: inputRead.Values[0].ModRevision},
	)
	if taskjournal.IsTerminalTaskStatus(task.Status) && task.Status != taskjournal.TaskStatusCompleted {
		conditions = append(conditions, ack.conditions...)
		conditions = append(conditions, keyvalue.Condition{
			Key: taskjournal.TaskStorageKey(task.ID), ModRevision: read.Values[0].ModRevision,
		})
		return task.ID, conditions, nil
	}
	return "", nil, nil
}

// Native retries reuse the originating immutable recipe. Its origin must remain
// pinned and match the reporting Task; removing the Task-id check is not proof.
func resolverInputTaskOwnerConditions(ctx context.Context, store hierarchyStore,
	task TaskRecord, input platformcomponents.PlatformComponentTaskRenderInput, revision int64,
) ([]keyvalue.Condition, error) {
	if task.PlanID != input.PlanID || task.Target != input.ComponentID || task.PlanHash != input.ExecutionPlanSHA256 {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS Task differs from its captured input")
	}
	if task.ID == input.TaskID {
		return nil, nil
	}
	if task.RetryOf == "" {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS Task has no captured retry origin")
	}
	key := taskjournal.TaskStorageKey(input.TaskID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS captured retry origin is unavailable")
	}
	origin, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if origin.ID != input.TaskID || origin.PlanID != task.PlanID || origin.Target != task.Target ||
		origin.PlanHash != task.PlanHash || origin.Executor != taskjournal.TaskExecutorAgent {
		return nil, errs.New(errs.KindStateConflict, "CoreDNS captured retry origin changed")
	}
	return []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}, nil
}

// Removal does not replace a direct operation's desired head until success.
// A failed request captured before its resource fence cannot fail the new removal.
func matchResolverRemovalFences(ctx context.Context, store hierarchyStore,
	captured []keyvalue.Condition, revision int64,
) ([]keyvalue.Condition, bool, error) {
	var matched []keyvalue.Condition
	for _, condition := range captured {
		if !strings.HasPrefix(condition.Key, deletions.TombstoneKey(string(deletions.DeletionTargetZone), "")) &&
			!strings.HasPrefix(condition.Key, deletions.TombstoneKey(string(deletions.DeletionTargetService), "")) {
			continue
		}
		read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{condition.Key}, Revision: revision})
		if err != nil {
			return nil, false, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			return nil, false, errs.New(errs.KindInternal, "DNS removal failure source read is incomplete")
		}
		if condition.ModRevision != keyvalue.RevisionOf(read.Values[0]) {
			return nil, false, nil
		}
		matched = append(matched, condition)
	}
	return matched, true, nil
}
