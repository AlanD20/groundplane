package resolverwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type Repository struct{ store workqueue.Store }

func NewRepository(store workqueue.Store) (*Repository, error) {
	if store == nil {
		return nil, invalid("DNS resolver execution storage is required")
	}
	return &Repository{store: store}, nil
}

func InputKey(workID string) string   { return "/v1/execution/resolver/inputs/" + workID }
func CaptureKey(workID string) string { return "/v1/execution/resolver/captures/" + workID }

const AppliedPrefix = "/v1/applied/resolver/"

func AppliedKey(hostID, componentID string) string {
	return AppliedPrefix + hostID + "/" + componentID
}

func IntentKey(hostID, componentID string) string {
	return "/v1/desired/resolver/" + hostID + "/" + componentID
}

func (repository *Repository) ReadIntent(
	ctx context.Context,
	hostID, componentID string,
	revision int64,
) (int64, error) {
	if ids.Validate(ids.KindHost, hostID) != nil || ids.Validate(ids.KindComponent, componentID) != nil {
		return 0, invalid("DNS resolver intent requires its selected host and Component")
	}
	read, err := repository.read(ctx, []string{IntentKey(hostID, componentID)}, revision)
	if err != nil {
		return 0, err
	}
	defer keyvalue.ClearValues(read.Values)
	if value := read.Values[0]; value != nil && ids.Validate(ids.KindWorkItem, string(value.Value)) != nil {
		return 0, invalid("DNS resolver desired execution identity is corrupt")
	}
	return keyvalue.RevisionOf(read.Values[0]), nil
}

type StoredInput struct {
	Input     resolverexecution.Input
	Capture   Capture
	Reference workexecution.Input
	Guards    []keyvalue.Condition
}

// PreparePublication returns only a contribution. Desired state, idempotency,
// native Task/item publication and projection changes MUST join its transaction.
// No executable item is published by staging a Corefile on its own.
func PreparePublication(input resolverexecution.Input, capture Capture) (workqueue.Change, error) {
	raw, _, err := resolverexecution.Capture(capture.PlanID, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	authority, err := encodeCapture(capture, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(raw)+len(authority) > keyvalue.MaximumBytes-64*1024 {
		return workqueue.Change{}, invalid("DNS resolver publication exceeds the atomic transaction input budget")
	}
	change := workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: InputKey(input.WorkID)}, {Key: CaptureKey(input.WorkID)},
			{Key: componentrecord.RecordKey(input.ComponentID), ModRevision: capture.ComponentRevision},
			{Key: AppliedKey(input.HostID, input.ComponentID), ModRevision: capture.AppliedRevision},
			{Key: IntentKey(input.HostID, input.ComponentID), ModRevision: capture.IntentRevision},
			{Key: resolverbaseline.StorageKey, ModRevision: capture.BaselineRevision},
			{Key: hostresolution.StorageKey, ModRevision: capture.ProjectionRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: InputKey(input.WorkID), Value: raw},
			{Type: keyvalue.MutationPut, Key: CaptureKey(input.WorkID), Value: authority},
			{Type: keyvalue.MutationPut, Key: IntentKey(input.HostID, input.ComponentID), Value: []byte(input.WorkID)}},
	}
	change.Conditions = append(change.Conditions, capture.References...)
	request, err := prepareRequest(input, capture)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	if len(raw)+len(authority)+len(request.Mutations[0].Value) > keyvalue.MaximumBytes-64*1024 {
		keyvalue.ClearMutationValues(change.Mutations)
		keyvalue.ClearMutationValues(request.Mutations)
		return workqueue.Change{}, invalid(
			"DNS resolver publication exceeds the atomic input and retained-request budget",
		)
	}
	change.Conditions = append(change.Conditions, request.Conditions...)
	change.Mutations = append(change.Mutations, request.Mutations...)
	return change, nil
}

func NewWork(input resolverexecution.Input, capture Capture) (workexecution.Task, workexecution.Item, error) {
	if err := capture.Validate(input); err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	_, reference, err := resolverexecution.Capture(capture.PlanID, input)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	task := workexecution.Task{
		ID:            input.TaskID,
		Action:        capture.Action,
		Actor:         capture.Actor,
		TargetID:      input.ComponentID,
		TargetName:    capture.TargetName,
		Intent:        reference,
		CreatedAt:     input.CreatedAt,
		UpdatedAt:     input.CreatedAt,
		Deadline:      input.Deadline,
		ExpectedItems: 1,
	}
	item := workexecution.Item{
		ID:        input.WorkID,
		TaskID:    input.TaskID,
		Operation: workexecution.Resolver,
		TargetID:  input.ComponentID,
		Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID},
		Input:     &reference,
		Claims: []workexecution.Claim{
			{HostID: input.HostID, ScopeID: input.ComponentID},
		},
		Phase:     workexecution.Ready,
		Cleanup:   workexecution.CleanupNone,
		CreatedAt: input.CreatedAt,
		UpdatedAt: input.CreatedAt,
		Deadline:  input.Deadline,
	}
	if err := task.Validate(); err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	return task, item, item.Validate()
}

func (repository *Repository) read(
	ctx context.Context,
	keys []string,
	revision int64,
) (*keyvalue.GetManyResult, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision ||
		len(read.Values) != len(keys) {
		return nil, invalid("DNS resolver execution snapshot read is incomplete")
	}
	for index, value := range read.Values {
		if value != nil &&
			(value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision) {
			keyvalue.ClearValues(read.Values)
			return nil, invalid("DNS resolver execution snapshot has invalid record ownership")
		}
	}
	return read, nil
}

func (repository *Repository) Load(ctx context.Context, view workqueue.View) (StoredInput, error) {
	if view.Task.Revision <= 0 || view.Item.Revision <= 0 || view.ReadRevision < view.Task.Revision ||
		view.ReadRevision < view.Item.Revision {
		return StoredInput{}, invalid("DNS resolver input requires retained native execution membership")
	}
	keys := []string{InputKey(view.Item.Item.ID), CaptureKey(view.Item.Item.ID)}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return StoredInput{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil || view.Item.Item.Input == nil {
		return StoredInput{}, invalid("DNS resolver captured input is unavailable")
	}
	input, reference, err := resolverexecution.Decode(view.Item.Item.Input.ID, read.Values[0].Value)
	if err != nil || matchesWork(input, reference, view.Task.Task, view.Item.Item) != nil {
		return StoredInput{}, invalid("DNS resolver input differs from its accepted native work")
	}
	capture, err := decodeCapture(read.Values[1].Value, input)
	if err != nil {
		return StoredInput{}, err
	}
	if view.Task.Task.Actor != capture.Actor || view.Task.Task.Action != capture.Action ||
		view.Task.Task.TargetName != capture.TargetName {
		return StoredInput{}, invalid("DNS resolver accepted action differs from its immutable capture")
	}
	if !view.Task.Task.CreatedAt.Equal(input.CreatedAt) || capture.ReadRevision > view.ReadRevision {
		return StoredInput{}, invalid("DNS resolver source snapshot is newer than its native publication")
	}
	return StoredInput{Input: input, Capture: capture, Reference: reference, Guards: []keyvalue.Condition{
		{
			Key:         keys[0],
			ModRevision: read.Values[0].ModRevision,
		}, {Key: keys[1], ModRevision: read.Values[1].ModRevision}}}, nil
}

// CleanupDeadline renews only a bounded cleanup budget; captured input is fixed.
func CleanupDeadline(at time.Time) time.Time { return at.UTC().Add(5 * time.Minute) }
