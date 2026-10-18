package entrywork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Removal identifies the private desired revision promoted only after files
// have been removed. Queue claims protect the Environment during execution;
// there is no second Task or generic Task tombstone lifecycle.
type Removal struct {
	EntryID        string `json:"entry_id"`
	BaseRevisionID string `json:"base_revision_id"`
	HeadRevision   int64  `json:"head_revision"`
	DescriptorID   string `json:"descriptor_id"`
	RevisionID     string `json:"revision_id"`
}

func (removal Removal) matches(view workqueue.View, environmentID string, generation uint64) error {
	if environmentID != view.Task.Task.Scope.EnvironmentID || generation == 0 || view.Task.Task.Action != workexecution.ActionRemove || ids.Validate(ids.KindEnvEntry, removal.EntryID) != nil ||
		ids.Validate(ids.KindTask, removal.BaseRevisionID) != nil ||
		ids.Validate(ids.KindTask, "task_"+removal.DescriptorID) != nil ||
		removal.RevisionID != view.Task.Task.ID || removal.BaseRevisionID == removal.RevisionID ||
		removal.HeadRevision <= 0 || removal.HeadRevision > view.ReadRevision {
		return conflict("Entry removal has no matching captured desired revision")
	}
	return nil
}

// prepareRemoval joins the exact desired-head promotion and Entry retirement
// with the host proof and queue completion. No current desired state is rendered.
func (repository *InputRepository) prepareRemoval(ctx context.Context, view workqueue.View,
	input *agentpb.EntryConfigurationWorkInput, capture Capture,
) (workqueue.Change, error) {
	if capture.Removal == nil {
		if input.Removal {
			return workqueue.Change{}, conflict("Entry removal authority is absent")
		}
		return workqueue.Change{}, nil
	}
	return repository.PrepareRemoval(ctx, view, *capture.Removal, input.RenderGeneration)
}

// PrepareRemoval commits only the captured desired metadata retirement. Host
// callers must join acknowledged file cleanup; Controller-only callers must
// join proof that the acknowledged configuration contains no target sources.
func (repository *InputRepository) PrepareRemoval(ctx context.Context, view workqueue.View, removal Removal, generation uint64) (workqueue.Change, error) {
	environmentID := view.Task.Task.Scope.EnvironmentID
	if err := removal.matches(view, environmentID, generation); err != nil {
		return workqueue.Change{}, err
	}
	keys := []string{
		blueprints.EnvironmentBlueprintHeadKey(environmentID),
		blueprints.EnvironmentBlueprintDescriptorKeyByID(removal.DescriptorID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, removal.RevisionID),
		entries.BlueprintEntryEnvironmentPrefix + removal.EntryID,
	}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value == nil {
			return workqueue.Change{}, conflict("Entry removal desired authority is unavailable")
		}
	}
	base, err := idempotency.DecodeTaskReference(read.Values[0].Value)
	if err != nil || base != removal.BaseRevisionID || read.Values[0].ModRevision != removal.HeadRevision ||
		string(read.Values[3].Value) != environmentID {
		return workqueue.Change{}, conflict("Entry removal baseline or owner changed")
	}
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(read.Values[1].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[2].Value)
	claim := descriptor.Claim
	if err != nil || descriptor.State != blueprints.EnvironmentBlueprintStageSealed ||
		claim.DescriptorID != removal.DescriptorID || claim.EnvironmentID != environmentID ||
		claim.RevisionID != removal.RevisionID || claim.TaskID != view.Task.Task.ID ||
		claim.RenderGeneration != generation || claim.BaselineHeadRevision != removal.HeadRevision ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		seal != blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor) {
		return workqueue.Change{}, conflict("Entry removal staged revision changed")
	}
	candidate, found, err := blueprints.ReadEffectiveProjectionRevisionAt(ctx, repository.store,
		environmentID, removal.RevisionID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found || candidate.Record.EnvironmentID != environmentID ||
		candidate.Record.RevisionID != removal.RevisionID || candidate.Record.RenderGeneration != generation {
		return workqueue.Change{}, conflict("Entry removal captured projection is unavailable")
	}
	for _, entry := range candidate.Record.Entries {
		if entry.Entry.ID == removal.EntryID {
			return workqueue.Change{}, conflict("Entry removal candidate still contains the Entry")
		}
	}
	scripts, err := entries.PrepareEntryScriptAbsence(ctx, repository.store, removal.EntryID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	locator, _, err := blueprints.EnvironmentBlueprintLocatorKey(claim.Locator)
	if err != nil {
		return workqueue.Change{}, err
	}
	descriptor.State = blueprints.EnvironmentBlueprintStagePublished
	descriptor.UpdatedAt = blueprints.NextBlueprintProgressTime(descriptor.UpdatedAt)
	encoded, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		return workqueue.Change{}, err
	}
	reference, err := idempotency.EncodeTaskReference(removal.RevisionID)
	if err != nil {
		clear(encoded)
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: scripts, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: keys[0], Value: reference},
		{Type: keyvalue.MutationPut, Key: keys[1], Value: encoded},
		{Type: keyvalue.MutationDelete, Key: locator},
		{Type: keyvalue.MutationDelete, Key: keys[3]},
		{Type: keyvalue.MutationDelete, Key: entryvalues.PlainPrefix + removal.EntryID + "/", Prefix: true},
		{Type: keyvalue.MutationDelete, Key: entryvalues.SecretPrefix + removal.EntryID + "/", Prefix: true},
	}}
	for index, key := range keys {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision})
	}
	return change, nil
}
