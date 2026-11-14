package blueprint

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/entryfiles"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *EntryRemovalPreparation) prepareFileInput(ctx context.Context, view workqueue.View,
	snapshot blueprintunits.Snapshot, source etcd.BlueprintEntryRemovalSource,
	guards []keyvalue.Condition, at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	allocate := func(kind ids.Kind, name string) string { return ids.DeriveAt(kind, item.CreatedAt, item.ID, name) }
	files, err := entryfiles.RemoveEntry(
		ctx,
		item.TargetID,
		source.Configuration.Files,
		allocate,
		owner.service.materials,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(files) == 0 || source.Removal == nil {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Acknowledged Entry source has no matching cleanup files",
		)
	}
	// Earlier omission units may already have retired their values. They must
	// not be reintroduced into this file-only candidate's Entry decorations.
	entries := slices.Clone(source.Projection.Entries)
	entries = slices.DeleteFunc(entries, func(entry entryrecord.Record) bool {
		for _, unit := range snapshot.Desired.Record.Units {
			if unit.Removal && unit.Target.Kind == ids.KindEnvEntry && unit.Target.ID == entry.Entry.ID {
				return true
			}
		}
		return false
	})
	candidate, _, err := composerender.ProjectEnvironmentEntryMutation(source.Projection,
		composerender.EnvironmentEntryArtifactMutation{RevisionID: task.ID, RenderGeneration: source.Generation,
			ArtifactID: allocate(
				ids.KindConfig,
				"entry-removal-artifact",
			), PlanID: allocate(ids.KindPlan, "entry-removal-plan"),
			Entries: entries})
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(candidate.ComposeArtifact)
	configuration, err := owner.service.repository.PrepareResourceWorkConfiguration(ctx, view,
		allocate(ids.KindConfig, "entry-removal-configuration"), source.Generation, files)
	if err != nil {
		return workqueue.Change{}, err
	}
	operationID := allocate(ids.KindOperation, "entry-operation")
	prepared, err := owner.service.repository.PrepareResourceWorkSecretPins(ctx, view, operationID, configuration)
	if err != nil {
		return workqueue.Change{}, err
	}
	var secrets *tasksecretpins.Binding
	if !prepared.IsZero() {
		binding, err := prepared.Binding()
		if err != nil {
			return workqueue.Change{}, err
		}
		secrets = &binding
	}
	runtime := taskplanning.EntryMutationRuntime{
		Projection:    source.Projection,
		EpochRevision: source.Configuration.EpochRevision,
	}
	input, capture, err := runtime.PrepareConfigurationWork(taskplanning.EntryConfigurationRequest{
		Task: task, Item: item, OperationID: operationID, Candidate: candidate, Files: files,
		Configuration: configuration, Secrets: secrets, BlueprintRemoval: source.Removal})
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(input.Baseline.CanonicalYaml)
	defer clear(input.Candidate.CanonicalYaml)
	configurationGuards, err := owner.service.repository.ResourceWorkConfigurationConditions(ctx, view, configuration)
	if err != nil {
		return workqueue.Change{}, err
	}
	guards = append(append([]keyvalue.Condition(nil), guards...), configurationGuards...)
	return owner.inputs.PrepareBlueprintInput(ctx, view, input, capture, prepared, guards, at)
}
