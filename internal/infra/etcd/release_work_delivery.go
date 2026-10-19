package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (ledger *ReleaseLedger) LoadReleaseWorkProgress(
	ctx context.Context,
	view workqueue.ResumeView,
	input releaseexecution.Input,
) (releasework.Snapshot, error) {
	if ledger == nil || ledger.store == nil || !view.ActiveAttempt || !view.CurrentExecutor ||
		!workexecution.SameAssignment(view.Attempt.Attempt, input.Attempt) || input.Attempt.StartedAt == nil {
		return releasework.Snapshot{}, errs.New(
			errs.KindStateConflict,
			"Release sources require the active started executor",
		)
	}
	return releasework.Load(ctx, ledger.store, input, view.Work.ReadRevision)
}

// LoadBlueprintReleaseConfiguration returns only changed destinations from the
// complete immutable configuration. It never follows the current head, and its
// records contain source references rather than resolved Secret values.
func (ledger *ReleaseLedger) LoadBlueprintReleaseConfiguration(
	ctx context.Context,
	view workqueue.ResumeView,
) (runtimeconfiguration.Reference, []taskmaterialization.Record, error) {
	var empty runtimeconfiguration.Reference
	if ledger == nil || ledger.store == nil || !view.ActiveAttempt || !view.CurrentExecutor ||
		view.Attempt.Attempt.StartedAt == nil ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return empty, nil, errs.New(
			errs.KindStateConflict,
			"Release configuration delivery requires an active started executor",
		)
	}
	sources, err := ledger.LoadReleaseWorkInput(ctx, view.Work)
	if err != nil {
		return empty, nil, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	repository, err := runtimeconfiguration.New(ledger.store)
	if err != nil {
		return empty, nil, err
	}
	capture := sources.Input.Configuration
	snapshot, err := repository.Load(ctx, capture.Current, view.Work.ReadRevision)
	if err != nil {
		return empty, nil, err
	}
	selected := make([]taskmaterialization.Record, 0, len(capture.Destinations))
	for _, file := range snapshot.Files {
		if slices.Contains(capture.Destinations, file.Destination) {
			selected = append(selected, file)
		}
	}
	slices.SortFunc(selected, func(a, b taskmaterialization.Record) int {
		if a.Destination < b.Destination {
			return -1
		}
		if a.Destination > b.Destination {
			return 1
		}
		return 0
	})
	if len(selected) != len(capture.Destinations) {
		return empty, nil, errs.New(
			errs.KindStateConflict,
			"captured Release configuration is missing selected destinations",
		)
	}
	for index, file := range selected {
		if file.Destination != capture.Destinations[index] {
			return empty, nil, errs.New(
				errs.KindStateConflict,
				"captured Release configuration destination selection differs",
			)
		}
	}
	return capture.Current, taskmaterialization.Clone(selected), nil
}
