package etcd

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (ledger *ReleaseLedger) LoadReleaseRecoveryProgress(ctx context.Context, view workqueue.ResumeView, input releaseexecution.RecoveryInput) (releasework.RecoverySnapshot, error) {
	if ledger == nil || ledger.store == nil || !view.ActiveAttempt || !view.CurrentExecutor ||
		!workexecution.SameAssignment(view.Attempt.Attempt, input.Input.Attempt) || input.Input.Attempt.StartedAt == nil {
		return releasework.RecoverySnapshot{}, errs.New(errs.KindStateConflict, "Release recovery sources require the active started executor")
	}
	return releasework.LoadRecovery(ctx, ledger.store, input, view.Work.ReadRevision)
}

// LoadBlueprintReleaseRecoveryConfiguration resolves the prior snapshot at the
// assignment's read revision. Missing destinations become explicit removals;
// missing snapshot bytes are an error, never interpreted as captured absence.
func (ledger *ReleaseLedger) LoadBlueprintReleaseRecoveryConfiguration(ctx context.Context, view workqueue.ResumeView) (*runtimeconfiguration.Reference, []taskmaterialization.Record, error) {
	if view.Attempt.Attempt.Direction != workexecution.Recovery {
		return nil, nil, errs.New(errs.KindStateConflict, "prior file delivery requires a recovery attempt")
	}
	current, changed, err := ledger.LoadBlueprintReleaseConfiguration(ctx, view)
	if err != nil {
		return nil, nil, err
	}
	sources, err := ledger.LoadBlueprintReleaseWorkInput(ctx, view.Work)
	if err != nil {
		return nil, nil, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	captured := sources.Input.Configuration
	if captured.Current != current {
		return nil, nil, errs.New(errs.KindStateConflict, "Release recovery configuration capture changed")
	}
	var previous *runtimeconfiguration.Snapshot
	if captured.Prior != nil {
		repository, err := runtimeconfiguration.New(ledger.store)
		if err != nil {
			return nil, nil, err
		}
		snapshot, err := repository.Load(ctx, *captured.Prior, view.Work.ReadRevision)
		if err != nil {
			return nil, nil, err
		}
		previous = &snapshot
	}
	restored, err := runtimeconfiguration.SelectRestorations(current.EnvironmentID, current.Generation, previous, changed)
	if err != nil {
		return nil, nil, err
	}
	files := make([]taskmaterialization.Record, len(restored))
	for index, file := range restored {
		files[index] = file.Prior
	}
	slices.SortFunc(files, func(left, right taskmaterialization.Record) int {
		return strings.Compare(left.Destination, right.Destination)
	})
	if len(files) != len(captured.Destinations) {
		return nil, nil, errs.New(errs.KindStateConflict, "Release recovery file selection is incomplete")
	}
	for index, file := range files {
		if file.Destination != captured.Destinations[index] {
			return nil, nil, errs.New(errs.KindStateConflict, "Release recovery file selection differs from its captured destinations")
		}
	}
	return captured.Prior, taskmaterialization.Clone(files), nil
}
