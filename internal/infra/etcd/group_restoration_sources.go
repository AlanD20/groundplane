package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type GroupRestorationSources struct {
	Restoration releasegroups.RestorationSnapshot
	Source      releasegroups.RestorationSource
	Release     ReleaseWorkSources
	Conditions  []keyvalue.Condition
}

func (sources *GroupRestorationSources) Clear() {
	if sources != nil {
		clear(sources.Release.Rollout.Record.CandidateArtifact)
	}
}

// LoadGroupRestorationSources joins the retained restoration input to its real
// successful source and explicitly captured failure. Reading a newer desired
// Blueprint or manufacturing a source assignment is never part of restoration.
func (ledger *ReleaseLedger) LoadGroupRestorationSources(ctx context.Context, work workqueue.View) (GroupRestorationSources, error) {
	if ledger == nil || ledger.store == nil || ctx == nil || work.Task.Task.FinishedAt != nil ||
		work.Task.Task.CollectionStartedAt != nil || !work.Task.Task.PublicationComplete {
		return GroupRestorationSources{}, errs.New(errs.KindStateConflict, "Group restoration sources require active retained execution")
	}
	groups, err := releasegroups.NewExecutionRepository(ledger.store)
	if err != nil {
		return GroupRestorationSources{}, err
	}
	stored, err := groups.LoadRestorationInput(ctx, work)
	if err != nil {
		return GroupRestorationSources{}, err
	}
	selected, err := groups.LoadRestorationSource(ctx, work, stored.Input.Authority.FailedWorkID)
	if err != nil {
		return GroupRestorationSources{}, err
	}
	expected, actual := stored.Input.Source, selected.SourceAttempt.Attempt
	if stored.Input.Authority != selected.Authority || stored.Input.Preparation != selected.Preparation ||
		!workexecution.SameAssignment(expected, actual) || expected.Result == nil || actual.Result == nil || *expected.Result != *actual.Result ||
		expected.Outcome != actual.Outcome || expected.StartedAt == nil || actual.StartedAt == nil ||
		!expected.StartedAt.Equal(*actual.StartedAt) || expected.FinishedAt == nil || actual.FinishedAt == nil ||
		!expected.FinishedAt.Equal(*actual.FinishedAt) {
		return GroupRestorationSources{}, errs.New(errs.KindStateConflict, "Group restoration cause differs from its retained successful source")
	}
	forward, err := ledger.LoadReleaseWorkInput(ctx, selected.Source)
	if err != nil {
		return GroupRestorationSources{}, err
	}
	if forward.Input.Configuration.WorkItemID != expected.WorkItemID || forward.Rollout.Record.Render.ServiceID != work.Item.Item.TargetID ||
		forward.Rollout.Record.Group == nil || forward.Rollout.Record.Group.GroupID != stored.Input.Authority.GroupID ||
		forward.Rollout.Record.Group.OperationID != stored.Input.Authority.GroupOperationID ||
		forward.Rollout.Record.Group.Ordinal != stored.Input.Authority.MemberOrdinal {
		clear(forward.Rollout.Record.CandidateArtifact)
		return GroupRestorationSources{}, errs.New(errs.KindStateConflict, "Group restoration has different executable Service sources")
	}
	conditions, err := resourcePublicationConditions(stored.Conditions, selected.Conditions, forward.Conditions)
	if err != nil {
		clear(forward.Rollout.Record.CandidateArtifact)
		return GroupRestorationSources{}, err
	}
	return GroupRestorationSources{Restoration: stored, Source: selected, Release: forward, Conditions: conditions}, nil
}
