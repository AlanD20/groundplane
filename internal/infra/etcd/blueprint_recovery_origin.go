package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (ledger *ReleaseLedger) LoadBlueprintReleaseRecoveryOrigin(ctx context.Context, view workqueue.ResumeView) (releasework.RecoveryOrigin, error) {
	return releasework.LoadRecoveryOrigin(ctx, ledger.store, view)
}

// BlueprintRecoveryConditions protects captured authority, not latest desired
// intent. Supersession cannot prevent restoration of already-owned effects.
func (ledger *ReleaseLedger) BlueprintRecoveryConditions(ctx context.Context, work workqueue.View) ([]keyvalue.Condition, error) {
	if ledger == nil || ledger.store == nil {
		return nil, errs.New(errs.KindInternal, "Release recovery storage is unavailable")
	}
	origin, err := releasework.LoadRecoverySource(ctx, ledger.store, work)
	if err != nil {
		return nil, err
	}
	sources, err := ledger.LoadBlueprintReleaseWorkInput(ctx, work)
	if err != nil {
		return nil, err
	}
	defer clear(sources.Rollout.Record.CandidateArtifact)
	if len(sources.Predecessors) != 1 || sources.Predecessors[0].ServiceID != work.Item.Item.TargetID {
		return nil, errs.New(errs.KindStateConflict, "Release recovery requires its captured Service predecessor")
	}
	files, err := ledger.ReleaseWorkConfigurationConditions(ctx, work, sources.Input.Configuration)
	if err != nil {
		return nil, err
	}
	predecessor := sources.Predecessors[0]
	conditions := append(origin.Conditions, sources.Conditions...)
	conditions = append(conditions, files...)
	return append(conditions,
		keyvalue.Condition{Key: releases.ReleaseProjectionKey(predecessor.ServiceID), ModRevision: predecessor.ProjectionRevision},
		keyvalue.Condition{Key: serviceruntimerecord.Key(predecessor.ServiceID), ModRevision: predecessor.RuntimeRevision},
	), nil
}
