// Package servicelifecycle owns immutable runtime selection for native Service
// lifecycle Tasks.
package servicelifecycle

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseReader interface {
	ResolveServing(context.Context, string, string, int64) (etcd.ServingRelease, error)
	GetReleaseRenderInputAt(context.Context, string, int64) (etcdstore.Versioned[etcd.ReleaseRenderInput], error)
}

func CaptureRelease(
	ctx context.Context,
	reader ReleaseReader,
	applied etcdstore.Versioned[etcd.EnvironmentComposeProjection],
	environmentID string,
	serviceID string,
) (etcd.ServiceLifecycleRelease, error) {
	serving, err := reader.ResolveServing(ctx, environmentID, serviceID, applied.ReadRevision)
	if err != nil {
		return etcd.ServiceLifecycleRelease{}, err
	}
	current, err := reader.GetReleaseRenderInputAt(ctx, serving.Intent.ID, applied.ReadRevision)
	if err != nil {
		return etcd.ServiceLifecycleRelease{}, err
	}
	if serving.Revision != applied.ReadRevision || current.ReadRevision != applied.ReadRevision ||
		current.Record.ReleaseID != serving.Intent.ID || current.Record.ServiceID != serviceID ||
		current.Record.EnvironmentID != environmentID {
		return etcd.ServiceLifecycleRelease{}, errs.New(
			errs.KindStateConflict,
			"Service lifecycle serving authority changed",
		)
	}
	authority := etcd.ServiceLifecycleRelease{
		ServingReleaseID: serving.Intent.ID, PriorServingReleaseID: serving.Intent.PriorServingReleaseID,
		ProjectionRevision: serving.ProjectionRevision, IntentRevision: serving.IntentRevision,
		RenderRevision: current.Revision, Current: current.Record,
	}
	if current.Record.Strategy == domain.StrategyBlueGreen &&
		current.Record.PriorStrategy == domain.StrategyBlueGreen &&
		current.Record.PriorArtifactID == "" && serving.Intent.PriorServingReleaseID != "" {
		prior, priorErr := reader.GetReleaseRenderInputAt(
			ctx,
			serving.Intent.PriorServingReleaseID,
			applied.ReadRevision,
		)
		if priorErr != nil {
			return etcd.ServiceLifecycleRelease{}, priorErr
		}
		if prior.ReadRevision != applied.ReadRevision || prior.Record.ServiceID != serviceID ||
			prior.Record.EnvironmentID != environmentID || prior.Record.CandidateTarget != current.Record.PriorTarget {
			return etcd.ServiceLifecycleRelease{}, errs.New(
				errs.KindStateConflict,
				"Service lifecycle retained authority changed",
			)
		}
		authority.RetainedPrior = &prior.Record
		authority.RetainedPriorRenderRevision = prior.Revision
	} else {
		authority.PriorServingReleaseID = ""
	}
	return authority, nil
}
