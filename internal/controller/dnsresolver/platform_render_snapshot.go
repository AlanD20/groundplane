package dnsresolver

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
)

type fixedProjectionReader struct {
	record resolutionrecord.HostResolutionProjectionRecord
}

type fixedObservationReader struct {
	componentID string
	record      platformcomponents.ComponentObservationRecord
}

func (reader fixedProjectionReader) GetHostResolutionProjection(
	context.Context,
) (etcdstore.Versioned[resolutionrecord.HostResolutionProjectionRecord], bool, error) {
	return etcdstore.Versioned[resolutionrecord.HostResolutionProjectionRecord]{Record: reader.record}, true, nil
}

func (reader fixedObservationReader) GetPlatformComponentObservation(
	_ context.Context,
	componentID string,
) (etcdstore.Versioned[platformcomponents.ComponentObservationRecord], bool, error) {
	if componentID != reader.componentID {
		return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{}, false, nil
	}
	return etcdstore.Versioned[platformcomponents.ComponentObservationRecord]{Record: reader.record}, true, nil
}

// PrepareConfigTaskAtProjection is the startup/recovery seam. It uses the
// same planner as an ordinary Component reconciliation while pinning the
// caller's exact projection snapshot instead of reading a second view.
func (planner *PlatformRenderPlanner) PrepareConfigTaskAtProjection(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
	task etcd.TaskRecord,
	projection resolutionrecord.HostResolutionProjectionRecord,
	priorObservation *platformcomponents.ComponentObservationRecord,
) (platformcomponents.PlatformComponentTaskRenderInput, error) {
	clone := *planner
	clone.projections = fixedProjectionReader{record: projection}
	if priorObservation != nil {
		clone.observations = fixedObservationReader{componentID: desired.ID, record: *priorObservation}
	}
	return clone.PrepareConfigTask(ctx, current, desired, task)
}

// PrepareBootstrapConfigTaskAtProjection is reserved for the clean-start seam,
// whose repository caller has proved same-revision singleton provenance.
func (planner *PlatformRenderPlanner) PrepareBootstrapConfigTaskAtProjection(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
	task etcd.TaskRecord,
	projection resolutionrecord.HostResolutionProjectionRecord,
) (platformcomponents.PlatformComponentTaskRenderInput, error) {
	clone := *planner
	clone.bootstrapProvenance = true
	return clone.PrepareConfigTaskAtProjection(ctx, current, desired, task, projection, nil)
}
