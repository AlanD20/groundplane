package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// EdgeCheckSource holds one repository-owned view of the Route and its serving
// Components. Public result metadata cannot substitute its publication fences.
type EdgeCheckSource struct {
	configuration  edgechecks.Source
	environment    keyvalue.Versioned[hierarchy.EnvironmentRecord]
	project        keyvalue.Versioned[hierarchy.ProjectRecord]
	fence          environmentfence.Evidence
	conditions     []keyvalue.Condition
	connector      keyvalue.Versioned[componentplanning.Runtime]
	router         keyvalue.Versioned[componentplanning.Runtime]
	projection     keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]
	target         *agentpb.ServiceObservationTarget
	originTarget   *agentpb.ServiceObservationTarget
	originReplicas uint32
	artifact       *agentpb.ComposeArtifact
	path           string
	boundAction    bool
}

func (source EdgeCheckSource) Scope() (keyvalue.Versioned[hierarchy.EnvironmentRecord], keyvalue.Versioned[hierarchy.ProjectRecord]) {
	return source.environment, source.project
}

func (source EdgeCheckSource) AgentID() string { return source.configuration.AgentID }

func (repository *RouteRepository) LoadEdgeCheckSource(ctx context.Context, environmentID, routeID string,
	routeRevision int64, connectorID, path string,
) (EdgeCheckSource, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindRoute, routeID) != nil ||
		ids.Validate(ids.KindComponent, connectorID) != nil || routeRevision <= 0 {
		return EdgeCheckSource{}, errs.New(
			errs.KindValidationFailed,
			"Tunnel check requires an Environment, Route revision and connector",
		)
	}
	source, err := repository.loadEdgeCheckEnvironment(ctx, environmentID, path)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	projection := source.projection
	if projection.Revision != routeRevision {
		return EdgeCheckSource{}, errs.New(
			errs.KindStateConflict,
			"Route configuration changed or is unavailable; refresh before checking public access",
		)
	}
	var selected *environmentprojection.EnvironmentRouteProjection
	for index := range projection.Record.DesiredRoutes {
		if projection.Record.DesiredRoutes[index].Desired.ID == routeID {
			selected = &projection.Record.DesiredRoutes[index]
			break
		}
	}
	if selected == nil {
		return EdgeCheckSource{}, errs.New(errs.KindRouteNotFound, "Route was not found in this Environment")
	}
	if selected.Desired.Exposure != "public" {
		return EdgeCheckSource{}, errs.New(errs.KindValidationFailed, "public access checks require a public Route")
	}
	keys := []string{routes.ObservationKey(routeID), components.RecordKey(connectorID),
		components.EnvironmentCapabilityKey(environmentID, core.ComponentCapabilityEdgeTunnel),
		localagents.LocalAgentSingletonKey, edgechecks.LatestKey(routeID)}
	observed, err := repository.edgeCheckRead(ctx, keys, source.fence.ReadRevision())
	if err != nil {
		return EdgeCheckSource{}, err
	}
	defer keyvalue.ClearValues(observed.Values)
	for index := 0; index < 4; index++ {
		if observed.Values[index] == nil {
			return EdgeCheckSource{}, errs.New(
				errs.KindStateConflict,
				"apply the Route and Tunnel and wait for the installed Agent before checking public access",
			)
		}
	}
	observation, err := routes.DecodeObservation(observed.Values[0].Value)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	if observation.EnvironmentID != environmentID || observation.RouteID != routeID ||
		observation.DesiredGeneration != selected.DesiredGeneration || observation.Observation.Status != routes.ObservedServed {
		return EdgeCheckSource{}, errs.New(
			errs.KindStateConflict,
			"Route has no acknowledged serving configuration; apply it before checking public access",
		)
	}
	connector, err := components.DecodeRecord(observed.Values[1].Value)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	if !edgeCheckComponent(connector, environmentID, connectorID, core.ComponentKindEdgeCloudflare) ||
		string(observed.Values[2].Value) != connectorID {
		return EdgeCheckSource{}, errs.New(
			errs.KindValidationFailed,
			"select the enabled Tunnel belonging to this Environment",
		)
	}
	routerID := observation.Observation.Provider.ComponentID
	agentID, err := localagents.DecodeLocalAgentReference(observed.Values[3].Value)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	source.configuration = edgechecks.Source{EnvironmentRevision: source.environment.Revision,
		BlueprintRevision: projection.Revision, RouteRevision: projection.Revision, RouteObservationRevision: observed.Values[0].ModRevision,
		ComponentRevision: observed.Values[1].ModRevision, RouterComponentID: routerID,
		AgentID: agentID, AgentSingletonRevision: observed.Values[3].ModRevision}
	source.conditions = append(
		source.conditions,
		keyvalue.Condition{
			Key:         blueprints.EnvironmentBlueprintHeadKey(environmentID),
			ModRevision: projection.Revision,
		},
	)
	for index, key := range keys {
		source.conditions = append(
			source.conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(observed.Values[index])},
		)
	}
	if err := repository.captureEdgeCheckRuntime(ctx, &source, connector, *selected, observation); err != nil {
		return EdgeCheckSource{}, err
	}
	return source, nil
}

func (repository *RouteRepository) loadEdgeCheckEnvironment(
	ctx context.Context,
	environmentID, path string,
) (EdgeCheckSource, error) {
	read, err := repository.edgeCheckRead(ctx, []string{hierarchy.EnvironmentKey(environmentID)}, 0)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return EdgeCheckSource{}, errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	environment, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	if environment.ID != environmentID {
		return EdgeCheckSource{}, errs.New(errs.KindInternal, "Tunnel check Environment ownership is corrupt")
	}
	source := EdgeCheckSource{environment: keyvalue.Versioned[hierarchy.EnvironmentRecord]{Record: environment,
		Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision}, path: path}
	source.project, err = repository.edgeCheckProject(ctx, environment.ProjectID, read.ReadRevision)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	source.fence, err = environmentfence.LoadOrdinary(ctx, repository.store, environmentID, read.ReadRevision)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	source.conditions = source.fence.TransactionConditions()
	projection, found, err := blueprints.ReadCurrentProjection(ctx, repository.store, environmentID, read.ReadRevision)
	if err != nil {
		return EdgeCheckSource{}, err
	}
	if !found || projection.ReadRevision != read.ReadRevision {
		return EdgeCheckSource{}, errs.New(
			errs.KindStateConflict,
			"Environment configuration is unavailable; apply its Blueprint before checking public access",
		)
	}
	source.projection = projection
	return source, nil
}

func edgeCheckComponent(record components.Record, environmentID, componentID string, kind core.ComponentKind) bool {
	return record.Desired.ID == componentID && record.Desired.Owner == core.ComponentOwnerEnvironment &&
		record.Desired.OwnerID == environmentID && record.Desired.Kind == kind && record.Desired.Enabled
}

func (repository *RouteRepository) edgeCheckProject(
	ctx context.Context,
	projectID string,
	revision int64,
) (keyvalue.Versioned[hierarchy.ProjectRecord], error) {
	read, err := repository.edgeCheckRead(ctx, []string{hierarchy.ProjectKey(projectID)}, revision)
	if err != nil {
		return keyvalue.Versioned[hierarchy.ProjectRecord]{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[hierarchy.ProjectRecord]{}, errs.New(
			errs.KindStateConflict,
			"Tunnel check Project is unavailable",
		)
	}
	project, err := hierarchy.DecodeProject(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[hierarchy.ProjectRecord]{}, err
	}
	if project.ID != projectID {
		return keyvalue.Versioned[hierarchy.ProjectRecord]{}, errs.New(
			errs.KindInternal,
			"Tunnel check Project ownership is corrupt",
		)
	}
	return keyvalue.Versioned[hierarchy.ProjectRecord]{
		Record:       project,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: revision,
	}, nil
}

func (repository *RouteRepository) edgeCheckRead(
	ctx context.Context,
	keys []string,
	revision int64,
) (*keyvalue.GetManyResult, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: slices.Clone(keys), Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision <= 0 || revision != 0 && read.ReadRevision != revision ||
		len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return nil, errs.New(errs.KindInternal, "Tunnel check source capture is incomplete")
	}
	return read, nil
}
