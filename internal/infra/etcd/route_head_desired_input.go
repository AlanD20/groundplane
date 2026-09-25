package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A direct Route mutation changes only Routes and its Compose-backed runtime
// fields. Every other authored decision comes from the exact prior desired
// input; the effective runtime projection cannot recreate Attach or Script
// decisions that are not represented there.
func routeHeadDesiredInput(
	ctx context.Context,
	store hierarchyStore,
	current *projectionrecord.EnvironmentComposeProjection,
	candidate projectionrecord.EnvironmentComposeProjection,
	revision int64,
) (projectionrecord.EnvironmentDesiredInput, error) {
	if current == nil {
		return projectionrecord.EnvironmentDesiredInput{}, errs.New(
			errs.KindStateConflict, "Route mutation requires an existing desired Service revision",
		)
	}
	prior, found, err := blueprints.ReadDesiredInputRevision(
		ctx, store, candidate.EnvironmentID, current.RevisionID, revision,
	)
	if err != nil {
		return projectionrecord.EnvironmentDesiredInput{}, err
	}
	if !found || prior.Record.RevisionID != current.RevisionID ||
		prior.Record.RenderGeneration+1 != candidate.RenderGeneration {
		return projectionrecord.EnvironmentDesiredInput{}, errs.New(
			errs.KindStateConflict, "Route mutation desired baseline changed",
		)
	}
	input := core.CloneBlueprintDesiredInput(prior.Record.Input)
	input.NormalizedCompose = append([]byte(nil), candidate.NormalizedCompose...)
	input.RuntimeFiles = core.CloneBlueprintDesiredInput(core.BlueprintDesiredInput{
		RuntimeFiles: candidate.RuntimeFiles,
	}).RuntimeFiles
	input.ServiceExtensions = core.CloneBlueprintDesiredInput(core.BlueprintDesiredInput{
		ServiceExtensions: candidate.ServiceExtensions,
	}).ServiceExtensions
	serviceNames := make(map[string]string, len(candidate.DesiredServices))
	for _, service := range candidate.DesiredServices {
		serviceNames[service.Desired.ID] = service.Desired.Name
	}
	input.Routes = make([]core.RouteSpec, len(candidate.DesiredRoutes))
	for index, projected := range candidate.DesiredRoutes {
		name, found := serviceNames[projected.Desired.TargetServiceID]
		if !found {
			return projectionrecord.EnvironmentDesiredInput{}, errs.New(
				errs.KindStateConflict, "Route mutation target Service is absent",
			)
		}
		input.Routes[index] = core.RouteSpec{
			Hostname: projected.Desired.Host, Path: projected.Desired.Path,
			Target: name, TargetPort: projected.Desired.TargetPort,
			Exposure: projected.Desired.Exposure,
		}
	}
	return projectionrecord.NewEnvironmentDesiredInput(
		candidate.EnvironmentID, candidate.RevisionID, candidate.RenderGeneration, input,
	)
}
