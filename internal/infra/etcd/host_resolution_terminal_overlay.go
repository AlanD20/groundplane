package etcd

import (
	"context"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) hostResolutionTerminalOverlay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
	routes []scannedRoute,
) (string, map[string]routerecord.Record, map[string]componentrecord.Record, error) {
	removal := ""
	routeOverride := make(map[string]routerecord.Record)
	componentOverride := make(map[string]componentrecord.Record)
	switch {
	case task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceComponent ||
		task.Params[componentTaskBlueprintProcedureParam] != "":
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{environmentchanges.ComponentTaskIntentKey(task.ID)}, Revision: revision,
		})
		if err != nil {
			return "", nil, nil, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			return "", nil, nil, errs.New(errs.KindInternal, "host-resolution Component intent read is incomplete")
		}
		if read.Values[0] != nil {
			intent, decodeErr := environmentchanges.DecodeComponentTaskIntent(read.Values[0].Value)
			if decodeErr != nil {
				return "", nil, nil, decodeErr
			}
			for _, candidate := range intent.Candidates {
				componentOverride[candidate.Candidate.Desired.ID] = candidate.Current
				if terminalStatus == taskjournal.TaskStatusCompleted {
					promoted, promoteErr := componentrecord.SetRuntime(candidate.Candidate,
						candidate.Candidate.Runtime.GeneratedServices, candidate.Candidate.Runtime.PinnedIPv4,
						candidate.Candidate.Desired.Enabled)
					if promoteErr != nil {
						return "", nil, nil, promoteErr
					}
					componentOverride[candidate.Candidate.Desired.ID] = promoted
				}
			}
			if intent.RouteProjection != nil && (terminalStatus == taskjournal.TaskStatusCompleted ||
				task.Params[releaserender.TaskReleasePublicationParam] == "") {
				for _, candidate := range intent.RouteProjection.Routes {
					route, routeErr := hostResolutionRouteAtSnapshot(routes, candidate.Desired.ID)
					if routeErr != nil {
						return "", nil, nil, routeErr
					}
					if route.EnvironmentID != intent.EnvironmentID ||
						route.DesiredGeneration != candidate.DesiredGeneration ||
						!componentplanning.RouteDesiredEqual(route.Desired, candidate.Desired) {
						return "", nil, nil, errs.New(
							errs.KindStateConflict,
							"Component Route desired state changed during host reconciliation",
						)
					}
					if intent.RouteProjection.Provider == nil && terminalStatus != taskjournal.TaskStatusCompleted {
						continue
					}
					status := routerecord.ObservedUnserved
					provider := routerecord.ProviderObservation{}
					if intent.RouteProjection.Provider != nil {
						status = routerecord.ObservedDegraded
						if terminalStatus == taskjournal.TaskStatusCompleted {
							status = routerecord.ObservedServed
						}
						pin := intent.RouteProjection.Provider
						provider = routerecord.ProviderObservation{
							ComponentID: pin.ComponentID, DefinitionDigest: pin.DefinitionDigest,
							CatalogDigest: pin.CatalogDigest, InputRevision: pin.InputRevision,
							InputGeneration: pin.InputGeneration,
						}
					}
					replacement, replaceErr := routerecord.SetObservation(route, routerecord.Observation{
						Status: status, DesiredGeneration: route.DesiredGeneration, Provider: provider,
					})
					if replaceErr != nil {
						return "", nil, nil, replaceErr
					}
					routeOverride[route.Desired.ID] = replacement
				}
			}
		}
	}
	return removal, routeOverride, componentOverride, nil
}
