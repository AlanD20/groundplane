package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) hostResolutionTerminalOverlay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) (string, map[string]routerecord.Record, map[string]componentrecord.Record, error) {
	removal := ""
	routeOverride := make(map[string]routerecord.Record)
	componentOverride := make(map[string]componentrecord.Record)
	switch task.Params[taskjournal.TaskResourceKindParam] {
	case taskjournal.TaskResourceRoute:
		mutationRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{environmentchanges.RouteMutationIntentKey(task.ID)}, Revision: revision,
		})
		if err != nil {
			return "", nil, nil, err
		}
		if mutationRead == nil || mutationRead.ReadRevision != revision || len(mutationRead.Values) != 1 {
			return "", nil, nil, errs.New(errs.KindInternal, "host-resolution Route intent read is incomplete")
		}
		if mutationRead.Values[0] == nil {
			removalRead, removalErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: []string{environmentchanges.RouteRemovalIntentKey(task.ID)}, Revision: revision,
			})
			if removalErr != nil {
				return "", nil, nil, removalErr
			}
			if removalRead == nil || removalRead.ReadRevision != revision || len(removalRead.Values) != 1 {
				return "", nil, nil, errs.New(
					errs.KindInternal,
					"host-resolution Route removal intent read is incomplete",
				)
			}
			if removalRead.Values[0] != nil {
				intent, decodeErr := environmentchanges.DecodeRouteRemovalIntent(removalRead.Values[0].Value)
				if decodeErr != nil {
					return "", nil, nil, decodeErr
				}
				if terminalStatus == taskjournal.TaskStatusCompleted {
					removal = intent.RouteID
				}
			}
		} else {
			intent, decodeErr := environmentchanges.DecodeRouteMutationIntent(mutationRead.Values[0].Value)
			if decodeErr != nil {
				return "", nil, nil, decodeErr
			}
			if terminalStatus == taskjournal.TaskStatusCompleted {
				route := intent.Route
				if intent.Provider != nil {
					route.Observed.Provider = routerecord.ProviderObservation{
						ComponentID: intent.Provider.ComponentID, DefinitionDigest: intent.Provider.DefinitionDigest,
						CatalogDigest: intent.Provider.CatalogDigest, InputRevision: intent.Provider.InputRevision,
						InputGeneration: intent.Provider.InputGeneration,
					}
					route.Observed.Status = routerecord.ObservedServed
				}
				routeOverride[route.Desired.ID] = route
			}
		}
	case taskjournal.TaskResourceComponent:
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{componentTaskIntentKey(task.ID)}, Revision: revision,
		})
		if err != nil {
			return "", nil, nil, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			return "", nil, nil, errs.New(errs.KindInternal, "host-resolution Component intent read is incomplete")
		}
		if read.Values[0] != nil {
			intent, decodeErr := decodeComponentTaskIntent(read.Values[0].Value)
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
			if intent.RouteProjection != nil {
				for _, candidate := range intent.RouteProjection.Routes {
					route, routeErr := repository.routeAtRevision(ctx, candidate.Desired.ID, revision)
					if routeErr != nil {
						return "", nil, nil, routeErr
					}
					if route.EnvironmentID != intent.EnvironmentID ||
						route.DesiredGeneration != candidate.DesiredGeneration ||
						!routeDesiredEqual(route.Desired, candidate.Desired) {
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
