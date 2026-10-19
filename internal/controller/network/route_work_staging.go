package network

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeWorkStageRequest struct {
	Current      keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]
	Route        routes.Record
	Action       workexecution.Action
	ReadRevision int64
	Locator      idempotency.IdempotencyLocator
	Evidence     routeMutationEvidence
	Idempotency  routeMutationIdempotency
	CreatedAt    time.Time
}

type stagedRouteWork struct {
	Claim         blueprints.EnvironmentBlueprintStageClaim
	Candidate     environmentprojection.EnvironmentComposeProjection
	Route         routes.Record
	EpochRevision int64
	Provider      *environmentchanges.RouteProviderPin
}

func (publisher *RouteWorkPublisher) stage(
	ctx context.Context,
	request routeWorkStageRequest,
) (stagedRouteWork, error) {
	current := request.Current
	if request.Idempotency == nil || request.CreatedAt.IsZero() ||
		request.Route.EnvironmentID != current.Record.EnvironmentID ||
		(request.Action != workexecution.ActionCreate && request.Action != workexecution.ActionUpdate && request.Action != workexecution.ActionRemove) {
		return stagedRouteWork{}, errs.New(errs.KindValidationFailed, "Route staged action is invalid")
	}
	claim, err := desiredrevision.Claim(ctx, publisher.desired, desiredrevision.ClaimInput{
		EnvironmentID: current.Record.EnvironmentID, CandidateTaskID: ids.New(ids.KindTask),
		Locator: request.Locator, Intent: request.Evidence.durable,
		MatchExistingIntent: func(ctx context.Context, existing idempotency.ProtectedIntentRecord) (bool, error) {
			return request.Idempotency.MatchesStaged(ctx, request.Evidence, existing)
		},
		BaselineHeadRevision: current.Revision, SourceKind: blueprints.EnvironmentBlueprintSourceMutation,
		RenderGeneration: current.Record.RenderGeneration + 1, CreatedAt: request.CreatedAt,
	})
	if err != nil {
		return stagedRouteWork{}, err
	}
	result, err := publisher.stageClaimed(ctx, request, claim)
	if err != nil {
		return stagedRouteWork{}, desiredrevision.AbandonClaim(ctx, publisher.desired, claim, err)
	}
	return result, nil
}

func (publisher *RouteWorkPublisher) stageClaimed(ctx context.Context, request routeWorkStageRequest,
	claim blueprints.EnvironmentBlueprintStageClaim,
) (stagedRouteWork, error) {
	record := request.Route
	if request.Action == workexecution.ActionCreate {
		desired := record.Desired
		desired.ID = ids.DeriveAt(ids.KindRoute, claim.CreatedAt, claim.TaskID, "route")
		var err error
		record, err = routes.NewRecord(record.EnvironmentID, desired)
		if err != nil {
			return stagedRouteWork{}, err
		}
	}
	audit := blueprints.EnvironmentRouteMutationAudit{
		BaseRevisionID: request.Current.Record.RevisionID,
		RouteID:        record.Desired.ID,
	}
	var candidate environmentprojection.EnvironmentComposeProjection
	if request.Action == workexecution.ActionRemove {
		audit.Action = blueprints.EnvironmentRouteMutationRemove
		candidate = environmentprojection.CloneEnvironmentComposeProjection(request.Current.Record)
		found := false
		for index, route := range candidate.DesiredRoutes {
			if route.Desired.ID == record.Desired.ID {
				candidate.DesiredRoutes = append(candidate.DesiredRoutes[:index], candidate.DesiredRoutes[index+1:]...)
				found = true
				break
			}
		}
		if !found {
			return stagedRouteWork{}, errs.New(errs.KindStateConflict, "Route is absent from the captured Environment")
		}
		candidate.RenderGeneration++
	} else {
		var err error
		candidate, err = environmentprojection.ApplyEnvironmentRoute(request.Current.Record, record)
		if err != nil {
			return stagedRouteWork{}, err
		}
		audit.Action = blueprints.EnvironmentRouteMutationEdit
		audit.Request = &blueprints.EnvironmentRouteMutationRequest{Exposure: record.Desired.Exposure}
		if request.Action == workexecution.ActionCreate {
			audit.Action = blueprints.EnvironmentRouteMutationCreate
			audit.Request = &blueprints.EnvironmentRouteMutationRequest{
				EnvironmentID: record.EnvironmentID, Host: record.Desired.Host, Path: record.Desired.Path,
				Exposure: record.Desired.Exposure, TargetServiceID: record.Desired.TargetServiceID, TargetPort: record.Desired.TargetPort,
			}
		}
	}
	candidate.RevisionID = claim.RevisionID
	provider, err := publisher.providers.Capture(ctx, candidate.EnvironmentID, request.Current.Revision,
		candidate, nil, "", candidate.RenderGeneration)
	if err != nil {
		return stagedRouteWork{}, err
	}
	readRevision := request.ReadRevision
	if provider != nil {
		readRevision = provider.InputRevision
	}
	if request.Action != workexecution.ActionRemove {
		observation := routes.Observation{Status: routes.ObservedUnserved, DesiredGeneration: record.DesiredGeneration}
		if provider != nil {
			observation.Status = routes.ObservedPending
			observation.Provider = routes.ProviderObservation{
				ComponentID: provider.ComponentID, DefinitionDigest: provider.DefinitionDigest, CatalogDigest: provider.CatalogDigest,
				InputRevision: provider.InputRevision, InputGeneration: provider.InputGeneration,
			}
		}
		record, err = routes.SetObservation(record, observation)
		if err != nil {
			return stagedRouteWork{}, err
		}
	}
	epoch, err := publisher.desired.StageRouteWorkRevision(ctx, desiredrevisionstore.RouteWorkStage{
		Claim: claim, Current: request.Current, Candidate: candidate, Audit: audit, ReadRevision: readRevision,
	})
	if err != nil {
		return stagedRouteWork{}, err
	}
	return stagedRouteWork{
		Claim:         claim,
		Candidate:     candidate,
		Route:         record,
		EpochRevision: epoch,
		Provider:      provider,
	}, nil
}
