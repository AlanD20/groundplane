package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func validatePromotionRequest(request domain.ServiceDeployInput, scoped bool) error {
	if scoped {
		return errs.New(errs.KindRequestForbidden, "promotion requires an Operator Deploy")
	}
	if ids.Validate(ids.KindDeployment, request.SourceReleaseID) != nil ||
		request.Image != "" || request.Tag != "" || request.HostID != "" ||
		request.Strategy != "" || request.OnFailure != "" {
		return errs.New(
			errs.KindValidationFailed,
			"promotion requires one Release ID and uses the destination rollout policy",
		)
	}
	return nil
}

func (service *Service) promotionCandidate(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	planning releasequeries.ReleasePlanningService, request acceptedServiceDeploy,
) (releaseCandidateInput, error) {
	if err := validatePromotionRequest(request.ServiceDeployInput, request.Source != nil); err != nil {
		return releaseCandidateInput{}, err
	}
	if scope.Tenant.Record.ID == "" || planning.Service.Record.BackingNetworkID != "" {
		return releaseCandidateInput{}, errs.New(errs.KindValidationFailed, "promotion requires an application Service")
	}
	source, guards, err := service.ledger.SelectPromotion(ctx, scope, request.SourceReleaseID)
	if err != nil {
		return releaseCandidateInput{}, err
	}
	candidate, err := service.historicalImageCandidate(ctx, scope, planning, source, "")
	candidate.promotionSource = source.ID
	candidate.promotionGuards = guards
	return candidate, err
}
