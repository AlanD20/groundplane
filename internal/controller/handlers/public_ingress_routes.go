package handlers

import (
	"context"
	"net/http"
	"net/netip"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

// PublicIngressOperator resolves Host authority and binds an accepted Apply
// Task to the exact reviewed candidate, current revision, and replay key.
type PublicIngressOperator interface {
	ReadPublicIngress(context.Context, string) (api.PublicIngressState, error)
	PreflightPublicIngress(context.Context, string, int64, api.PublicIngressDraft) (api.PublicIngressReview, error)
	ApplyPublicIngress(
		context.Context,
		string,
		int64,
		api.PublicIngressApplyRequest,
		string,
	) (idempotency.IdempotencyResponse, error)
}

type PublicIngressHostInput struct {
	HostID string `path:"id" pattern:"^host_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type PublicIngressReviewInput struct {
	PublicIngressHostInput
	IfMatch string `header:"If-Match" required:"true"`
}

type publicIngressPreflightInput struct {
	PublicIngressReviewInput
	Body api.PublicIngressDraft
}

type publicIngressApplyInput struct {
	PublicIngressReviewInput
	Key  string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body api.PublicIngressApplyRequest
}

type publicIngressStateOutput struct {
	ETag string `header:"ETag"`
	Body api.PublicIngressState
}

type publicIngressReviewOutput struct{ Body api.PublicIngressReview }

type publicIngressRoutes struct {
	server   *Server
	operator PublicIngressOperator
}

func (s *Server) registerPublicIngress(operator PublicIngressOperator) {
	const path = "/hosts/{id}/public-ingress"
	routes := publicIngressRoutes{server: s, operator: operator}
	huma.Register(s.API, huma.Operation{
		OperationID: "public-ingress.show", Method: http.MethodGet, Path: path,
		Summary: "Show public ingress configuration and eligible Routes for a Host",
		Tags:    []string{"Public Ingress"},
	}, routes.show)
	huma.Register(s.API, huma.Operation{
		OperationID: "public-ingress.preflight", Method: http.MethodPost, Path: path + "/preflight",
		Summary: "Review a public ingress configuration for a Host",
		Tags:    []string{"Public Ingress"}, MaxBodyBytes: 64 * 1024,
	}, routes.preflight)
	huma.Register(s.API, huma.Operation{
		OperationID: "public-ingress.apply", Method: http.MethodPost, Path: path + "/apply",
		Summary: "Apply a reviewed public ingress configuration to a Host",
		Tags:    []string{"Public Ingress"}, MaxBodyBytes: 64 * 1024,
		DefaultStatus: http.StatusAccepted,
		Responses: attachMutationResponses(
			openAPISchema[api.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted"),
		),
	}, routes.apply)
	s.setRoutePolicy("POST /api/v1"+path+"/preflight", routePolicy{body: jsonBody})
	s.setRoutePolicy("POST /api/v1"+path+"/apply", routePolicy{body: jsonBody})
}

func (routes publicIngressRoutes) show(
	ctx context.Context,
	input *PublicIngressHostInput,
) (*publicIngressStateOutput, error) {
	if routes.operator == nil {
		return nil, publicIngressUnavailable()
	}
	state, err := routes.operator.ReadPublicIngress(ctx, input.HostID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if state.Configuration.Addresses == nil {
		state.Configuration.Addresses = []string{}
	}
	if state.Configuration.RouteIDs == nil {
		state.Configuration.RouteIDs = []string{}
	}
	if state.Routes == nil {
		state.Routes = []api.PublicIngressRouteOption{}
	}
	return &publicIngressStateOutput{ETag: strconv.Quote(state.Configuration.Revision), Body: state}, nil
}

func (routes publicIngressRoutes) preflight(
	ctx context.Context,
	input *publicIngressPreflightInput,
) (*publicIngressReviewOutput, error) {
	if routes.operator == nil {
		return nil, publicIngressUnavailable()
	}
	revision, err := publicIngressRevision(input.IfMatch)
	if err != nil {
		return nil, err
	}
	if err := validatePublicIngressDraft(input.HostID, input.Body); err != nil {
		return nil, err
	}
	review, err := routes.operator.PreflightPublicIngress(ctx, input.HostID, revision, input.Body)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if review.Checks == nil {
		review.Checks = []api.PublicIngressCheck{}
	}
	return &publicIngressReviewOutput{Body: review}, nil
}

func (routes publicIngressRoutes) apply(
	ctx context.Context,
	input *publicIngressApplyInput,
) (*serviceMutationOutput, error) {
	if routes.operator == nil {
		return nil, publicIngressUnavailable()
	}
	revision, err := publicIngressRevision(input.IfMatch)
	if err != nil {
		return nil, err
	}
	if err := validatePublicIngressDraft(input.HostID, input.Body.PublicIngressDraft); err != nil {
		return nil, err
	}
	response, err := routes.operator.ApplyPublicIngress(ctx, input.HostID, revision, input.Body, input.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	if routes.server.controllerTaskWake != nil {
		routes.server.controllerTaskWake()
	}
	return routes.server.serviceMutationResponse(response), nil
}

func publicIngressUnavailable() error {
	return errs.New(errs.KindRequestUnavailable, "Public ingress is unavailable")
}

func publicIngressRevision(header string) (int64, error) {
	text, err := strconv.Unquote(header)
	revision, parseErr := strconv.ParseInt(text, 10, 64)
	if err != nil || parseErr != nil || revision < 0 || header != strconv.Quote(strconv.FormatInt(revision, 10)) {
		return 0, errs.New(errs.KindValidationFailed, "If-Match requires the quoted public ingress revision")
	}
	return revision, nil
}

func validatePublicIngressDraft(hostID string, draft api.PublicIngressDraft) error {
	if ids.Validate(ids.KindHost, hostID) != nil {
		return errs.New(errs.KindValidationFailed, "Public ingress requires a stable Host ID")
	}
	if draft.Enabled && (len(draft.Addresses) == 0 || len(draft.RouteIDs) == 0) {
		return errs.New(errs.KindValidationFailed, "Public ingress requires a bind address and at least one Route")
	}
	addresses := make(map[netip.Addr]struct{}, len(draft.Addresses))
	for _, input := range draft.Addresses {
		address, err := netip.ParseAddr(input)
		if err != nil || !address.IsGlobalUnicast() || address.Is4In6() || address.Zone() != "" {
			return errs.New(errs.KindValidationFailed, "Public ingress requires explicit unicast bind addresses")
		}
		if _, exists := addresses[address]; exists {
			return errs.New(errs.KindValidationFailed, "Public ingress bind addresses must be unique")
		}
		addresses[address] = struct{}{}
	}
	routeIDs := make(map[string]struct{}, len(draft.RouteIDs))
	for _, id := range draft.RouteIDs {
		if ids.Validate(ids.KindRoute, id) != nil {
			return errs.New(errs.KindValidationFailed, "Public ingress requires stable Route IDs")
		}
		if _, exists := routeIDs[id]; exists {
			return errs.New(errs.KindValidationFailed, "Public ingress Route IDs must be unique")
		}
		routeIDs[id] = struct{}{}
	}
	return nil
}
