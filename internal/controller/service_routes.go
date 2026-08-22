package controller

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ServiceReader interface {
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
}

type serviceListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit" required:"false"`
	Cursor      string `query:"cursor" required:"false"`
}

type servicePageOutput struct {
	Body apiTypes.Page[apiTypes.Service]
}

func (s *Server) registerServices() {
	huma.Register(s.API, huma.Operation{
		OperationID: "service.list", Method: http.MethodGet, Path: "/services",
		Summary: "List services", Tags: []string{"Service"},
		Middlewares: huma.Middlewares{s.validateServiceListQuery},
	}, s.listServices)
}

func (s *Server) listServices(
	ctx context.Context,
	request *serviceListInput,
) (*servicePageOutput, error) {
	if s.services == nil {
		return nil, errs.New(errs.KindInternal, "Service reader is not configured")
	}
	pageRequest, err := serviceListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.services.ListServices(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Service]{
		Items: make([]apiTypes.Service, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = serviceResponse(item.Record)
	}
	return &servicePageOutput{Body: response}, nil
}

func serviceListRequest(environmentID string, limit int, cursor string) (etcd.PageRequest, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Service list requires a stable Environment id",
		)
	}
	if limit < 0 {
		return etcd.PageRequest{}, errs.New(errs.KindValidationFailed, "Service list limit must be a positive integer")
	}
	return etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func serviceResponse(record etcd.ServiceRecord) apiTypes.Service {
	return apiTypes.Service{
		ID: record.Desired.ID, Name: record.Desired.Name, Image: record.Desired.Image,
		RuntimeIntent: apiTypes.ServiceRuntimeIntent(record.Runtime.RuntimeIntent),
		Zones:         append([]string(nil), record.Desired.Zones...), Strategy: string(record.Desired.Strategy),
		OnFailure: apiTypes.OnFailure(record.Desired.OnFailure), Replicas: record.Desired.Replicas,
		Adapter: record.Desired.Adapter, FactsPrefix: record.Desired.FactsPrefix,
		BackingNetworkID: record.BackingNetworkID,
	}
}

func (s *Server) validateServiceListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "environment" && key != "limit" && key != "cursor" {
			s.writeServiceProblem(ctx, "Service list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeServiceProblem(ctx, "Service list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) writeServiceProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Service request problem", slog.Any("error", err))
	}
}
