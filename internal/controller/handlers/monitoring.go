package handlers

import (
	"context"
	"log/slog"
	"net/http"

	monitoringcapability "github.com/AlanD20/groundplane/internal/controller/monitoring"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type MonitoringReader interface {
	Show(context.Context, monitoringcapability.Query) (apiTypes.MonitoringResponse, error)
}

type monitoringShowInput struct {
	Scope          string `path:"scope" enum:"host,tenant,project,environment,service,backing"`
	ID             string `path:"id" minLength:"1" maxLength:"64"`
	Range          string `query:"range" enum:"1h,6h,24h,7d" default:"1h"`
	Points         int    `query:"points" minimum:"16" maximum:"720" default:"240"`
	Limit          int    `query:"limit" minimum:"1" maximum:"200" default:"25"`
	Cursor         string `query:"cursor" maxLength:"8192"`
	ConsumerCursor string `query:"consumer_cursor" maxLength:"8192"`
}

type monitoringShowOutput struct {
	Body apiTypes.MonitoringResponse
}

func (s *Server) registerMonitoring() {
	huma.Register(s.API, huma.Operation{
		OperationID: "monitoring.show", Method: http.MethodGet, Path: "/monitoring/{scope}/{id}",
		Summary: "Show live and historical monitoring for one scope", Tags: []string{"Monitoring"},
		Middlewares: huma.Middlewares{s.validateMonitoringQuery},
	}, s.showMonitoring)
}

func (s *Server) validateMonitoringQuery(ctx huma.Context, next func(huma.Context)) {
	allowed := map[string]bool{
		"range": true, "points": true, "limit": true, "cursor": true, "consumer_cursor": true,
	}
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if !allowed[key] || len(values) != 1 {
			if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, "Monitoring query is invalid"); err != nil &&
				s.Logger != nil {
				s.Logger.Error("controller: write Monitoring request problem", slog.Any("error", err))
			}
			return
		}
	}
	next(ctx)
}

func (s *Server) showMonitoring(
	ctx context.Context,
	input *monitoringShowInput,
) (*monitoringShowOutput, error) {
	if s.monitoring == nil {
		return nil, errs.New(errs.KindInternal, "monitoring reader is not configured")
	}
	response, err := s.monitoring.Show(ctx, monitoringcapability.Query{
		Kind: input.Scope, ID: input.ID, Range: input.Range, Points: input.Points, Limit: input.Limit,
		Cursor: input.Cursor, ConsumerCursor: input.ConsumerCursor,
	})
	if err != nil {
		return nil, err
	}
	return &monitoringShowOutput{Body: response}, nil
}
