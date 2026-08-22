package controller

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ZoneReader interface {
	GetZone(context.Context, string) (etcd.Versioned[etcd.ZoneRecord], error)
	ListZones(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ZoneRecord], error)
}

type zoneListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit" required:"false"`
	Cursor      string `query:"cursor" required:"false"`
}

type zoneShowInput struct {
	ID string `path:"id" pattern:"^net_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type zoneOutput struct {
	Body apiTypes.Zone
}

type zonePageOutput struct {
	Body apiTypes.Page[apiTypes.Zone]
}

func (s *Server) registerZones() {
	huma.Register(s.API, huma.Operation{
		OperationID: "zone.list", Method: http.MethodGet, Path: "/zones",
		Summary: "List network zones", Tags: []string{"Zone"},
		Middlewares: huma.Middlewares{s.validateZoneListQuery},
	}, s.listZones)
	huma.Register(s.API, huma.Operation{
		OperationID: "zone.show", Method: http.MethodGet, Path: "/zones/{id}",
		Summary: "Show a network zone", Tags: []string{"Zone"},
	}, s.showZone)
}

func (s *Server) listZones(
	ctx context.Context,
	request *zoneListInput,
) (*zonePageOutput, error) {
	if s.zones == nil {
		return nil, errs.New(errs.KindInternal, "Zone reader is not configured")
	}
	pageRequest, err := zoneListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	page, err := s.zones.ListZones(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	response := apiTypes.Page[apiTypes.Zone]{
		Items: make([]apiTypes.Zone, len(page.Items)), NextCursor: page.NextCursor,
	}
	for index, item := range page.Items {
		response.Items[index] = zoneResponse(item.Record)
	}
	return &zonePageOutput{Body: response}, nil
}

func (s *Server) showZone(
	ctx context.Context,
	request *zoneShowInput,
) (*zoneOutput, error) {
	if s.zones == nil {
		return nil, errs.New(errs.KindInternal, "Zone reader is not configured")
	}
	zone, err := s.zones.GetZone(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &zoneOutput{Body: zoneResponse(zone.Record)}, nil
}

func zoneListRequest(environmentID string, limit int, cursor string) (etcd.PageRequest, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.PageRequest{}, errs.New(
			errs.KindValidationFailed,
			"Zone list requires a stable Environment id",
		)
	}
	if limit < 0 {
		return etcd.PageRequest{}, errs.New(errs.KindValidationFailed, "Zone list limit must be a positive integer")
	}
	return etcd.PageRequest{Limit: limit, Cursor: cursor}, nil
}

func zoneResponse(record etcd.ZoneRecord) apiTypes.Zone {
	return apiTypes.Zone{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Name: record.Desired.Name,
		Subnet: record.Desired.Subnet, Internal: record.Desired.Internal,
		OwnerKind: zoneOwnerKindResponse(record.Desired.OwnerKind), OwnerID: record.Desired.OwnerID,
	}
}

func zoneOwnerKindResponse(kind core.ZoneOwnerKind) apiTypes.ZoneOwnerKind {
	return apiTypes.ZoneOwnerKind(kind)
}

func (s *Server) validateZoneListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "environment" && key != "limit" && key != "cursor" {
			s.writeZoneProblem(ctx, "Zone list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeZoneProblem(ctx, "Zone list query contains duplicate values")
			return
		}
	}
	next(ctx)
}

func (s *Server) writeZoneProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Zone request problem", slog.Any("error", err))
	}
}
