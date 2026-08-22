package controller

import (
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type zoneRemovalImpactReader interface {
	GetZoneRemovalImpact(context.Context, string) (apiTypes.ZoneRemovalImpact, error)
}

type zoneRemovalImpactInput struct {
	ID string `path:"id" pattern:"^net_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type zoneRemovalImpactOutput struct {
	Body apiTypes.ZoneRemovalImpact
}

func (s *Server) registerZoneRemovalImpact() {
	huma.Register(s.API, huma.Operation{
		OperationID: "zone.removal-impact", Method: http.MethodGet, Path: "/zones/{id}/removal-impact",
		Summary: "Preview the exact impact of removing a network zone", Tags: []string{"Zone"},
	}, s.getZoneRemovalImpact)
}

func (s *Server) getZoneRemovalImpact(
	ctx context.Context,
	request *zoneRemovalImpactInput,
) (*zoneRemovalImpactOutput, error) {
	reader, ok := s.zoneMutations.(zoneRemovalImpactReader)
	if !ok || reader == nil {
		return nil, errs.New(errs.KindInternal, "Zone removal impact reader is not configured")
	}
	impact, err := reader.GetZoneRemovalImpact(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &zoneRemovalImpactOutput{Body: impact}, nil
}
