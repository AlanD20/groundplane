package controller

import (
	"context"
	"net/http"

	corenetwork "github.com/AlanD20/groundplane/internal/core/network"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type zoneRemovalImpactReader interface {
	GetZoneRemovalImpact(context.Context, string) (corenetwork.ZoneRemovalImpact, error)
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
	reader, ok := s.zones.(zoneRemovalImpactReader)
	if !ok || reader == nil {
		return nil, errs.New(errs.KindInternal, "Zone removal impact reader is not configured")
	}
	impact, err := reader.GetZoneRemovalImpact(ctx, request.ID)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &zoneRemovalImpactOutput{Body: zoneRemovalImpactResponse(impact)}, nil
}

func zoneRemovalImpactResponse(impact corenetwork.ZoneRemovalImpact) apiTypes.ZoneRemovalImpact {
	response := apiTypes.ZoneRemovalImpact{
		ZoneID: impact.ZoneID, ZoneName: impact.ZoneName,
		Mode: apiTypes.ZoneRemovalImpactMode(impact.Mode), ImpactToken: impact.ImpactToken,
		Attaches:  make([]apiTypes.ZoneRemovalImpactAttach, len(impact.Attaches)),
		Services:  make([]apiTypes.ZoneRemovalImpactService, len(impact.Services)),
		Databases: make([]apiTypes.ZoneRemovalImpactDatabase, len(impact.Databases)),
	}
	for index, attach := range impact.Attaches {
		response.Attaches[index] = apiTypes.ZoneRemovalImpactAttach{
			ID: attach.ID, Name: attach.Name, EnvironmentID: attach.EnvironmentID,
			ServiceID: attach.ServiceID, Database: attach.Database, Status: attach.Status,
		}
	}
	for index, service := range impact.Services {
		response.Services[index] = apiTypes.ZoneRemovalImpactService{
			ID: service.ID, Name: service.Name, EnvironmentID: service.EnvironmentID,
		}
	}
	for index, database := range impact.Databases {
		response.Databases[index] = apiTypes.ZoneRemovalImpactDatabase{
			AttachID: database.AttachID, Name: database.Name,
		}
	}
	return response
}
