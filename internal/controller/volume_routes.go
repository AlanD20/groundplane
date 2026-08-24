package controller

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type VolumeReader interface {
	ListVolumes(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.VolumeRecord], error)
}

type volumeListInput struct {
	Environment string `query:"environment" required:"true" pattern:"^env_[0-9A-HJKMNP-TV-Z]{26}$"`
	Limit       int    `query:"limit" required:"false"`
	Cursor      string `query:"cursor" required:"false"`
}

type volumePageOutput struct {
	Body apiTypes.Page[apiTypes.Volume]
}

func (s *Server) registerVolumes() {
	huma.Register(s.API, huma.Operation{
		OperationID: "volume.list", Method: http.MethodGet, Path: "/volumes",
		Summary: "List Environment volumes", Tags: []string{"Volume"},
	}, s.listVolumes)
}

func (s *Server) listVolumes(ctx context.Context, request *volumeListInput) (*volumePageOutput, error) {
	if s.volumes == nil {
		return nil, errs.New(errs.KindInternal, "volume reader is not configured")
	}
	pageRequest, err := serviceListRequest(request.Environment, request.Limit, request.Cursor)
	if err != nil {
		return nil, err
	}
	page, err := s.volumes.ListVolumes(ctx, request.Environment, pageRequest)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	items := make([]apiTypes.Volume, len(page.Items))
	for index, item := range page.Items {
		items[index] = apiTypes.Volume{ID: item.Record.ID, Name: item.Record.Name}
	}
	return &volumePageOutput{Body: apiTypes.Page[apiTypes.Volume]{Items: items, NextCursor: page.NextCursor}}, nil
}
