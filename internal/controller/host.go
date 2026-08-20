package controller

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type hostOutput struct {
	Body api.Host
}

func (s *Server) registerHost() {
	huma.Register(s.API, huma.Operation{
		OperationID: "host-show",
		Method:      http.MethodGet,
		Path:        "/host",
		Summary:     "Show host health",
		Tags:        []string{"Host"},
	}, s.showHost)
}

func (s *Server) showHost(ctx context.Context, _ *struct{}) (*hostOutput, error) {
	if s.host == nil {
		return nil, errs.New(errs.KindInternal, "host service is not configured")
	}
	host, err := s.host.Show(ctx)
	if err != nil {
		return nil, err
	}
	return &hostOutput{Body: host}, nil
}
