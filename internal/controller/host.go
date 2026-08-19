package controller

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/pkg/api"
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
	response := &hostOutput{}
	response.Body.Etcd.Endpoints = append([]string(nil), s.etcdEndpoints...)
	response.Body.Etcd.Healthy = s.Store != nil && s.Store.Health(ctx) == nil
	return response, nil
}
