package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/controller/remoteimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type RemoteImageFetcher interface {
	Fetch(context.Context, string, string, string, []string) (idempotency.IdempotencyResponse, error)
}

type remoteImageInput struct {
	HostID         string `path:"id" pattern:"^host_[0-9A-HJKMNP-TV-Z]{26}$"`
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.RemoteImageFetchRequest
}

func (s *Server) registerRemoteImage(fetcher RemoteImageFetcher) {
	schema := openAPISchema[apiTypes.TaskAccepted](s.API.OpenAPI().Components.Schemas, "TaskAccepted")
	huma.Register(s.API, huma.Operation{
		OperationID: "remote-host.fetch-image", Method: http.MethodPost, Path: remoteimage.Route,
		Summary: "Fetch an exact public image manifest into an enrolled remote Host", Tags: []string{"Remote Hosts"},
		DefaultStatus: http.StatusAccepted, Responses: attachMutationResponses(schema),
		Middlewares: huma.Middlewares{s.rejectImageFetchQuery},
	}, func(ctx context.Context, input *remoteImageInput) (*imageFetchOutput, error) {
		if fetcher == nil {
			return nil, errs.New(errs.KindRequestUnavailable, "Remote image Fetch is unavailable")
		}
		response, err := fetcher.Fetch(ctx, input.HostID, input.Body.Image, input.IdempotencyKey, input.Body.After)
		if err != nil {
			return nil, normalizeProjectError(err)
		}
		if s.controllerTaskWake != nil {
			s.controllerTaskWake()
		}
		return s.imageAcceptance(response), nil
	})
	s.setRoutePolicy(
		"POST /api/v1"+remoteimage.Route,
		routePolicy{body: jsonBody, validateJSON: validateRemoteImageJSON},
	)
}

func validateRemoteImageJSON(content []byte) error {
	canonical, err := jcs.Canonicalize(content)
	if err != nil {
		return err
	}
	_, err = jcs.Decode[apiTypes.RemoteImageFetchRequest](canonical)
	return err
}
