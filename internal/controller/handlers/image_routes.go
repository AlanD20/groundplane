package handlers

import (
	"context"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/controller/imagedelivery"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type ImageFetcher interface {
	FetchImage(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
}

type imageFetchInput struct {
	IdempotencyKey string `header:"Idempotency-Key" required:"true" minLength:"16" maxLength:"128" pattern:"^[A-Za-z0-9._:-]+$"`
	Body           apiTypes.ImageFetchRequest
}

type imageFetchOutput struct {
	Status      int
	ContentType string `header:"Content-Type"`
	Body        func(huma.Context)
}

func (s *Server) registerImageFetch() {
	schema := openAPISchema[apiTypes.ImageFetchAccepted](s.API.OpenAPI().Components.Schemas, "ImageFetchAccepted")
	huma.Register(s.API, huma.Operation{
		OperationID: "image.fetch", Method: http.MethodPost, Path: imagedelivery.FetchRoute,
		Summary: "Fetch selected private-registry content into host Docker without deploying", Tags: []string{"Images"},
		DefaultStatus: http.StatusAccepted, Responses: attachMutationResponses(schema),
		Middlewares: huma.Middlewares{s.rejectImageFetchQuery},
	}, s.fetchImage)
	s.setRoutePolicy("POST /api/v1/images/fetch", routePolicy{body: jsonBody, validateJSON: validateImageFetchJSON})
}

func (s *Server) fetchImage(ctx context.Context, input *imageFetchInput) (*imageFetchOutput, error) {
	if s.images == nil {
		return nil, errs.New(errs.KindInternal, "image fetch service is not configured")
	}
	response, err := s.images.FetchImage(ctx, input.Body.Image, input.IdempotencyKey)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &imageFetchOutput{Status: response.Status, ContentType: response.ContentKind, Body: func(ctx huma.Context) {
		ctx.SetStatus(response.Status)
		if _, err := ctx.BodyWriter().Write(response.Body); err != nil && s.Logger != nil {
			s.Logger.Error("controller: write image fetch acceptance", "error", err)
		}
	}}, nil
}

func (s *Server) rejectImageFetchQuery(ctx huma.Context, next func(huma.Context)) {
	if ctx.URL().RawQuery != "" {
		if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, "image fetch does not accept query parameters"); err != nil &&
			s.Logger != nil {
			s.Logger.Error("controller: write image fetch rejection", "error", err)
		}
		return
	}
	next(ctx)
}

func validateImageFetchJSON(raw []byte) error {
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return err
	}
	_, err = jcs.Decode[apiTypes.ImageFetchRequest](canonical)
	return err
}
