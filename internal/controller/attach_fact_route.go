package controller

import (
	"context"
	"net/http"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/danielgtaylor/huma/v2"
)

type AttachFactReader interface {
	RevealAttachFact(context.Context, string, string, string) (string, error)
}

type attachFactRevealInput struct {
	ID            string `path:"id" pattern:"^att_[0-9A-HJKMNP-TV-Z]{26}$"`
	Key           string `path:"key" pattern:"^[A-Za-z][A-Za-z0-9_]{0,127}$"`
	GrantAttachID string `query:"grant_attach_id" required:"false" pattern:"^att_[0-9A-HJKMNP-TV-Z]{26}$"`
}

type attachFactValueOutput struct {
	Body apiTypes.AttachFactValue
}

func (s *Server) registerAttachFactReveal() {
	huma.Register(s.API, huma.Operation{
		OperationID: "attach.fact.reveal", Method: http.MethodGet, Path: "/attaches/{id}/facts/{key}",
		Summary: "Reveal one ready Attach fact", Tags: []string{"Attach"},
		Middlewares: huma.Middlewares{s.validateAttachFactQuery},
	}, s.revealAttachFact)
}

func (s *Server) revealAttachFact(
	ctx context.Context,
	request *attachFactRevealInput,
) (*attachFactValueOutput, error) {
	if s.attachFacts == nil {
		return nil, errs.New(errs.KindInternal, "Attach fact reader is not configured")
	}
	value, err := s.attachFacts.RevealAttachFact(ctx, request.ID, request.GrantAttachID, request.Key)
	if err != nil {
		return nil, normalizeProjectError(err)
	}
	return &attachFactValueOutput{Body: apiTypes.AttachFactValue{Value: value}}, nil
}

func (s *Server) validateAttachFactQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	for key, values := range requestURL.Query() {
		if key != "grant_attach_id" || len(values) != 1 {
			s.writeAttachListProblem(ctx, "Attach fact query is invalid")
			return
		}
	}
	next(ctx)
}
