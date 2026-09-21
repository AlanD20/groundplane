package handlers

import (
	"errors"
	"github.com/danielgtaylor/huma/v2"
	"io"
	"log/slog"
	"net/http"
)

func (s *Server) rejectEntryQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	if len(requestURL.Query()) != 0 {
		s.writeEntryProblem(ctx, "entry request query is invalid")
		return
	}
	next(ctx)
}

func (s *Server) rejectEntryDeleteBody(ctx huma.Context, next func(huma.Context)) {
	var probe [1]byte
	count, err := ctx.BodyReader().Read(probe[:])
	if count != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		s.writeEntryProblem(ctx, "entry removal body is not allowed")
		return
	}
	next(ctx)
}

func (s *Server) validateEntryListQuery(ctx huma.Context, next func(huma.Context)) {
	requestURL := ctx.URL()
	query := requestURL.Query()
	for key, values := range query {
		if key != "environment" && key != "limit" && key != "cursor" {
			s.writeEntryProblem(ctx, "entry list query is invalid")
			return
		}
		if len(values) != 1 {
			s.writeEntryProblem(ctx, "entry list query contains duplicate values")
			return
		}
	}
	environments, present := query["environment"]
	if !present || len(environments) != 1 || environments[0] == "" {
		s.writeEntryProblem(ctx, "entry list requires one Environment selector")
		return
	}
	next(ctx)
}

func (s *Server) writeEntryProblem(ctx huma.Context, detail string) {
	if err := huma.WriteErr(s.API, ctx, http.StatusBadRequest, detail); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Entry request problem", slog.Any("error", err))
	}
}
