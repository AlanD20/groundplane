package controller

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentDeleter interface {
	DeleteEnvironment(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

func (s *Server) environmentDelete(w http.ResponseWriter, r *http.Request) {
	if s.environmentDeletions == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Environment deleter is not configured"))
		return
	}
	environmentID := r.PathValue("id")
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		s.writeProjectProblem(w, errs.New(errs.KindMalformedRequest, "Environment id is invalid"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.writeProjectProblem(w, errs.New(errs.KindMalformedRequest, "Environment deletion query is invalid"))
		return
	}
	if err := validateBodylessAgentMutation(r); err != nil {
		s.writeProjectProblem(w, errs.New(errs.KindMalformedRequest, "Environment deletion body is not allowed"))
		return
	}
	response, err := s.environmentDeletions.DeleteEnvironment(
		r.Context(), environmentID, r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Environment deletion response", slog.Any("error", err))
	}
}
