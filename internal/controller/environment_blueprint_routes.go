package controller

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentBlueprintMutator applies one verified closed Blueprint bundle to
// an existing Environment and returns the accepted reconcile Task.
type EnvironmentBlueprintMutator interface {
	ApplyBlueprint(context.Context, string, core.BlueprintBundle, string) (etcd.IdempotencyResponse, error)
}

func (s *Server) environmentBlueprintApply(w http.ResponseWriter, r *http.Request) {
	if s.environmentBlueprints == nil {
		s.writeProblem(w, errs.New(errs.KindInternal, "Environment Blueprint mutator is not configured"))
		return
	}
	if len(r.URL.Query()) != 0 {
		s.writeProjectProblem(w, errs.New(errs.KindMalformedRequest, "Environment Blueprint query is invalid"))
		return
	}
	environmentID := r.PathValue("id")
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		s.writeProjectProblem(w, errs.New(errs.KindValidationFailed, "Environment id is invalid"))
		return
	}
	bundle, err := decodeBlueprintMultipart(r)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	defer clearBlueprintBundle(bundle)
	response, err := s.environmentBlueprints.ApplyBlueprint(
		r.Context(), environmentID, bundle, r.Header.Get(idempotencyKeyHeader),
	)
	if err != nil {
		s.writeProjectProblem(w, err)
		return
	}
	w.Header().Set("Content-Type", response.ContentKind)
	w.WriteHeader(response.Status)
	if _, err := w.Write(response.Body); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write Environment Blueprint response", slog.Any("error", err))
	}
}

func clearBlueprintBundle(bundle core.BlueprintBundle) {
	for index := range bundle.Files {
		clear(bundle.Files[index].Content)
	}
}
