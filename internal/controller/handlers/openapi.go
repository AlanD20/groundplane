package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// OpenAPIDocument serializes the code-first human API contract in its one
// committed representation. JSON map keys are sorted by encoding/json, making
// repeated generation byte-for-byte deterministic for an unchanged server.
func (s *Server) OpenAPIDocument() ([]byte, error) {
	if s == nil || s.API == nil || s.API.OpenAPI() == nil {
		return nil, errs.New(errs.KindInternal, "Controller OpenAPI is not configured")
	}
	document, err := json.MarshalIndent(s.API.OpenAPI(), "", "  ")
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("marshal Controller OpenAPI: %w", err))
	}
	return append(document, '\n'), nil
}

func (s *Server) openAPI(w http.ResponseWriter, _ *http.Request) {
	document, err := s.OpenAPIDocument()
	if err != nil {
		s.writeProblem(w, errs.Wrap(errs.KindInternal, err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(document); err != nil && s.Logger != nil {
		s.Logger.Error("write OpenAPI document", "error", err)
	}
}
