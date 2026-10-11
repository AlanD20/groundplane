package handlers

import (
	"log/slog"
	"net/http"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/problemresponse"
)

// A request reference identifies this HTTP attempt, not a Task or replay key.
// Generate it locally; do not accept caller-supplied identifiers or log bodies,
// query strings, credentials, terminal input or arbitrary request paths.
func (s *Server) withRequestReference(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reference := ids.NewULID()
		w.Header().Set(problemresponse.RequestReferenceHeader, reference)
		if s.Logger != nil {
			_, pattern := s.Mux.Handler(r)
			s.Logger.Debug("controller: HTTP request", slog.String("request_reference", reference),
				slog.String("method", r.Method), slog.String("route", pattern))
		}
		next.ServeHTTP(w, r)
	})
}
