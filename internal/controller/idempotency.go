package controller

import (
	"net/http"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const idempotencyKeyHeader = "Idempotency-Key"

func (s *Server) requestHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requiresIdempotencyKey(r) && !hasValidIdempotencyKey(r.Header.Values(idempotencyKeyHeader)) {
			s.writeProblem(w, errs.New(
				errs.CodeValidationFailed,
				"Idempotency-Key must occur exactly once and contain 16 to 128 characters matching [A-Za-z0-9._:-]+",
			))
			return
		}
		s.Mux.ServeHTTP(w, r)
	})
}

func requiresIdempotencyKey(r *http.Request) bool {
	if r.URL.Path != "/api/v1" && !strings.HasPrefix(r.URL.Path, "/api/v1/") {
		return false
	}
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func hasValidIdempotencyKey(values []string) bool {
	if len(values) != 1 {
		return false
	}
	value := values[0]
	if len(value) < 16 || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		char := value[i]
		if (char >= 'A' && char <= 'Z') ||
			(char >= 'a' && char <= 'z') ||
			(char >= '0' && char <= '9') {
			continue
		}
		switch char {
		case '.', '_', ':', '-':
			continue
		default:
			return false
		}
	}
	return true
}
