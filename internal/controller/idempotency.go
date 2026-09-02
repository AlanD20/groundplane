package controller

import (
	"net/http"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const idempotencyKeyHeader = "Idempotency-Key"

func (s *Server) requestHandler() http.Handler {
	return s.dispatchRequests(http.HandlerFunc(s.serveAPIRequest))
}

func (s *Server) serveAPIRequest(w http.ResponseWriter, r *http.Request) {
	if requiresIdempotencyKey(r) && !hasValidIdempotencyKey(r.Header.Values(idempotencyKeyHeader)) {
		s.writeProblem(w, errs.New(
			errs.KindValidationFailed,
			"Idempotency-Key must occur exactly once and contain 16 to 128 characters matching [A-Za-z0-9._:-]+",
		))
		return
	}
	policy := s.policyFor(r)
	if policy.validateJSON != nil && !s.prepareJSONBody(
		w, r, productionJSONBodyLimit, policy.validateJSON,
	) {
		return
	}
	if (s.controllerTaskWake == nil && s.agentTaskWake == nil) || !mayAcceptTask(r.Method) {
		s.Mux.ServeHTTP(w, r)
		return
	}
	response := &acceptedTaskResponseWriter{ResponseWriter: w}
	s.Mux.ServeHTTP(response, r)
	if response.status == http.StatusAccepted {
		if s.controllerTaskWake != nil {
			s.controllerTaskWake()
		}
		if s.agentTaskWake != nil {
			s.agentTaskWake()
		}
	}
}

type acceptedTaskResponseWriter struct {
	http.ResponseWriter
	status int
}

func (writer *acceptedTaskResponseWriter) WriteHeader(status int) {
	if writer.status != 0 {
		return
	}
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *acceptedTaskResponseWriter) Write(body []byte) (int, error) {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(body)
}

func mayAcceptTask(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func requiresIdempotencyKey(r *http.Request) bool {
	if r.URL.Path != "/api/v1" && !strings.HasPrefix(r.URL.Path, "/api/v1/") {
		return false
	}
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		if r.Method == http.MethodPost && isBackupKeyExportPath(r.URL.Path) {
			return false
		}
		return true
	default:
		return false
	}
}

func isBackupKeyExportPath(path string) bool {
	const prefix = "/api/v1/environments/"
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	remainder := strings.TrimPrefix(path, prefix)
	environmentID, action, found := strings.Cut(remainder, "/")
	return found && environmentID != "" && action == "export-key"
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
