package handlers

import (
	"errors"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

var consoleContentTypes = map[string]string{
	".avif":  "image/avif",
	".css":   "text/css; charset=utf-8",
	".gif":   "image/gif",
	".html":  "text/html; charset=utf-8",
	".ico":   "image/x-icon",
	".jpeg":  "image/jpeg",
	".jpg":   "image/jpeg",
	".js":    "text/javascript; charset=utf-8",
	".json":  "application/json",
	".map":   "application/json",
	".mjs":   "text/javascript; charset=utf-8",
	".png":   "image/png",
	".svg":   "image/svg+xml",
	".txt":   "text/plain; charset=utf-8",
	".webp":  "image/webp",
	".woff":  "font/woff",
	".woff2": "font/woff2",
}

var controllerRouteMethods = [...]string{
	http.MethodGet,
	http.MethodHead,
	http.MethodPost,
	http.MethodPut,
	http.MethodPatch,
	http.MethodDelete,
	http.MethodOptions,
}

func (s *Server) dispatchRequests(api http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assetPath, ok := canonicalConsolePath(r)
		if !ok {
			s.writeStaticProblem(w, errs.KindRequestNotFound, "request path was not found")
			return
		}
		if controllerOwnsPath(r.URL.Path) || s.controllerRouteExists(r) {
			s.serveControllerRoute(w, r, api)
			return
		}
		s.serveConsole(w, r, assetPath)
	})
}

func (s *Server) controllerRouteExists(r *http.Request) bool {
	if _, pattern := s.Mux.Handler(r); pattern != "" {
		return true
	}
	return s.controllerPathHasAnotherMethod(r)
}

func controllerOwnsPath(requestPath string) bool {
	return requestPath == "/api" || strings.HasPrefix(requestPath, "/api/") || requestPath == "/openapi.json"
}

func (s *Server) serveControllerRoute(w http.ResponseWriter, r *http.Request, api http.Handler) {
	if _, pattern := s.Mux.Handler(r); pattern != "" {
		api.ServeHTTP(w, r)
		return
	}
	if s.controllerPathHasAnotherMethod(r) {
		s.writeStaticProblem(w, errs.KindRequestMethodNotAllowed, "request method is not allowed")
		return
	}
	s.writeStaticProblem(w, errs.KindRequestNotFound, "request path was not found")
}

func (s *Server) controllerPathHasAnotherMethod(r *http.Request) bool {
	for _, method := range controllerRouteMethods {
		if method == r.Method {
			continue
		}
		candidate := r.Clone(r.Context())
		candidate.Method = method
		if _, pattern := s.Mux.Handler(candidate); pattern != "" {
			return true
		}
	}
	return false
}

func canonicalConsolePath(r *http.Request) (string, bool) {
	if r == nil || r.URL == nil || r.URL.Path == "" || !strings.HasPrefix(r.URL.Path, "/") {
		return "", false
	}
	decoded := r.URL.Path
	escaped := r.URL.EscapedPath()
	if escaped == "" || escaped != (&url.URL{Path: decoded}).EscapedPath() {
		return "", false
	}
	lowerDecoded := strings.ToLower(decoded)
	if strings.ContainsAny(decoded, "\\\x00") || strings.Contains(lowerDecoded, "%2e") ||
		strings.Contains(lowerDecoded, "%2f") || strings.Contains(lowerDecoded, "%5c") ||
		strings.Contains(lowerDecoded, "%00") {
		return "", false
	}
	if decoded == "/" {
		return "", true
	}
	assetPath := strings.TrimPrefix(decoded, "/")
	if !fs.ValidPath(assetPath) {
		return "", false
	}
	for _, element := range strings.Split(assetPath, "/") {
		if element == "" || strings.HasPrefix(element, ".") || strings.HasPrefix(element, "_") {
			return "", false
		}
	}
	return assetPath, true
}

func (s *Server) serveConsole(w http.ResponseWriter, r *http.Request, assetPath string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		s.writeStaticProblem(w, errs.KindRequestMethodNotAllowed, "request method is not allowed")
		return
	}
	if s.console == nil {
		s.writeStaticProblem(w, errs.KindRequestUnavailable, "development binary has no embedded Console build")
		return
	}

	exactPath := assetPath
	if exactPath == "" {
		exactPath = "index.html"
	}
	served, missing, err := s.serveConsoleFile(w, r, exactPath)
	if err != nil {
		s.writeStaticProblem(w, errs.KindInternal, "embedded Console asset is unavailable")
		return
	}
	if served {
		return
	}
	if !missing {
		s.writeStaticProblem(w, errs.KindRequestNotFound, "Console asset was not found")
		return
	}
	if assetPath == "" || assetPath == "assets" || strings.HasPrefix(assetPath, "assets/") {
		s.writeStaticProblem(w, errs.KindRequestNotFound, "Console asset was not found")
		return
	}
	w.Header().Set("Vary", "Accept")
	if !acceptsConsoleHTML(r.Header.Values("Accept")) {
		s.writeStaticProblem(w, errs.KindRequestNotFound, "Console asset was not found")
		return
	}
	served, _, err = s.serveConsoleFile(w, r, "index.html")
	if err != nil {
		s.writeStaticProblem(w, errs.KindInternal, "embedded Console index is unavailable")
		return
	}
	if !served {
		s.writeStaticProblem(w, errs.KindRequestNotFound, "Console index was not found")
	}
}

func (s *Server) serveConsoleFile(w http.ResponseWriter, r *http.Request, name string) (bool, bool, error) {
	info, err := fs.Stat(s.console, name)
	if errors.Is(err, fs.ErrNotExist) {
		return false, true, nil
	}
	if err != nil {
		return false, false, err
	}
	if !info.Mode().IsRegular() {
		return false, false, nil
	}
	body, err := fs.ReadFile(s.console, name)
	if err != nil {
		return false, false, err
	}
	w.Header().Set("Content-Type", consoleContentType(name))
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Cache-Control", consoleCacheControl(name))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		if _, err := w.Write(body); err != nil && s.Logger != nil {
			s.Logger.Error("write Console response", "error", err)
		}
	}
	return true, false, nil
}

func consoleContentType(name string) string {
	if contentType, ok := consoleContentTypes[path.Ext(name)]; ok {
		return contentType
	}
	return "application/octet-stream"
}

func consoleCacheControl(name string) string {
	if hasConsoleFingerprint(name) {
		return "public, max-age=31536000, immutable"
	}
	return "no-cache"
}

func hasConsoleFingerprint(name string) bool {
	if !strings.HasPrefix(name, "assets/") {
		return false
	}
	base := path.Base(name)
	stem := strings.TrimSuffix(base, path.Ext(base))
	separator := strings.LastIndexByte(stem, '-')
	if separator <= 0 || len(stem)-separator-1 != 8 {
		return false
	}
	for _, character := range stem[separator+1:] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func acceptsConsoleHTML(values []string) bool {
	for _, value := range values {
		for _, member := range strings.Split(value, ",") {
			mediaType, parameters, err := mime.ParseMediaType(strings.TrimSpace(member))
			if err != nil || mediaType != "text/html" {
				continue
			}
			quality := "1"
			for name, parameter := range parameters {
				if name != "q" {
					quality = "0"
					break
				}
				quality = parameter
			}
			parsed, err := strconv.ParseFloat(quality, 64)
			if err == nil && parsed > 0 && parsed <= 1 {
				return true
			}
		}
	}
	return false
}

func (s *Server) writeStaticProblem(w http.ResponseWriter, kind errs.Kind, detail string) {
	w.Header().Set("Cache-Control", "no-store")
	s.writeProblem(w, errs.New(kind, detail))
}
