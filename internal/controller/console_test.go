package controller

import (
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Controller and operational namespaces must win before SPA fallback,
// including unknown paths and method errors produced as RFC 7807 responses.
func TestConsoleDispatcherPreservesControllerRoutePrecedence(t *testing.T) {
	t.Parallel()
	server := consoleTestServer(consoleTestFS())
	server.Mux.HandleFunc("GET /api/v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{\"ok\":true}"))
	})
	server.Mux.HandleFunc("GET /openapi.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{\"openapi\":\"3.1.0\"}"))
	})

	tests := []struct {
		name   string
		method string
		path   string
		status int
		code   errs.Code
	}{
		{name: "known API", method: http.MethodGet, path: "/api/v1/ping", status: http.StatusOK},
		{
			name:   "exact API namespace",
			method: http.MethodGet,
			path:   "/api",
			status: http.StatusNotFound,
			code:   errs.CodeRequestNotFound,
		},
		{
			name:   "unknown API",
			method: http.MethodGet,
			path:   "/api/v1/missing",
			status: http.StatusNotFound,
			code:   errs.CodeRequestNotFound,
		},
		{
			name:   "API wrong method",
			method: http.MethodPost,
			path:   "/api/v1/ping",
			status: http.StatusMethodNotAllowed,
			code:   errs.CodeRequestMethodNotAllowed,
		},
		{
			name:   "operational wrong method",
			method: http.MethodPost,
			path:   "/openapi.json",
			status: http.StatusMethodNotAllowed,
			code:   errs.CodeRequestMethodNotAllowed,
		},
		{
			name:   "static wrong method",
			method: http.MethodPost,
			path:   "/",
			status: http.StatusMethodNotAllowed,
			code:   errs.CodeRequestMethodNotAllowed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("Accept", "text/html")
			response := httptest.NewRecorder()
			server.requestHandler().ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, test.status, response.Body.String())
			}
			if test.code != "" {
				assertConsoleProblem(t, response, test.code)
				if response.Header().Get("Cache-Control") != "no-store" {
					t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
				}
			}
			if strings.Contains(response.Body.String(), "console-index") && strings.HasPrefix(test.path, "/api") {
				t.Fatal("API response received SPA HTML")
			}
		})
	}
}

// Rationale: ServeMux cleaning or redirects must never turn hostile or
// non-canonical request paths into a valid API route or embedded asset.
func TestConsoleDispatcherRejectsNonCanonicalPathsBeforeRouting(t *testing.T) {
	t.Parallel()
	server := consoleTestServer(consoleTestFS())
	tests := []string{
		"/a//b", "/a/./b", "/a/../b", "/%2e/secret", "/a%2fb", "/a%5cb",
		"/.hidden", "/_hidden", "/a\\b", "/a/", "/a\x00b",
	}
	for _, target := range tests {
		t.Run(strconv.Quote(target), func(t *testing.T) {
			var request *http.Request
			if strings.ContainsRune(target, '\x00') {
				request = &http.Request{Method: http.MethodGet, Header: make(http.Header), URL: &url.URL{Path: target}}
			} else {
				request = httptest.NewRequest(http.MethodGet, target, nil)
			}
			request.Header.Set("Accept", "text/html")
			response := httptest.NewRecorder()
			server.requestHandler().ServeHTTP(response, request)
			if response.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404; body=%s", response.Code, response.Body.String())
			}
			assertConsoleProblem(t, response, errs.CodeRequestNotFound)
		})
	}
}

// Rationale: exact files and HEAD must use only embedded byte metadata, the
// closed MIME map, and the cache policy justified by Vite's locked hash form.
func TestConsoleServesExactFilesWithDeterministicHeaders(t *testing.T) {
	t.Parallel()
	server := consoleTestServer(consoleTestFS())
	tests := []struct {
		name        string
		method      string
		path        string
		contentType string
		cache       string
		body        string
	}{
		{
			name:        "index",
			method:      http.MethodGet,
			path:        "/",
			contentType: "text/html; charset=utf-8",
			cache:       "no-cache",
			body:        "console-index",
		},
		{
			name:        "head",
			method:      http.MethodHead,
			path:        "/app.css",
			contentType: "text/css; charset=utf-8",
			cache:       "no-cache",
		},
		{
			name:        "fingerprinted asset",
			method:      http.MethodGet,
			path:        "/assets/app-1a2b3c4d.js",
			contentType: "text/javascript; charset=utf-8",
			cache:       "public, max-age=31536000, immutable",
			body:        "console-js",
		},
		{
			name:        "unfingerprinted asset",
			method:      http.MethodGet,
			path:        "/assets/plain.js",
			contentType: "text/javascript; charset=utf-8",
			cache:       "no-cache",
			body:        "plain-js",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			server.requestHandler().ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
			if response.Code != http.StatusOK || response.Header().Get("Content-Type") != test.contentType ||
				response.Header().Get("Cache-Control") != test.cache ||
				response.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatalf("response = %d %#v", response.Code, response.Header())
			}
			if response.Body.String() != test.body {
				t.Fatalf("body = %q, want %q", response.Body.String(), test.body)
			}
			wantLength := len(test.body)
			if test.method == http.MethodHead {
				wantLength = len("console-css")
			}
			if response.Header().Get("Content-Length") != strconv.Itoa(wantLength) {
				t.Fatalf("Content-Length = %q, want %d", response.Header().Get("Content-Length"), wantLength)
			}
		})
	}
}

// Rationale: SPA fallback is explicit HTML negotiation, never an absent or
// wildcard Accept shortcut, and the asset namespace can never receive HTML.
func TestConsoleFallbackRequiresPositiveExactHTMLAccept(t *testing.T) {
	t.Parallel()
	server := consoleTestServer(consoleTestFS())
	tests := []struct {
		name   string
		path   string
		accept string
		status int
		vary   string
	}{
		{name: "absent", path: "/workspace", status: http.StatusNotFound, vary: "Accept"},
		{name: "wildcard", path: "/workspace", accept: "*/*", status: http.StatusNotFound, vary: "Accept"},
		{
			name:   "malformed",
			path:   "/workspace",
			accept: "text/html;q=broken",
			status: http.StatusNotFound,
			vary:   "Accept",
		},
		{
			name:   "zero quality",
			path:   "/workspace",
			accept: "text/html;q=0",
			status: http.StatusNotFound,
			vary:   "Accept",
		},
		{
			name:   "positive HTML",
			path:   "/workspace",
			accept: "application/json, text/html;q=0.5",
			status: http.StatusOK,
			vary:   "Accept",
		},
		{name: "missing asset", path: "/assets/missing.js", accept: "text/html", status: http.StatusNotFound},
		{name: "directory", path: "/dir", accept: "text/html", status: http.StatusNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			if test.accept != "" {
				request.Header.Set("Accept", test.accept)
			}
			response := httptest.NewRecorder()
			server.requestHandler().ServeHTTP(response, request)
			if response.Code != test.status || response.Header().Get("Vary") != test.vary {
				t.Fatalf(
					"response = %d Vary %q; want %d %q",
					response.Code,
					response.Header().Get("Vary"),
					test.status,
					test.vary,
				)
			}
			if test.status == http.StatusOK && response.Body.String() != "console-index" {
				t.Fatalf("fallback body = %q", response.Body.String())
			}
		})
	}
}

// Rationale: a clean-checkout untagged Controller remains useful for API
// development while every otherwise valid Console request fails closed.
func TestConsoleUnavailableWithoutEmbeddedAssets(t *testing.T) {
	t.Parallel()
	server := consoleTestServer(nil)
	response := httptest.NewRecorder()
	server.requestHandler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response = %d %#v", response.Code, response.Header())
	}
	assertConsoleProblem(t, response, errs.CodeRequestFailed)
}

// Rationale: content types never depend on the host MIME registry or content sniffing.
func TestConsoleContentTypeMapIsClosedAndCaseSensitive(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"x.avif": "image/avif", "x.css": "text/css; charset=utf-8", "x.gif": "image/gif",
		"x.html": "text/html; charset=utf-8", "x.ico": "image/x-icon", "x.jpeg": "image/jpeg",
		"x.jpg": "image/jpeg", "x.js": "text/javascript; charset=utf-8", "x.mjs": "text/javascript; charset=utf-8",
		"x.json": "application/json", "x.map": "application/json", "x.png": "image/png",
		"x.svg": "image/svg+xml", "x.txt": "text/plain; charset=utf-8", "x.webp": "image/webp",
		"x.woff": "font/woff", "x.woff2": "font/woff2", "x.CSS": "application/octet-stream",
		"x.unknown": "application/octet-stream",
	}
	for name, want := range tests {
		if got := consoleContentType(name); got != want {
			t.Errorf("consoleContentType(%q) = %q, want %q", name, got, want)
		}
	}
}

func consoleTestFS() fs.FS {
	return fstest.MapFS{
		"index.html":             &fstest.MapFile{Data: []byte("console-index")},
		"app.css":                &fstest.MapFile{Data: []byte("console-css")},
		"assets/app-1a2b3c4d.js": &fstest.MapFile{Data: []byte("console-js")},
		"assets/plain.js":        &fstest.MapFile{Data: []byte("plain-js")},
		"dir/file.txt":           &fstest.MapFile{Data: []byte("nested")},
	}
}

func consoleTestServer(assets fs.FS) *Server {
	return &Server{
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Mux:     http.NewServeMux(),
		console: assets,
	}
}

func assertConsoleProblem(t *testing.T, response *httptest.ResponseRecorder, code errs.Code) {
	t.Helper()
	var problem errs.Problem
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if problem.Code != code || response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("problem = %#v headers=%#v", problem, response.Header())
	}
}
