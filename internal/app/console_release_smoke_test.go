//go:build groundplane_console

package app

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller"
)

const (
	consoleIndexDigestEnvironment = "GROUNDPLANE_CONSOLE_INDEX_SHA256"
	consoleAssetPathEnvironment   = "GROUNDPLANE_CONSOLE_ASSET_PATH"
	consoleAssetDigestEnvironment = "GROUNDPLANE_CONSOLE_ASSET_SHA256"
)

// Rationale: the release gate must prove that production application wiring
// serves the exact Vite bytes after their ignored source tree is unavailable,
// while the API namespace remains owned by the Controller dispatcher.
func TestProductionConsoleReleaseSmoke(t *testing.T) {
	indexDigest, assetPath, assetDigest := consoleReleaseSmokeInputs(t)
	assets, err := controllerConsoleAssets()
	if err != nil {
		t.Fatalf("controllerConsoleAssets() error = %v", err)
	}
	server := controller.New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), controller.Options{Console: assets})
	handler := server.HTTPHandler()

	index := httptest.NewRecorder()
	handler.ServeHTTP(index, httptest.NewRequest(http.MethodGet, "/", nil))
	assertConsoleReleaseAsset(t, index, indexDigest, "text/html; charset=utf-8")

	asset := httptest.NewRecorder()
	handler.ServeHTTP(asset, httptest.NewRequest(http.MethodGet, assetPath, nil))
	assertConsoleReleaseAsset(t, asset, assetDigest, "")

	request := httptest.NewRequest(http.MethodGet, "/api", nil)
	request.Header.Set("Accept", "text/html")
	api := httptest.NewRecorder()
	handler.ServeHTTP(api, request)
	if api.Code != http.StatusNotFound || api.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("API response = %d %#v; want RFC 7807 404", api.Code, api.Header())
	}
	if strings.Contains(strings.ToLower(api.Body.String()), "<html") {
		t.Fatal("API namespace received Console HTML")
	}
}

func consoleReleaseSmokeInputs(t *testing.T) (string, string, string) {
	t.Helper()
	indexDigest := os.Getenv(consoleIndexDigestEnvironment)
	assetPath := os.Getenv(consoleAssetPathEnvironment)
	assetDigest := os.Getenv(consoleAssetDigestEnvironment)
	if indexDigest == "" && assetPath == "" && assetDigest == "" {
		t.Skip("release smoke is run by make console-release-smoke")
	}
	if indexDigest == "" || !strings.HasPrefix(assetPath, "/assets/") || assetDigest == "" {
		t.Fatal("release smoke inputs are incomplete")
	}
	return indexDigest, assetPath, assetDigest
}

func assertConsoleReleaseAsset(
	t *testing.T,
	response *httptest.ResponseRecorder,
	wantDigest string,
	wantContentType string,
) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("Console response status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if wantContentType != "" && response.Header().Get("Content-Type") != wantContentType {
		t.Fatalf("Content-Type = %q, want %q", response.Header().Get("Content-Type"), wantContentType)
	}
	if response.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", response.Header().Get("X-Content-Type-Options"))
	}
	digest := sha256.Sum256(response.Body.Bytes())
	if got := hex.EncodeToString(digest[:]); got != wantDigest {
		t.Fatalf("Console response digest = %q, want %q", got, wantDigest)
	}
}
