package registryimages

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
)

// IMG-01: consuming Docker progress must preserve daemon error detection;
// bytes reported by multiple layers cannot become a synthetic completion.
func TestFetchStreamReportsProgressAndRejectsDaemonFailure(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "download", true: "daemon failure"}[failed], func(t *testing.T) {
			plan := imagefetch.Plan{Requested: "nginx:latest", Repository: "docker.io/library/nginx", Architecture: "amd64", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConfigDigest: "sha256:" + strings.Repeat("b", 64)}
			pulled := false
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/images/create") {
					if r.URL.Query().Get("tag") != plan.ManifestDigest {
						t.Error("pull selected mutable content")
					}
					pulled = true
					body := `{"status":"Downloading","id":"layer1","progressDetail":{"current":40,"total":50}}` + "\n" +
						`{"status":"Downloading","id":"layer2","progressDetail":{"current":25,"total":50}}` + "\n"
					if failed {
						body += `{"errorDetail":{"message":"private daemon diagnostic","code":500}}` + "\n"
					}
					if _, err := w.Write([]byte(body)); err != nil {
						t.Error(err)
					}
					return
				}
				if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/json") {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					w.WriteHeader(400)
					return
				}
				if !pulled {
					w.WriteHeader(404)
					_, _ = w.Write([]byte(`{"message":"missing"}`))
					return
				}
				if err := json.NewEncoder(w).Encode(image.InspectResponse{ID: plan.ConfigDigest, Os: "linux", Architecture: "amd64", RepoDigests: []string{"nginx@" + plan.ManifestDigest}}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			engine, err := client.New(client.WithHost(server.URL), client.WithAPIVersion("1.55"), client.WithHTTPClient(server.Client()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := engine.Close(); err != nil {
					t.Error(err)
				}
			}()
			registry := &Client{engine: engine, architecture: "amd64"}
			var reported []imagefetch.Progress
			id, err := registry.Fetch(context.Background(), plan, func(value imagefetch.Progress) error { reported = append(reported, value); return nil })
			if failed {
				if err == nil || id != "" {
					t.Fatal("daemon error accepted as success")
				}
				if len(reported) != 1 || reported[0].Phase != "downloading" {
					t.Fatalf("failed pull advanced to verification: %v", reported)
				}
				return
			}
			if err != nil || id != plan.ConfigDigest {
				t.Fatalf("fetch = %q, %v", id, err)
			}
			last := reported[len(reported)-1]
			if last.Phase != "verifying" || last.DownloadedBytes != 65 || last.TotalBytes != 100 {
				t.Fatalf("reported progress = %#v", last)
			}
		})
	}
}
