package registryimages

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// IMG-02: multi-repository removal must address only captured content, protect
// stopped/child containers, resume after lost untag responses, and never count
// a successful untag as proof that the image was actually removed.
func TestImageRemovalHandlesAliasesWithoutForce(t *testing.T) {
	const firstRepo = "example.com/app"
	const secondRepo = "mirror.example.com/app"
	id := "sha256:" + strings.Repeat("a", 64)
	otherID := "sha256:" + strings.Repeat("b", 64)
	first := firstRepo + "@" + id
	second := secondRepo + "@" + id
	for _, scenario := range []string{"remove all", "already absent", "moved reference", "stopped container", "child container", "lost response", "final conflict"} {
		t.Run(scenario, func(t *testing.T) {
			present := scenario != "already absent"
			firstPresent := true
			interrupted := false
			var deleted []string
			observed := image.InspectResponse{
				ID:          id,
				RepoTags:    []string{firstRepo + ":one", firstRepo + ":two", secondRepo + ":latest"},
				RepoDigests: []string{first, second},
				Descriptor:  &ocispec.Descriptor{MediaType: ocispec.MediaTypeImageManifest, Digest: digest.Digest(id)},
			}
			if scenario == "child container" {
				observed.Descriptor.MediaType = ocispec.MediaTypeImageIndex
				observed.Manifests = []image.ManifestSummary{{ID: otherID}}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				path := strings.TrimPrefix(r.URL.Path, "/v1.55")
				write := func(value any) {
					if err := json.NewEncoder(w).Encode(value); err != nil {
						t.Error(err)
					}
				}
				if path == "/containers/json" {
					if r.URL.Query().Get("all") != "1" {
						t.Error("stopped containers excluded from removal checks")
					}
					containers := []container.Summary{}
					if scenario == "stopped container" {
						containers = append(containers, container.Summary{ImageID: id, State: "exited"})
					}
					if scenario == "child container" {
						containers = append(containers, container.Summary{ImageID: otherID, State: "exited"})
					}
					write(containers)
					return
				}
				selector := strings.TrimPrefix(path, "/images/")
				if r.Method == http.MethodGet && strings.HasSuffix(selector, "/json") {
					selector = strings.TrimSuffix(selector, "/json")
					if !present || selector == first && !firstPresent {
						w.WriteHeader(http.StatusNotFound)
						write(struct{ Message string }{"missing"})
						return
					}
					value := observed
					if scenario == "moved reference" && selector == first {
						value.ID = otherID
					}
					write(value)
					return
				}
				if r.Method != http.MethodDelete || !slices.Contains([]string{first, second, id}, selector) {
					t.Errorf("unexpected Docker operation: %s %s", r.Method, path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if r.URL.Query().Get("force") == "1" || r.URL.Query().Get("noprune") != "1" {
					t.Error("removal forced deletion or permitted unrelated pruning")
				}
				deleted = append(deleted, selector)
				if selector == first {
					firstPresent = false
					if scenario == "lost response" && !interrupted {
						interrupted = true
						w.WriteHeader(http.StatusInternalServerError)
						write(struct{ Message string }{"response unavailable after untag"})
						return
					}
				}
				if selector == id {
					if scenario == "final conflict" {
						w.WriteHeader(http.StatusConflict)
						write(struct{ Message string }{"image still needed"})
						return
					}
					present = false
				}
				write([]image.DeleteResponse{})
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
			err = removeLocalImage(t.Context(), engine, id)
			if scenario == "lost response" {
				if err == nil || !present || !slices.Equal(deleted, []string{first}) {
					t.Fatalf("interrupted removal = %v, present=%v, deleted=%v", err, present, deleted)
				}
				err = removeLocalImage(t.Context(), engine, id)
			}
			switch scenario {
			case "remove all", "lost response":
				if err != nil || present || !slices.Equal(deleted, []string{first, second, id}) {
					t.Fatalf("remove = %v, present=%v, deleted=%v", err, present, deleted)
				}
			case "already absent":
				if err != nil || len(deleted) != 0 {
					t.Fatalf("absent removal = %v, deleted=%v", err, deleted)
				}
			case "moved reference":
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || len(deleted) != 0 {
					t.Fatalf("moved content removal = %v, deleted=%v", err, deleted)
				}
			default:
				if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) || !present {
					t.Fatalf("protected image removal = %v, present=%v", err, present)
				}
				if scenario != "final conflict" && len(deleted) != 0 {
					t.Fatalf("untagged image used by a container: %v", deleted)
				}
			}
		})
	}
}
