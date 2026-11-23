package softwarestaging

import (
	"archive/tar"
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

type archiveTransport func(*http.Request) (*http.Response, error)

func (transport archiveTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestControllerPayloadCopyChecksPreparedOpenAPI(t *testing.T) {
	api := []byte(`{"openapi":"3.1.0"}`)
	files := map[string][]byte{
		"controller-release.json": []byte("release metadata"),
		"controller":              []byte("candidate Controller"),
		"groundplane":             []byte("candidate CLI"),
		"openapi.json":            api,
	}
	selected := preparation.ControllerResult{
		MetadataSHA256: string(upgrade.Hash(files["controller-release.json"])),
		BinarySHA256:   string(upgrade.Hash(files["controller"])),
		CLISHA256:      string(upgrade.Hash(files["groundplane"])),
		APISHA256:      string(upgrade.Hash(api)),
	}
	for _, test := range []struct {
		name string
		api  []byte
	}{
		{"matching document", api},
		{"changed document", []byte(`{"openapi":"3.1.0","paths":{}}`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := archiveTransport(func(request *http.Request) (*http.Response, error) {
				name := filepath.Base(request.URL.Query().Get("path"))
				payload, found := files[name]
				if request.URL.Path != "/v1.55/containers/fixture/archive" || !found {
					t.Fatalf("unexpected archive copy: %s", request.URL)
				}
				if name == "openapi.json" {
					payload = test.api
				}
				var body bytes.Buffer
				writer := tar.NewWriter(&body)
				if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(payload))}); err != nil {
					t.Fatal(err)
				}
				if _, err := writer.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				return &http.Response{StatusCode: http.StatusOK, Request: request,
					Header: http.Header{"X-Docker-Container-Path-Stat": {"e30="}}, Body: io.NopCloser(&body)}, nil
			})
			engine, err := client.New(client.WithAPIVersion("1.55"),
				client.WithHTTPClient(&http.Client{Transport: transport}))
			if err != nil {
				t.Fatal(err)
			}
			defer engine.Close()
			workspace := t.TempDir()
			err = copyControllerPayload(t.Context(), engine, "fixture", workspace, selected)
			if !bytes.Equal(test.api, api) {
				if kind, _ := errs.KindOf(err); kind != errs.KindValidationFailed {
					t.Fatalf("expected changed OpenAPI rejection, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			actual, err := os.ReadFile(filepath.Join(workspace, "openapi.json"))
			if err != nil || !bytes.Equal(actual, api) {
				t.Fatalf("copied OpenAPI = %q, %v", actual, err)
			}
		})
	}
}
