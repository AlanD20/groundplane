package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBuildBlueprintMultipartProducesCanonicalManifestAndParts(t *testing.T) {
	// Rationale: the CLI must reproduce the API's path-sorted file namespace
	// while preserving the operator's separate Compose layer order.
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "compose"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "blueprint.yaml"), []byte("root"), 0o600); err != nil {
		t.Fatalf("write root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(directory, "compose", "base.yaml"), []byte("base"), 0o600); err != nil {
		t.Fatalf("write layer: %v", err)
	}

	body, contentType, err := buildBlueprintMultipart(
		directory, "blueprint.yaml", []string{"compose/base.yaml"}, []string{"TAG=v1"},
	)
	if err != nil {
		t.Fatalf("buildBlueprintMultipart() error = %v", err)
	}
	request := httptest.NewRequest(http.MethodPut, "/blueprint", bytes.NewReader(body))
	request.Header.Set("Content-Type", contentType)
	reader, err := request.MultipartReader()
	if err != nil {
		t.Fatalf("MultipartReader(): %v", err)
	}
	manifestPart, err := reader.NextPart()
	if err != nil {
		t.Fatalf("NextPart(manifest): %v", err)
	}
	var manifest blueprintCLIManifest
	if err := json.NewDecoder(manifestPart).Decode(&manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.Root != "blueprint.yaml" || len(manifest.ComposeSources) != 2 ||
		manifest.ComposeSources[1] != "compose/base.yaml" || manifest.Interpolation["TAG"] != "v1" ||
		len(manifest.Files) != 2 || manifest.Files[0].Path != "blueprint.yaml" ||
		manifest.Files[1].Path != "compose/base.yaml" {
		t.Fatalf("manifest = %#v", manifest)
	}
	for index, want := range [][]byte{[]byte("root"), []byte("base")} {
		part, err := reader.NextPart()
		if err != nil {
			t.Fatalf("NextPart(%d): %v", index, err)
		}
		got, err := io.ReadAll(part)
		if err != nil || !bytes.Equal(got, want) || part.FormName() != blueprintCLIFilePartName(index) ||
			part.FileName() != "" {
			t.Fatalf("part %d = %q, %q, %q, %v", index, part.FormName(), part.FileName(), got, err)
		}
	}
	if part, err := reader.NextPart(); err != io.EOF || part != nil {
		t.Fatalf("trailing part = %v, %v", part, err)
	}
}

func TestBuildBlueprintMultipartRejectsSymlinks(t *testing.T) {
	// Rationale: local convenience must not make ambient files or symlink
	// targets part of a supposedly closed, reproducible Blueprint bundle.
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "blueprint.yaml"), []byte("root"), 0o600); err != nil {
		t.Fatalf("write root: %v", err)
	}
	if err := os.Symlink("blueprint.yaml", filepath.Join(directory, "linked.yaml")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	_, _, err := buildBlueprintMultipart(directory, "blueprint.yaml", nil, nil)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("buildBlueprintMultipart() error = %v, want validation failure", err)
	}
}

func TestEnvironmentApplySendsMultipartSingletonReplacement(t *testing.T) {
	// Rationale: the CLI command is the required 1:1 mirror of the Blueprint
	// API endpoint, including the revision read, fenced PUT, idempotency,
	// multipart media, and Task result.
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "blueprint.yaml"), []byte("services: {}\n"), 0o600); err != nil {
		t.Fatalf("write root: %v", err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path != "/api/v1/environments/"+environmentID+"/blueprint" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		if requests == 1 {
			if request.Method != http.MethodGet || len(request.Header.Values("Idempotency-Key")) != 0 {
				t.Errorf(
					"revision read = %s with idempotency values %q",
					request.Method,
					request.Header.Values("Idempotency-Key"),
				)
			}
			writer.Header().Set("ETag", `"7"`)
			_, _ = io.WriteString(
				writer,
				`{"environment_id":"`+environmentID+`","revision":"7","document":"services: {}\n"}`,
			)
			return
		}
		if requests != 2 || request.Method != http.MethodPut {
			t.Errorf("request %d = %s, want second request PUT", requests, request.Method)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if got := request.Header.Values("If-Match"); len(got) != 1 || got[0] != `"7"` {
			t.Errorf("If-Match = %q, want the read revision", got)
		}
		if len(request.Header.Values("Idempotency-Key")) != 1 {
			t.Errorf("Idempotency-Key values = %q", request.Header.Values("Idempotency-Key"))
		}
		mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if err != nil || mediaType != "multipart/form-data" {
			t.Errorf("Content-Type = %q, %v", request.Header.Get("Content-Type"), err)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(writer, `{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`)
	}))
	defer server.Close()

	executeNoun(
		t, newEnvironmentCmd(), server.URL, Scope{AsID: true},
		"blueprint", "apply", environmentID,
		"--bundle-dir", directory, "--root", "blueprint.yaml", "--var", "TAG=v1",
	)
	if requests != 2 {
		t.Fatalf("requests = %d, want revision read then apply", requests)
	}
}
