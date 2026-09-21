package handlers

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintTestPart struct {
	name        string
	contentType string
	filename    string
	content     []byte
}

func TestDecodeBlueprintMultipartAcceptsClosedVerifiedBundle(t *testing.T) {
	// Rationale: the API boundary must preserve ordered Compose sources and
	// verified file bytes exactly before parsing or durable intent hashing.
	content := []byte("services: {}\n")
	manifest := blueprintTestManifest(content)
	request := blueprintMultipartTestRequest(t, manifest, []blueprintTestPart{{
		name: "file-000001", contentType: "application/octet-stream", content: content,
	}})

	bundle, err := decodeBlueprintMultipart(request)
	if err != nil {
		t.Fatalf("decodeBlueprintMultipart() error = %v", err)
	}
	if bundle.RootPath != "blueprint.yaml" || len(bundle.ComposeSources) != 1 ||
		bundle.ComposeSources[0] != "blueprint.yaml" || bundle.Interpolation["TAG"] != "v1" ||
		len(bundle.Files) != 1 || bundle.Files[0].Path != "blueprint.yaml" ||
		!bytes.Equal(bundle.Files[0].Content, content) {
		t.Fatalf("decodeBlueprintMultipart() = %#v", bundle)
	}
}

func TestDecodeBlueprintMultipartRejectsAmbiguousOrUnverifiedParts(t *testing.T) {
	// Rationale: a closed bundle is reproducible only when manifest members,
	// part identities, sizes, and digests have one canonical interpretation.
	content := []byte("services: {}\n")
	validManifest := blueprintTestManifest(content)
	tests := []struct {
		name     string
		manifest []byte
		parts    []blueprintTestPart
		kind     errs.Kind
	}{
		{
			name: "duplicate manifest member",
			manifest: bytes.Replace(validManifest, []byte(`"root":"blueprint.yaml"`),
				[]byte(`"root":"blueprint.yaml","root":"other.yaml"`), 1),
			parts: []blueprintTestPart{
				{name: "file-000001", contentType: "application/octet-stream", content: content},
			},
			kind: errs.KindMalformedRequest,
		},
		{
			name: "unknown manifest member",
			manifest: bytes.Replace(validManifest, []byte(`{"root"`),
				[]byte(`{"unknown":true,"root"`), 1),
			parts: []blueprintTestPart{
				{name: "file-000001", contentType: "application/octet-stream", content: content},
			},
			kind: errs.KindMalformedRequest,
		},
		{
			name:     "wrong part identity",
			manifest: validManifest,
			parts: []blueprintTestPart{
				{name: "file-000002", contentType: "application/octet-stream", content: content},
			},
			kind: errs.KindMalformedRequest,
		},
		{
			name:     "filename smuggling",
			manifest: validManifest,
			parts: []blueprintTestPart{
				{
					name:        "file-000001",
					filename:    "blueprint.yaml",
					contentType: "application/octet-stream",
					content:     content,
				},
			},
			kind: errs.KindMalformedRequest,
		},
		{
			name: "digest mismatch",
			manifest: bytes.Replace(validManifest, []byte(blueprintTestDigest(content)),
				[]byte(fmt.Sprintf("%064x", 1)), 1),
			parts: []blueprintTestPart{
				{name: "file-000001", contentType: "application/octet-stream", content: content},
			},
			kind: errs.KindMalformedRequest,
		},
		{
			name: "declared size mismatch",
			manifest: bytes.Replace(validManifest,
				[]byte(fmt.Sprintf(`"size":%d`, len(content))), []byte(`"size":1`), 1),
			parts: []blueprintTestPart{
				{name: "file-000001", contentType: "application/octet-stream", content: content},
			},
			kind: errs.KindMalformedRequest,
		},
		{
			name:     "undeclared trailing part",
			manifest: validManifest,
			parts: []blueprintTestPart{
				{name: "file-000001", contentType: "application/octet-stream", content: content},
				{name: "file-000002", contentType: "application/octet-stream", content: []byte("extra")},
			},
			kind: errs.KindMalformedRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := blueprintMultipartTestRequest(t, test.manifest, test.parts)
			_, err := decodeBlueprintMultipart(request)
			if !errors.Is(err, errs.New(test.kind, "")) {
				t.Fatalf("decodeBlueprintMultipart() error = %v, want kind %v", err, test.kind)
			}
		})
	}
}

func TestDecodeBlueprintMultipartRejectsInvalidClosedNamespace(t *testing.T) {
	// Rationale: verified bytes are still invalid when their declared logical
	// path can escape the bundle or the root is absent from Compose sources.
	content := []byte("services: {}\n")
	manifest := bytes.Replace(blueprintTestManifest(content),
		[]byte(`"path":"blueprint.yaml"`), []byte(`"path":"../blueprint.yaml"`), 1)
	request := blueprintMultipartTestRequest(t, manifest, []blueprintTestPart{{
		name: "file-000001", contentType: "application/octet-stream", content: content,
	}})
	_, err := decodeBlueprintMultipart(request)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("decodeBlueprintMultipart() error = %v, want validation failure", err)
	}
}

func blueprintTestManifest(content []byte) []byte {
	return []byte(fmt.Sprintf(
		`{"root":"blueprint.yaml","compose_sources":["blueprint.yaml"],"interpolation":{"TAG":"v1"},"files":[{"path":"blueprint.yaml","part":"file-000001","size":%d,"sha256":"%s"}]}`,
		len(content),
		blueprintTestDigest(content),
	))
}

func blueprintTestDigest(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}

func blueprintMultipartTestRequest(
	t *testing.T,
	manifest []byte,
	parts []blueprintTestPart,
) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	manifestHeader := make(textproto.MIMEHeader)
	manifestHeader.Set("Content-Disposition", `form-data; name="manifest"`)
	manifestHeader.Set("Content-Type", "application/json")
	manifestPart, err := writer.CreatePart(manifestHeader)
	if err != nil {
		t.Fatalf("CreatePart(manifest): %v", err)
	}
	if _, err := manifestPart.Write(manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	for _, part := range parts {
		header := make(textproto.MIMEHeader)
		disposition := fmt.Sprintf(`form-data; name="%s"`, part.name)
		if part.filename != "" {
			disposition += fmt.Sprintf(`; filename="%s"`, part.filename)
		}
		header.Set("Content-Disposition", disposition)
		header.Set("Content-Type", part.contentType)
		filePart, err := writer.CreatePart(header)
		if err != nil {
			t.Fatalf("CreatePart(%s): %v", part.name, err)
		}
		if _, err := filePart.Write(part.content); err != nil {
			t.Fatalf("write %s: %v", part.name, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	request, err := http.NewRequest(
		http.MethodPut,
		"/api/v1/environments/env_x/blueprint",
		bytes.NewReader(body.Bytes()),
	)
	if err != nil {
		t.Fatalf("NewRequest(): %v", err)
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	return request
}
