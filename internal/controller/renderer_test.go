package controller

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestRenderCaddyfileUsesDocumentedPlaceholders(t *testing.T) {
	got, err := RenderCaddyfile("reverse_proxy {host}__{slot}:8080", "green", "api")
	if err != nil {
		t.Fatalf("RenderCaddyfile() error = %v", err)
	}
	if want := "reverse_proxy api__green:8080"; string(got) != want {
		t.Errorf("RenderCaddyfile() = %q, want %q", got, want)
	}
}

func TestRenderEnvFileIsDeterministic(t *testing.T) {
	entries := []core.EnvEntry{
		{ID: "ev_b", Kind: core.EntryKindEnv, Key: "B", Exposure: []string{"all"}},
		{ID: "ev_a", Kind: core.EntryKindEnv, Key: "A", Exposure: []string{"all"}},
	}
	got, err := RenderEnvFile(entries, map[string]string{"ev_a": "one", "ev_b": "two"})
	if err != nil {
		t.Fatalf("RenderEnvFile() error = %v", err)
	}
	if want := "A=one\nB=two\n"; string(got) != want {
		t.Errorf("RenderEnvFile() = %q, want %q", got, want)
	}
}

func TestRenderServiceEnvFileRejectsDuplicateScopedKeys(t *testing.T) {
	entries := []core.EnvEntry{
		{ID: "ev_a", Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"api"}},
		{ID: "ev_b", Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"worker", "api"}},
	}
	_, err := RenderServiceEnvFile("api", entries, map[string]string{"ev_a": "one", "ev_b": "two"})
	if !errors.Is(err, errs.New(errs.CodeValidationFailed, "")) {
		t.Fatalf("RenderServiceEnvFile() error = %v, want %q", err, errs.CodeValidationFailed)
	}
}

func TestRenderEnvFileClassifiesMissingResolvedValueAsInternal(t *testing.T) {
	entries := []core.EnvEntry{
		{ID: "ev_a", Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"all"}},
	}
	_, err := RenderEnvFile(entries, nil)
	if !errors.Is(err, errs.New(errs.CodeInternal, "")) {
		t.Fatalf("RenderEnvFile() error = %v, want %q", err, errs.CodeInternal)
	}
}

func TestRenderCorefileFailsClosedUntilComplete(t *testing.T) {
	_, err := RenderCorefile(nil, nil, false)
	if !errors.Is(err, errs.New(errs.CodeNotImplemented, "")) {
		t.Fatalf("RenderCorefile() error = %v, want %q", err, errs.CodeNotImplemented)
	}
}
