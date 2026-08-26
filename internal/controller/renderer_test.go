package controller

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/AlanD20/groundplane/internal/components/coredns"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: generated environment files are derived artifacts and must stay
// byte-stable when the same entries arrive in a different order.
func TestRenderEnvFileIsDeterministic(t *testing.T) {
	entries := []core.EnvEntry{
		{ID: "ev_b", Kind: core.EntryKindEnv, Key: "B", Exposure: []string{"all"}},
		{ID: "ev_a", Kind: core.EntryKindEnv, Key: "A", Exposure: []string{"all"}},
	}
	got, err := RenderEnvFile(entries, map[string]string{"ev_a": "one", "ev_b": "two"})
	if err != nil {
		t.Fatalf("RenderEnvFile() error = %v", err)
	}
	if want := "A=\"one\"\nB=\"two\"\n"; string(got) != want {
		t.Errorf("RenderEnvFile() = %q, want %q", got, want)
	}
}

// Rationale: Compose dotenv parsing has its own escape grammar, so rendered
// values must preserve every supported control character and delimiter.
func TestRenderEnvFileEscapesComposeDotEnvValues(t *testing.T) {
	entries := []core.EnvEntry{
		{ID: "ev_value", Kind: core.EntryKindEnv, Key: "VALUE", Exposure: []string{"all"}},
	}
	value := "\\\"$line\n\r\t\a\b\f\v# end "

	got, err := RenderEnvFile(entries, map[string]string{"ev_value": value})
	if err != nil {
		t.Fatalf("RenderEnvFile() error = %v", err)
	}
	want := []byte(`VALUE="\\\"$$line\n\r\t\a\b\f\v# end "` + "\n")
	if string(got) != string(want) {
		t.Fatalf("RenderEnvFile() = %q, want %q", got, want)
	}
}

// Rationale: dollar escaping prevents Compose interpolation from changing a
// resolved secret or environment value during materialization.
func TestRenderEnvFileEscapesEveryDollarForCompose(t *testing.T) {
	entries := []core.EnvEntry{
		{ID: "ev_token", Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"all"}},
	}

	got, err := RenderEnvFile(entries, map[string]string{"ev_token": "$HOME ${TOKEN} $$"})
	if err != nil {
		t.Fatalf("RenderEnvFile() error = %v", err)
	}
	want := `TOKEN="$$HOME $${TOKEN} $$$$"` + "\n"
	if string(got) != want {
		t.Fatalf("RenderEnvFile() = %q, want %q", got, want)
	}
}

// Rationale: NUL cannot be represented safely in a materialized dotenv file
// and must be rejected before bytes are emitted.
func TestRenderEnvFileRejectsNUL(t *testing.T) {
	entries := []core.EnvEntry{
		{ID: "ev_nul", Kind: core.EntryKindEnv, Key: "NUL", Exposure: []string{"all"}},
	}

	_, err := RenderEnvFile(entries, map[string]string{"ev_nul": "before\x00after"})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("RenderEnvFile() error = %v, want %q", err, errs.CodeValidationFailed)
	}
}

// Rationale: one service-scoped dotenv file cannot represent two values for
// one key without making the resolved environment ambiguous.
func TestRenderServiceEnvFileRejectsDuplicateScopedKeys(t *testing.T) {
	entries := []core.EnvEntry{
		{ID: "ev_a", Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"api"}},
		{ID: "ev_b", Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"worker", "api"}},
	}
	_, err := RenderServiceEnvFile("api", entries, map[string]string{"ev_a": "one", "ev_b": "two"})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("RenderServiceEnvFile() error = %v, want %q", err, errs.CodeValidationFailed)
	}
}

// Rationale: a missing resolved value is a Controller defect, not an
// operator validation error, because resolution precedes pure rendering.
func TestRenderEnvFileClassifiesMissingResolvedValueAsInternal(t *testing.T) {
	entries := []core.EnvEntry{
		{ID: "ev_a", Kind: core.EntryKindEnv, Key: "TOKEN", Exposure: []string{"all"}},
	}
	_, err := RenderEnvFile(entries, nil)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("RenderEnvFile() error = %v, want %q", err, errs.CodeInternal)
	}
}

// Rationale: the Controller must expose one canonical Corefile boundary while
// delegating all validation and deterministic output to the pure component renderer.
func TestRenderCorefileDelegatesToPureRenderer(t *testing.T) {
	input := coredns.CoreDNSRenderInput{
		Hosts: []coredns.CoreDNSHost{{
			Address: netip.MustParseAddr("10.200.30.4"), Hostnames: []string{"api.example.com"},
		}},
		CatchAll: []coredns.ResolverEndpoint{{Address: netip.MustParseAddr("1.1.1.1")}},
	}
	got, err := RenderCorefile(input)
	if err != nil {
		t.Fatalf("RenderCorefile() error = %v", err)
	}
	want := ".:53 {\n" +
		"    bind 127.0.0.1\n" +
		"    hosts {\n" +
		"        10.200.30.4 api.example.com\n" +
		"        no_reverse\n" +
		"        fallthrough\n" +
		"    }\n" +
		"    forward . 1.1.1.1\n" +
		"    reload\n" +
		"    prometheus 127.0.0.1:9153\n" +
		"    log\n" +
		"    errors\n" +
		"}\n"
	if string(got) != want {
		t.Fatalf("RenderCorefile() = %q, want %q", got, want)
	}
}
