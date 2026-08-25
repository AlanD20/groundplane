package controller

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: direct credentials are security-sensitive structured input, so
// unknown, duplicate, malformed, and trailing JSON must fail closed.
func TestDecodeDirectCredentialJSONIsStrict(t *testing.T) {
	// Rationale: direct credentials are durable ciphertext plaintext and must
	// not accept unknown, duplicate, missing, non-string, or trailing fields.
	expected := map[core.ConnectorCredentialName]core.ConnectorCredentialKind{
		core.ConnectorCredentialAccessKey: core.ConnectorCredentialDirect,
		core.ConnectorCredentialSecretKey: core.ConnectorCredentialDirect,
	}
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{name: "valid", value: `{"access_key":"access","secret_key":"secret"}`, valid: true},
		{name: "unknown", value: `{"access_key":"access","secret_key":"secret","extra":"x"}`},
		{name: "duplicate", value: `{"access_key":"one","access_key":"two","secret_key":"secret"}`},
		{name: "missing", value: `{"access_key":"access"}`},
		{name: "wrong type", value: `{"access_key":7,"secret_key":"secret"}`},
		{name: "trailing", value: `{"access_key":"access","secret_key":"secret"} false`},
		{name: "trailing comma", value: `{"access_key":"access","secret_key":"secret",}`},
		{name: "empty", value: `{"access_key":"","secret_key":"secret"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoded, err := decodeDirectCredentialJSON([]byte(test.value), expected)
			if test.valid {
				if err != nil {
					t.Fatalf("decodeDirectCredentialJSON() error = %v", err)
				}
				if string(decoded[core.ConnectorCredentialAccessKey]) != "access" ||
					string(decoded[core.ConnectorCredentialSecretKey]) != "secret" {
					t.Fatal("decoded direct credentials differ")
				}
				clearBackupSecretMap(decoded)
				return
			}
			if err == nil {
				t.Fatal("decodeDirectCredentialJSON() error = nil")
			}
		})
	}
}

// Rationale: every resolved slot is transient plaintext and must be released
// together when delivery or later resolution fails.
func TestClearBackupSecretMapClearsOwnedBytes(t *testing.T) {
	// Rationale: parser and resolver failures must not retain partial plaintext.
	first := bytes.Repeat([]byte{0xA1}, 16)
	second := bytes.Repeat([]byte{0xB2}, 16)
	values := map[core.ConnectorCredentialName][]byte{
		core.ConnectorCredentialAccessKey: first,
		core.ConnectorCredentialSecretKey: second,
	}
	clearBackupSecretMap(values)
	if len(values) != 0 {
		t.Fatal("clearBackupSecretMap() retained map entries")
	}
	for index, value := range first {
		if value != 0 {
			t.Fatalf("first buffer byte %d = %d, want zero", index, value)
		}
	}
	for index, value := range second {
		if value != 0 {
			t.Fatalf("second buffer byte %d = %d, want zero", index, value)
		}
	}
}

// Rationale: escaped JSON must decode directly into clearable owned bytes,
// including valid surrogate pairs, without retaining immutable value strings.
func TestDecodeDirectCredentialJSONDecodesEscapesIntoOwnedBytes(t *testing.T) {
	// Rationale: byte-oriented parsing must preserve JSON semantics without
	// routing secret values through immutable Go strings.
	expected := map[core.ConnectorCredentialName]core.ConnectorCredentialKind{
		core.ConnectorCredentialAccessKey: core.ConnectorCredentialDirect,
	}
	decoded, err := decodeDirectCredentialJSON(
		[]byte(`{"access\u005fkey":"line\n\uD83D\uDD11"}`),
		expected,
	)
	if err != nil {
		t.Fatalf("decodeDirectCredentialJSON() error = %v", err)
	}
	if got := string(decoded[core.ConnectorCredentialAccessKey]); got != "line\n🔑" {
		t.Fatalf("decoded credential = %q", got)
	}
	clearBackupSecretMap(decoded)
}
