package app

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

// Rationale: the locked backing identity uses the service name and the first six characters of the
// ULID random tail, not the timestamp or mutable Attach name.
func TestAttachProvisionIdentityUsesRandomULIDTail(t *testing.T) {
	identity, err := attachProvisionIdentity("att_01ARZ3NDEKTSV4RRFFQ69G5FAV", "api-web")
	if err != nil {
		t.Fatalf("attachProvisionIdentity() error = %v", err)
	}
	if identity != "api-web_tsv4rr" {
		t.Fatalf("attachProvisionIdentity() = %q, want api-web_tsv4rr", identity)
	}
	if _, err := attachProvisionIdentity(
		"att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		strings.Repeat("a", maximumAttachServiceNameLen+1),
	); err == nil {
		t.Fatal("attachProvisionIdentity() accepted an identity PostgreSQL would truncate")
	}
}

// Rationale: every credential-backed Attach must receive exactly 256 bits of entropy encoded without
// padding in the URL-safe alphabet used by connection facts.
func TestGenerateAttachPasswordUsesExactURLSafeEncoding(t *testing.T) {
	raw := make([]byte, attachPasswordEntropyBytes)
	for index := range raw {
		raw[index] = byte(index)
	}
	password, err := generateAttachPassword(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("generateAttachPassword() error = %v", err)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(string(password))
	if err != nil {
		t.Fatalf("generated password is not unpadded base64url: %v", err)
	}
	if len(password) != 43 || bytes.ContainsRune(password, '=') || !bytes.Equal(decoded, raw) {
		t.Fatalf("generated password = %q, decoded = %x", password, decoded)
	}
}

// Rationale: omitted Attach names must be reproducible from the four current labels and select the
// lowest numeric suffix without changing their stored name after creation.
func TestSuggestAttachNameNormalizesLabelsAndUsesLowestSuffix(t *testing.T) {
	base := "acme-platform-production-api-worker"
	name, err := suggestAttachName(attachNameLabels{
		tenant: "Acme", project: "platform", environment: "production", service: "api_worker",
	}, map[string]struct{}{base: {}, base + "-2": {}})
	if err != nil {
		t.Fatalf("suggestAttachName() error = %v", err)
	}
	if name != base+"-3" {
		t.Fatalf("suggestAttachName() = %q, want %q", name, base+"-3")
	}
}
