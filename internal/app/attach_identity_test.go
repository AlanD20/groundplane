package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"testing"

	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
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

// Rationale: standalone Attach publication must retain the selected authentication when it
// copies a draft identity, and clearing the temporary password must not erase the source.
func TestDraftAttachIdentityPreservesAuthenticationAndSecretOwnership(t *testing.T) {
	for _, mode := range []core.BackingAuthentication{
		"", core.BackingAuthenticationUsernamePassword, core.BackingAuthenticationPassword, core.BackingAuthenticationNone,
	} {
		t.Run(string(mode), func(t *testing.T) {
			password := []byte("draft-password")
			role := "consumer"
			if mode == core.BackingAuthenticationPassword {
				role = "default"
			} else if mode == core.BackingAuthenticationNone {
				role, password = "", nil
			}
			current := etcd.Versioned[etcd.AttachRecord]{Record: etcd.AttachRecord{ID: "draft-attach"}}
			state := draftAttachPlanState{
				current: current,
				identity: &controllerpkg.AttachPlanIdentity{
					Authentication: mode, Database: "consumer", Role: role, Password: password,
					Grants: []controllerpkg.AttachPlanGrantIdentity{{AttachID: "grant", Database: "shared"}},
				},
			}
			var consumedPassword []byte
			called := false
			err := state.ResolveTaskIdentity(
				t.Context(),
				current,
				"draft-task",
				func(identity controllerpkg.AttachPlanIdentity) error {
					called = true
					if identity.Authentication != mode || identity.Database != "consumer" || identity.Role != role ||
						!bytes.Equal(identity.Password, password) || len(identity.Grants) != 1 ||
						identity.Grants[0] != state.identity.Grants[0] {
						t.Fatal("draft identity changed authentication or credential fields")
					}
					consumedPassword = identity.Password
					identity.Grants[0].Database = "changed-copy"
					return context.Canceled
				},
			)
			if err != context.Canceled || !called {
				t.Fatalf("ResolveTaskIdentity() = %v, called = %t", err, called)
			}
			if !bytes.Equal(consumedPassword, make([]byte, len(consumedPassword))) ||
				len(password) != 0 &&
					string(password) != "draft-password" || state.identity.Grants[0].Database != "shared" {
				t.Fatal("draft identity cleanup or copy changed source ownership")
			}
		})
	}
}
