package attachments

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: native provisioning and deletion share this secret boundary.
// Consumer failure must erase its password copy without changing captured
// authentication, credentials or grant data needed for recovery.
func TestCapturedIdentityConsumerFailureClearsOnlyOwnedSecrets(t *testing.T) {
	for _, mode := range []core.BackingAuthentication{
		"", core.BackingAuthenticationUsernamePassword, core.BackingAuthenticationPassword, core.BackingAuthenticationNone,
	} {
		t.Run(string(mode), func(t *testing.T) {
			password, role := []byte("captured-password"), "consumer"
			if mode == core.BackingAuthenticationPassword {
				role = "default"
			} else if mode == core.BackingAuthenticationNone {
				role, password = "", nil
			}
			bundle := attachFactBundle{Identity: attachTaskIdentity{
				Authentication: mode, Database: "consumer", Role: role, Password: password,
				Grants: []attachTaskGrantIdentity{{AttachID: "grant", Database: "shared"}},
			}}
			var consumedPassword []byte
			called := false
			err := consumeAttachTaskIdentity(&bundle, func(identity attachplan.Identity) error {
				called = true
				if identity.Authentication != mode || identity.Database != "consumer" || identity.Role != role ||
					!bytes.Equal(identity.Password, password) || len(identity.Grants) != 1 ||
					identity.Grants[0].Database != "shared" {
					t.Fatal("captured identity changed before consumption")
				}
				consumedPassword = identity.Password
				identity.Grants[0].Database = "changed-copy"
				return context.Canceled
			})
			if err != context.Canceled || !called {
				t.Fatalf("consume captured identity = %v, called=%t", err, called)
			}
			if !bytes.Equal(consumedPassword, make([]byte, len(consumedPassword))) ||
				len(password) != 0 && string(password) != "captured-password" ||
				bundle.Identity.Grants[0].Database != "shared" {
				t.Fatal("consumer cleanup changed captured secrets or grant ownership")
			}
		})
	}
}
