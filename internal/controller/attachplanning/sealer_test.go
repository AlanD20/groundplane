package attachplanning

import (
	"bytes"
	"context"
	"testing"

	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// QA: BACK-05, ATT-10; local source-copy proof, not live authentication.
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
			current := testkeyvalue.Versioned[testattachments.Record]{
				Record: testattachments.Record{ID: "draft-attach"},
			}
			state := draftAttachPlanState{
				current: current,
				identity: &testtaskplanning.AttachPlanIdentity{
					Authentication: mode, Database: "consumer", Role: role, Password: password,
					Grants: []testtaskplanning.AttachPlanGrantIdentity{{AttachID: "grant", Database: "shared"}},
				},
			}
			var consumedPassword []byte
			called := false
			err := state.ResolveTaskIdentity(
				t.Context(),
				current,
				"draft-task",
				func(identity testtaskplanning.AttachPlanIdentity) error {
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
