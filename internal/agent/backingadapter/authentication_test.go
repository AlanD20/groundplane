package backingadapter

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: the Agent must reject unset, unknown, or adapter-incompatible
// wire modes before compiling any side effect.
func TestBackingAuthenticationDecodingIsExplicit(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		wire agentpb.BackingAuthentication
		mode core.BackingAuthentication
	}{
		{agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD, core.BackingAuthenticationUsernamePassword},
		{agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD, core.BackingAuthenticationPassword},
		{agentpb.BackingAuthentication_BACKING_AUTHENTICATION_NONE, core.BackingAuthenticationNone},
	} {
		mode, err := decodeBackingAuthentication(test.wire, true)
		if err != nil || mode != test.mode {
			t.Fatalf("decodeBackingAuthentication(%v, true) = %q, %v", test.wire, mode, err)
		}
	}
	mode, err := decodeBackingAuthentication(
		agentpb.BackingAuthentication_BACKING_AUTHENTICATION_UNSPECIFIED, false,
	)
	if err != nil || mode != "" {
		t.Fatalf("decodeBackingAuthentication(unspecified, false) = %q, %v", mode, err)
	}
	if _, err := decodeBackingAuthentication(
		agentpb.BackingAuthentication_BACKING_AUTHENTICATION_UNSPECIFIED, true,
	); err == nil {
		t.Fatal("decodeBackingAuthentication(unset selectable mode) succeeded")
	}
	if _, err := decodeBackingAuthentication(
		agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD, false,
	); err == nil {
		t.Fatal("decodeBackingAuthentication(explicit unsupported mode) succeeded")
	}
}
