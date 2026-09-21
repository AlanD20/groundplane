package taskplanning

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: Controller encoding must map every durable mode explicitly so a
// protobuf enum reordering cannot silently change Agent authorization.
func TestBackingAuthenticationEncodingIsExplicit(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		mode core.BackingAuthentication
		wire agentpb.BackingAuthentication
	}{
		{"", agentpb.BackingAuthentication_BACKING_AUTHENTICATION_UNSPECIFIED},
		{core.BackingAuthenticationUsernamePassword, agentpb.BackingAuthentication_BACKING_AUTHENTICATION_USERNAME_PASSWORD},
		{core.BackingAuthenticationPassword, agentpb.BackingAuthentication_BACKING_AUTHENTICATION_PASSWORD},
		{core.BackingAuthenticationNone, agentpb.BackingAuthentication_BACKING_AUTHENTICATION_NONE},
	} {
		wire, err := encodeBackingAuthentication(test.mode)
		if err != nil || wire != test.wire {
			t.Fatalf("encodeBackingAuthentication(%q) = %v, %v", test.mode, wire, err)
		}
	}
	if _, err := encodeBackingAuthentication("invalid"); err == nil {
		t.Fatal("encodeBackingAuthentication(invalid) succeeded")
	}
}
