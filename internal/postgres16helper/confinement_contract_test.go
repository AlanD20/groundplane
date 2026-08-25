package postgres16helper

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
)

// Rationale: the helper must delegate to the sole shared contract and fail
// closed until the root-supervisor and client-gate runtime supplies real proof.
func TestConfinementDelegationRejectsEmptyEvidence(t *testing.T) {
	if err := validateConfinementLaunch(
		postgres16protocol.ConfinementLaunchIntent{},
		postgres16protocol.ConfinementReleaseAuthority{},
	); err == nil {
		t.Fatal("empty launch evidence accepted")
	}
	if err := validateConfinementGateStatus(
		postgres16protocol.ConfinementGateStatus{},
		postgres16protocol.ConfinementLaunchIntent{},
		postgres16protocol.ConfinementReleaseAuthority{},
		postgres16protocol.Digest{},
		postgres16protocol.ConfinementProcessIdentity{},
	); err == nil {
		t.Fatal("empty gate evidence accepted")
	}
	if err := validateConfinementState(postgres16protocol.ConfinementStateShape{}); err == nil {
		t.Fatal("empty durable state accepted")
	}
	if validConfinementTransition(
		postgres16protocol.ConfinementStateShape{},
		postgres16protocol.ConfinementStateShape{},
	) {
		t.Fatal("empty durable transition accepted")
	}
}
