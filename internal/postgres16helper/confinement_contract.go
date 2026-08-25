package postgres16helper

import "github.com/AlanD20/groundplane/internal/common/postgres16protocol"

func validateConfinementLaunch(
	intent postgres16protocol.ConfinementLaunchIntent,
	authority postgres16protocol.ConfinementReleaseAuthority,
) error {
	return intent.Validate(authority)
}

func validateConfinementGateStatus(
	status postgres16protocol.ConfinementGateStatus,
	intent postgres16protocol.ConfinementLaunchIntent,
	authority postgres16protocol.ConfinementReleaseAuthority,
	intentSHA256 postgres16protocol.Digest,
	observedParent postgres16protocol.ConfinementProcessIdentity,
) error {
	return status.Validate(intent, authority, intentSHA256, observedParent)
}

func validateConfinementState(state postgres16protocol.ConfinementStateShape) error {
	return state.Validate()
}

func validConfinementTransition(
	current postgres16protocol.ConfinementStateShape,
	next postgres16protocol.ConfinementStateShape,
) bool {
	return postgres16protocol.ValidConfinementTransition(current, next)
}
