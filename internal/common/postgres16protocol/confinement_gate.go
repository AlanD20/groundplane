package postgres16protocol

func (status ConfinementGateStatus) Validate(
	intent ConfinementLaunchIntent,
	authority ConfinementReleaseAuthority,
	expectedIntentSHA256 Digest,
	observedParent ConfinementProcessIdentity,
) error {
	if err := intent.Validate(authority); err != nil {
		return err
	}
	if status.Schema != 1 || status.Nonce != intent.Nonce || status.IntentSHA256 != expectedIntentSHA256 {
		return invalidConfinement("postgres helper gate status header is invalid")
	}
	switch status.Kind {
	case ConfinementGateStatusReady:
		if status.Sequence != 1 || status.Ready == nil || status.Profile != nil ||
			status.FDSetSHA256 != (Digest{}) || status.FatalStage != 0 ||
			status.FatalErrno != 0 {
			return invalidConfinement("postgres helper ready status is invalid")
		}
		return status.Ready.ValidateReadyGate(intent, observedParent, authority)
	case ConfinementGateStatusProfileApplied:
		if status.Sequence != 2 || status.Ready != nil || status.Profile == nil ||
			status.FDSetSHA256 == (Digest{}) || status.FDSetSHA256 != intent.GateProfileFDSetSHA256 ||
			status.FatalStage != 0 || status.FatalErrno != 0 {
			return invalidConfinement("postgres helper profile status is invalid")
		}
		return status.Profile.ValidateChild(intent.GateSeccompSHA256)
	case ConfinementGateStatusFatal:
		if status.Ready != nil || status.Profile != nil || status.FDSetSHA256 != (Digest{}) ||
			!status.FatalStage.Valid() || status.Sequence != status.FatalStage.Sequence() || status.FatalErrno == 0 {
			return invalidConfinement("postgres helper fatal status is invalid")
		}
		return nil
	default:
		return invalidConfinement("postgres helper gate status kind is invalid")
	}
}

func (stage ConfinementFatalStage) Valid() bool {
	return stage >= ConfinementFatalSetProcessGroup && stage <= ConfinementFatalExec
}

func (stage ConfinementFatalStage) Sequence() uint32 {
	switch {
	case stage >= ConfinementFatalSetProcessGroup && stage <= ConfinementFatalReadyFrame:
		return 1
	case stage >= ConfinementFatalReleaseRead && stage <= ConfinementFatalProfileFrame:
		return 2
	case stage == ConfinementFatalExec:
		return 3
	default:
		return 0
	}
}
