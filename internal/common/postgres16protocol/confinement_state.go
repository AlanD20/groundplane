package postgres16protocol

func (state ConfinementStateShape) Validate() error {
	if state.EncodedSizeBytes == 0 || state.EncodedSizeBytes > confinementMaximumStateBytes || state.Sequence == 0 ||
		state.LaunchIntentSHA256 == (Digest{}) ||
		state.Phase < ConfinementPhaseCreated || state.Phase > ConfinementPhaseRecoveryRetired ||
		state.ProgressPhase < ConfinementPhaseCreated || state.ProgressPhase > ConfinementPhaseIOComplete {
		return invalidConfinement("postgres helper state header is invalid")
	}
	expectedReady := state.ProgressPhase >= ConfinementPhaseGateDurable
	expectedRelease := state.ProgressPhase >= ConfinementPhaseGateReleased
	expectedProfile := state.ProgressPhase >= ConfinementPhaseProfileApplied
	expectedChild := state.ProgressPhase >= ConfinementPhaseChildDurable
	expectedIO := state.ProgressPhase >= ConfinementPhaseIOComplete
	if state.GateReady != expectedReady || state.GateRelease != expectedRelease ||
		state.GateProfile != expectedProfile || state.Child != expectedChild || state.IO != expectedIO ||
		state.GateReady != (state.ReadyFrameSHA256 != (Digest{})) ||
		state.GateProfile != (state.ProfileFrameSHA256 != (Digest{})) ||
		state.IO != (state.IOEvidence != nil) {
		return invalidConfinement("postgres helper state presence is invalid")
	}
	validChildEvidence := state.ExecFD4EOF && (state.ExecEvidence == 1 || state.ExecEvidence == 2)
	validAbsentChildEvidence := !state.ExecFD4EOF && state.ExecEvidence == 0
	if state.Child && !validChildEvidence || !state.Child && !validAbsentChildEvidence {
		return invalidConfinement("postgres helper child evidence is invalid")
	}
	terminalPhase := state.Phase >= ConfinementPhaseTerminal
	if terminalPhase != (state.Terminal != nil) {
		return invalidConfinement("postgres helper terminal presence is invalid")
	}
	if !terminalPhase {
		if state.Phase != state.ProgressPhase || state.GateFatal != nil {
			return invalidConfinement("postgres helper nonterminal phase is invalid")
		}
		return nil
	}
	terminalGateFatal := state.Terminal.Kind == ConfinementTerminalGateFatal
	if (state.GateFatal != nil) != terminalGateFatal {
		return invalidConfinement("postgres helper fatal evidence presence is invalid")
	}
	if state.GateFatal != nil {
		if err := state.GateFatal.Validate(); err != nil {
			return err
		}
		if state.Child || state.IO || state.Terminal.EvidenceSHA256 == state.GateFatal.FrameSHA256 {
			return invalidConfinement("postgres helper fatal state linkage is invalid")
		}
	}
	if (state.Terminal.Kind == ConfinementTerminalExited ||
		state.Terminal.Kind == ConfinementTerminalSignaled) && !state.Child {
		return invalidConfinement("postgres helper client terminal lacks child evidence")
	}
	if err := state.Terminal.Validate(state.Phase); err != nil {
		return err
	}
	expectedTerminalEvidence, err := state.TerminalEvidenceSHA256()
	if err != nil || state.Terminal.EvidenceSHA256 != expectedTerminalEvidence {
		return invalidConfinement("postgres helper terminal evidence digest is invalid")
	}
	return nil
}

func (evidence ConfinementFatalEvidence) Validate() error {
	if !evidence.Stage.Valid() || evidence.Errno == 0 || evidence.Sequence != evidence.Stage.Sequence() ||
		!evidence.FD4EOF || evidence.FrameSHA256 == (Digest{}) {
		return invalidConfinement("postgres helper fatal evidence is invalid")
	}
	return nil
}

func (state ConfinementStateShape) TerminalEvidenceSHA256() (Digest, error) {
	if state.Terminal == nil || state.LaunchIntentSHA256 == (Digest{}) ||
		state.ProgressPhase < ConfinementPhaseCreated || state.ProgressPhase > ConfinementPhaseIOComplete ||
		state.GateReady != (state.ReadyFrameSHA256 != (Digest{})) ||
		state.GateProfile != (state.ProfileFrameSHA256 != (Digest{})) ||
		state.IO != (state.IOEvidence != nil) {
		return Digest{}, invalidConfinement("postgres helper terminal evidence inputs are invalid")
	}
	body := make([]byte, 0, 267)
	body = append(body, state.LaunchIntentSHA256[:]...)
	body = append(body, byte(state.ProgressPhase))
	body = appendOptionalDigest(body, state.GateReady, state.ReadyFrameSHA256)
	body = appendOptionalDigest(body, state.GateProfile, state.ProfileFrameSHA256)
	if state.GateFatal == nil {
		body = append(body, make([]byte, len(Digest{}))...)
	} else {
		if err := state.GateFatal.Validate(); err != nil {
			return Digest{}, err
		}
		body = append(body, state.GateFatal.FrameSHA256[:]...)
	}
	if state.IOEvidence == nil {
		body = appendUint32(body, 0)
	} else {
		body = appendStreamEvidence(body, state.IOEvidence.Stdin)
		body = appendStreamEvidence(body, state.IOEvidence.Stdout)
		body = appendStreamEvidence(body, state.IOEvidence.Stderr)
	}
	body = append(body, byte(state.Terminal.Kind))
	body = appendUint32(body, state.Terminal.ExitCode)
	body = appendUint32(body, state.Terminal.Signal)
	body = appendBool(body, state.Terminal.CoreDumped)
	body = appendUint32(body, state.Terminal.RawWaitStatus)
	body = appendBool(body, state.Terminal.Wait4Reaped)
	return domainSeparatedDigest("groundplane.postgres16.terminal-evidence.v1", body), nil
}

func (terminal ConfinementTerminal) Validate(phase ConfinementStatePhase) error {
	if terminal.EvidenceSHA256 == (Digest{}) {
		return invalidConfinement("postgres helper terminal evidence digest is invalid")
	}
	switch phase {
	case ConfinementPhaseTerminal:
		if terminal.Wait4Reaped || terminal.RawWaitStatus != 0 {
			return invalidConfinement("postgres helper terminal wait evidence is invalid")
		}
		return terminal.validateExitOrSignal()
	case ConfinementPhaseReaped:
		if !terminal.Wait4Reaped {
			return invalidConfinement("postgres helper reaped state lacks wait evidence")
		}
		if err := terminal.validateExitOrSignal(); err != nil {
			return err
		}
		return terminal.validateRawWaitStatus()
	case ConfinementPhaseRecoveryRetired:
		if terminal.Wait4Reaped || terminal.RawWaitStatus != 0 || terminal.ExitCode != 0 || terminal.Signal != 0 ||
			terminal.CoreDumped || terminal.Kind != ConfinementTerminalUnavailableNotParent &&
			terminal.Kind != ConfinementTerminalUnavailableBootChanged {
			return invalidConfinement("postgres helper recovery retirement is invalid")
		}
		return nil
	default:
		return invalidConfinement("postgres helper terminal phase is invalid")
	}
}

func (terminal ConfinementTerminal) validateRawWaitStatus() error {
	if terminal.RawWaitStatus > 0xffff {
		return invalidConfinement("postgres helper raw wait status is out of range")
	}
	termination := terminal.RawWaitStatus & 0x7f
	if terminal.Kind == ConfinementTerminalExited ||
		terminal.Kind == ConfinementTerminalGateFatal && terminal.Signal == 0 {
		if termination != 0 || terminal.RawWaitStatus != terminal.ExitCode<<8 {
			return invalidConfinement("postgres helper raw exit status is inconsistent")
		}
		return nil
	}
	expected := terminal.Signal
	if terminal.CoreDumped {
		expected |= 0x80
	}
	if termination == 0 || termination == 0x7f || terminal.RawWaitStatus != expected {
		return invalidConfinement("postgres helper raw signal status is inconsistent")
	}
	return nil
}

func (terminal ConfinementTerminal) validateExitOrSignal() error {
	switch terminal.Kind {
	case ConfinementTerminalExited:
		if terminal.ExitCode > 255 || terminal.Signal != 0 || terminal.CoreDumped {
			return invalidConfinement("postgres helper exited status is invalid")
		}
	case ConfinementTerminalSignaled:
		if terminal.ExitCode != 0 || terminal.Signal == 0 || terminal.Signal > 64 {
			return invalidConfinement("postgres helper signaled status is invalid")
		}
	case ConfinementTerminalGateFatal:
		validExit := terminal.ExitCode >= 1 && terminal.ExitCode <= 255 && terminal.Signal == 0 &&
			!terminal.CoreDumped
		validSignal := terminal.ExitCode == 0 && terminal.Signal >= 1 && terminal.Signal <= 64
		if !validExit && !validSignal {
			return invalidConfinement("postgres helper gate fatal status is invalid")
		}
	default:
		return invalidConfinement("postgres helper terminal kind is invalid")
	}
	return nil
}

func ValidConfinementTransition(current, next ConfinementStateShape) bool {
	if current.Validate() != nil || next.Validate() != nil || next.Sequence != current.Sequence+1 {
		return false
	}
	if current.Phase <= ConfinementPhaseIOComplete && next.Phase == current.Phase+1 &&
		next.ProgressPhase == next.Phase {
		return true
	}
	if current.Phase <= ConfinementPhaseIOComplete &&
		(next.Phase == ConfinementPhaseTerminal || next.Phase == ConfinementPhaseRecoveryRetired) &&
		next.ProgressPhase == current.ProgressPhase {
		return true
	}
	if current.Phase >= ConfinementPhaseTerminal && current.Phase < ConfinementPhaseRecoveryRetired &&
		next.Phase == ConfinementPhaseRecoveryRetired && next.ProgressPhase == current.ProgressPhase {
		return true
	}
	return validConfinementReapTransition(current, next)
}

func validConfinementReapTransition(current, next ConfinementStateShape) bool {
	if current.Phase != ConfinementPhaseTerminal || next.Phase != ConfinementPhaseReaped ||
		next.ProgressPhase != current.ProgressPhase || current.Terminal == nil || next.Terminal == nil {
		return false
	}
	if current.GateReady != next.GateReady || current.GateRelease != next.GateRelease ||
		current.GateProfile != next.GateProfile || current.Child != next.Child || current.IO != next.IO ||
		current.LaunchIntentSHA256 != next.LaunchIntentSHA256 ||
		current.ReadyFrameSHA256 != next.ReadyFrameSHA256 ||
		current.ProfileFrameSHA256 != next.ProfileFrameSHA256 ||
		current.ExecFD4EOF != next.ExecFD4EOF || current.ExecEvidence != next.ExecEvidence ||
		!equalFatalEvidence(current.GateFatal, next.GateFatal) ||
		!equalIOEvidence(current.IOEvidence, next.IOEvidence) {
		return false
	}
	return current.Terminal.Kind == next.Terminal.Kind &&
		current.Terminal.ExitCode == next.Terminal.ExitCode && current.Terminal.Signal == next.Terminal.Signal &&
		current.Terminal.CoreDumped == next.Terminal.CoreDumped
}
