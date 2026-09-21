package postgres16protocol

func (profile ConfinementSecurityProfile) ValidateChild(expectedSeccomp Digest) error {
	if !profile.hasCredentials(PostgreSQLUID) ||
		profile.RealGID != PostgreSQLGID ||
		profile.EffectiveGID != PostgreSQLGID ||
		profile.SavedGID != PostgreSQLGID ||
		profile.FilesystemGID != PostgreSQLGID ||
		len(profile.SupplementaryGIDs) != 0 || profile.LastCapability > 63 ||
		profile.CapabilityInh != 0 || profile.CapabilityPrm != 0 || profile.CapabilityEff != 0 ||
		profile.CapabilityBnd != 0 || profile.CapabilityAmb != 0 || !profile.NoNewPrivileges ||
		profile.SeccompSHA256 != expectedSeccomp || !profile.ForkSyscallsDenied {
		return invalidConfinement("postgres helper child security profile is invalid")
	}
	return nil
}

func (intent ConfinementLaunchIntent) Validate(authority ConfinementReleaseAuthority) error {
	if err := authority.Validate(); err != nil {
		return err
	}
	if intent.Schema != 1 || intent.Nonce == (Nonce{}) ||
		!intent.Operation.ValidRun() || intent.DeadlineUnixNano == 0 {
		return invalidConfinement("postgres helper launch intent header is invalid")
	}
	if err := intent.Supervisor.ValidateRootSupervisor(authority); err != nil {
		return err
	}
	if err := validateExactFile(
		intent.GateFile,
		ClientGatePath,
		ClientGateMode,
	); err != nil {
		return err
	}
	if intent.GateFile.SHA256 != authority.GateSHA256 {
		return invalidConfinement("postgres helper launch gate release is invalid")
	}
	clientPath, err := ClientPath(intent.Operation)
	if err != nil {
		return invalidConfinement("postgres helper launch client is invalid")
	}
	if err := validateClientFile(intent.ClientFile, clientPath); err != nil {
		return err
	}
	clientSHA256, err := authority.clientSHA256(intent.Operation)
	if err != nil || intent.ClientFile.SHA256 != clientSHA256 {
		return invalidConfinement("postgres helper launch client release is invalid")
	}
	if !validArguments(intent.Arguments) || !validClientArguments(intent.Operation, intent.Arguments) ||
		!validEnvironment(intent.Environment) {
		return invalidConfinement("postgres helper launch vectors are invalid")
	}
	expectedProfile, err := ProfileFor(intent.Operation)
	if err != nil || intent.FDProfile != expectedProfile {
		return invalidConfinement("postgres helper launch fd profile is invalid")
	}
	if err := intent.ChildSecurity.ValidateChild(intent.GateSeccompSHA256); err != nil {
		return err
	}
	if intent.NoFileLimit != ClientNoFileLimit {
		return invalidConfinement("postgres helper launch nofile limit is invalid")
	}
	if intent.LaunchProfileSHA256 != authority.LaunchProfileSHA256 ||
		intent.GateSeccompSHA256 != authority.GateSeccompSHA256 || intent.GateArgvSHA256 == (Digest{}) ||
		intent.GateEnvironmentSHA256 == (Digest{}) || intent.GateReadyFDSetSHA256 == (Digest{}) ||
		intent.GateProfileFDSetSHA256 == (Digest{}) {
		return invalidConfinement("postgres helper launch sealed identity is invalid")
	}
	return nil
}
