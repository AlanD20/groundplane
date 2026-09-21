package postgres16protocol

import (
	"slices"
)

func (identity ConfinementFileIdentity) Validate() error {
	if !validText(identity.Path) || identity.Device == 0 || identity.Inode == 0 || identity.SizeBytes == 0 ||
		identity.SHA256 == (Digest{}) {
		return invalidConfinement("postgres helper file identity is incomplete")
	}
	if identity.Mode > 0o7777 || !identity.Regular || !identity.SetUIDAbsent || !identity.SetGIDAbsent ||
		!identity.FileCapabilitiesAbsent {
		return invalidConfinement("postgres helper file identity metadata is invalid")
	}
	return nil
}

func (identity ConfinementProcessIdentity) Validate() error {
	if identity.PID == 0 || identity.ParentPID == 0 || identity.ProcessGroupID == 0 || identity.StartTicks == 0 ||
		identity.BootID == (ConfinementBootID{}) || identity.ThreadCount == 0 {
		return invalidConfinement("postgres helper process identity is incomplete")
	}
	if identity.SeccompMode != 0 && identity.SeccompMode != 2 {
		return invalidConfinement("postgres helper process seccomp mode is invalid")
	}
	if !validGroups(identity.SupplementaryGIDs) {
		return invalidConfinement("postgres helper process groups are invalid")
	}
	return identity.Executable.Validate()
}

func (authority ConfinementReleaseAuthority) Validate() error {
	if authority.HelperSHA256 == (Digest{}) || authority.GateSHA256 == (Digest{}) ||
		authority.PGDumpSHA256 == (Digest{}) || authority.PGRestoreSHA256 == (Digest{}) ||
		authority.PSQLSHA256 == (Digest{}) || authority.LaunchProfileSHA256 == (Digest{}) ||
		authority.GateSeccompSHA256 == (Digest{}) {
		return invalidConfinement("postgres helper release authority is incomplete")
	}
	return nil
}

func (authority ConfinementReleaseAuthority) clientSHA256(operation Operation) (Digest, error) {
	switch operation {
	case OperationProbePGDump, OperationDump:
		return authority.PGDumpSHA256, nil
	case OperationProbePGRestore, OperationRestoreList, OperationRestoreApply:
		return authority.PGRestoreSHA256, nil
	case OperationProbePSQL, OperationServerMajor, OperationTerminateDBConnections,
		OperationAssertZeroDBConnections, OperationPostRestoreVerify:
		return authority.PSQLSHA256, nil
	default:
		return Digest{}, invalidConfinement("postgres helper release client is invalid")
	}
}

func (identity ConfinementProcessIdentity) ValidateRootSupervisor(
	authority ConfinementReleaseAuthority,
) error {
	if err := authority.Validate(); err != nil {
		return err
	}
	if err := identity.Validate(); err != nil {
		return err
	}
	if !identity.hasCredentials(0) || len(identity.SupplementaryGIDs) != 0 ||
		identity.CapabilityInh != 0 || identity.CapabilityPrm != confinementRootCapabilityMask ||
		identity.CapabilityEff != confinementRootCapabilityMask ||
		identity.CapabilityBnd != confinementRootCapabilityMask || identity.CapabilityAmb != 0 ||
		!identity.NoNewPrivileges || identity.SeccompMode != 0 ||
		identity.Executable.Path != HelperPath ||
		identity.Executable.UID != HelperUID ||
		identity.Executable.GID != HelperGID ||
		identity.Executable.Mode != HelperMode ||
		identity.Executable.SHA256 != authority.HelperSHA256 {
		return invalidConfinement("postgres helper root supervisor identity is invalid")
	}
	return nil
}

func (identity ConfinementProcessIdentity) ValidateReadyGate(
	intent ConfinementLaunchIntent,
	observedParent ConfinementProcessIdentity,
	authority ConfinementReleaseAuthority,
) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if err := intent.Supervisor.ValidateRootSupervisor(authority); err != nil {
		return err
	}
	if err := observedParent.ValidateRootSupervisor(authority); err != nil {
		return err
	}
	if !equalProcessIdentity(observedParent, intent.Supervisor) ||
		identity.ParentPID != intent.Supervisor.PID || identity.BootID != intent.Supervisor.BootID ||
		identity.ProcessGroupID != identity.PID ||
		!identity.hasCredentials(0) || len(identity.SupplementaryGIDs) != 0 || identity.ThreadCount != 1 ||
		identity.CapabilityInh != 0 || identity.CapabilityPrm != confinementRootCapabilityMask ||
		identity.CapabilityEff != confinementRootCapabilityMask ||
		identity.CapabilityBnd != confinementRootCapabilityMask || identity.CapabilityAmb != 0 ||
		!identity.NoNewPrivileges || identity.SeccompMode != 0 ||
		!equalFileIdentity(identity.Executable, intent.GateFile) ||
		identity.ArgvSHA256 != intent.GateArgvSHA256 ||
		identity.EnvironmentSHA256 != intent.GateEnvironmentSHA256 ||
		identity.FDSetSHA256 != intent.GateReadyFDSetSHA256 {
		return invalidConfinement("postgres helper ready gate identity is invalid")
	}
	return nil
}

func (identity ConfinementProcessIdentity) hasCredentials(value uint32) bool {
	return identity.RealUID == value && identity.EffectiveUID == value && identity.SavedUID == value &&
		identity.FilesystemUID == value && identity.RealGID == value && identity.EffectiveGID == value &&
		identity.SavedGID == value && identity.FilesystemGID == value
}

func (profile ConfinementSecurityProfile) hasCredentials(value uint32) bool {
	return profile.RealUID == value && profile.EffectiveUID == value && profile.SavedUID == value &&
		profile.FilesystemUID == value
}

func equalFileIdentity(left, right ConfinementFileIdentity) bool {
	return left.Path == right.Path && left.Device == right.Device && left.Inode == right.Inode &&
		left.SizeBytes == right.SizeBytes && left.SHA256 == right.SHA256 && left.UID == right.UID &&
		left.GID == right.GID && left.Mode == right.Mode && left.Regular == right.Regular &&
		left.SetUIDAbsent == right.SetUIDAbsent && left.SetGIDAbsent == right.SetGIDAbsent &&
		left.FileCapabilitiesAbsent == right.FileCapabilitiesAbsent
}

func equalProcessIdentity(left, right ConfinementProcessIdentity) bool {
	return left.PID == right.PID && left.ParentPID == right.ParentPID &&
		left.ProcessGroupID == right.ProcessGroupID && left.StartTicks == right.StartTicks &&
		left.BootID == right.BootID && left.RealUID == right.RealUID &&
		left.EffectiveUID == right.EffectiveUID && left.SavedUID == right.SavedUID &&
		left.FilesystemUID == right.FilesystemUID && left.RealGID == right.RealGID &&
		left.EffectiveGID == right.EffectiveGID && left.SavedGID == right.SavedGID &&
		left.FilesystemGID == right.FilesystemGID &&
		slices.Equal(left.SupplementaryGIDs, right.SupplementaryGIDs) &&
		left.CapabilityInh == right.CapabilityInh && left.CapabilityPrm == right.CapabilityPrm &&
		left.CapabilityEff == right.CapabilityEff && left.CapabilityBnd == right.CapabilityBnd &&
		left.CapabilityAmb == right.CapabilityAmb && left.NoNewPrivileges == right.NoNewPrivileges &&
		left.SeccompMode == right.SeccompMode && left.ThreadCount == right.ThreadCount &&
		equalFileIdentity(left.Executable, right.Executable) && left.ArgvSHA256 == right.ArgvSHA256 &&
		left.EnvironmentSHA256 == right.EnvironmentSHA256 && left.FDSetSHA256 == right.FDSetSHA256
}

func equalFatalEvidence(left, right *ConfinementFatalEvidence) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func equalIOEvidence(left, right *ConfinementIOEvidence) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}
