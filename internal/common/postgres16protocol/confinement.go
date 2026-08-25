package postgres16protocol

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/postgresidentity"
	grounderrs "github.com/AlanD20/groundplane/pkg/errs"
)

const (
	confinementMaximumTextBytes      = 16 * 1024
	confinementMaximumArguments      = 32
	confinementMaximumArgumentBytes  = 4 * 1024
	confinementMaximumArgumentsBytes = 16 * 1024
	confinementMaximumGroups         = 64
	confinementMaximumIntentBytes    = 32 * 1024
	confinementMaximumStateBytes     = 64 * 1024
	confinementMaximumStatusBytes    = 4 * 1024
)

const confinementRootCapabilityMask uint64 = 1<<0 | // CHOWN
	1<<1 | // DAC_OVERRIDE
	1<<3 | // FOWNER
	1<<5 | // KILL
	1<<6 | // SETGID
	1<<7 | // SETUID
	1<<8 | // SETPCAP
	1<<19 // SYS_PTRACE

type ConfinementBootID [16]byte

type ConfinementFileIdentity struct {
	Path                   string
	Device                 uint64
	Inode                  uint64
	SizeBytes              uint64
	SHA256                 Digest
	UID                    uint32
	GID                    uint32
	Mode                   uint32
	Regular                bool
	SetUIDAbsent           bool
	SetGIDAbsent           bool
	FileCapabilitiesAbsent bool
}

type ConfinementProcessIdentity struct {
	PID               uint32
	ParentPID         uint32
	ProcessGroupID    uint32
	StartTicks        uint64
	BootID            ConfinementBootID
	RealUID           uint32
	EffectiveUID      uint32
	SavedUID          uint32
	FilesystemUID     uint32
	RealGID           uint32
	EffectiveGID      uint32
	SavedGID          uint32
	FilesystemGID     uint32
	SupplementaryGIDs []uint32
	CapabilityInh     uint64
	CapabilityPrm     uint64
	CapabilityEff     uint64
	CapabilityBnd     uint64
	CapabilityAmb     uint64
	NoNewPrivileges   bool
	SeccompMode       uint8
	ThreadCount       uint32
	Executable        ConfinementFileIdentity
	ArgvSHA256        Digest
	EnvironmentSHA256 Digest
	FDSetSHA256       Digest
}

type ConfinementSecurityProfile struct {
	RealUID            uint32
	EffectiveUID       uint32
	SavedUID           uint32
	FilesystemUID      uint32
	RealGID            uint32
	EffectiveGID       uint32
	SavedGID           uint32
	FilesystemGID      uint32
	SupplementaryGIDs  []uint32
	LastCapability     uint32
	CapabilityInh      uint64
	CapabilityPrm      uint64
	CapabilityEff      uint64
	CapabilityBnd      uint64
	CapabilityAmb      uint64
	NoNewPrivileges    bool
	SeccompSHA256      Digest
	ForkSyscallsDenied bool
}

type ConfinementLaunchIntent struct {
	Schema                 uint32
	Nonce                  Nonce
	Operation              Operation
	DeadlineUnixNano       uint64
	Supervisor             ConfinementProcessIdentity
	GateFile               ConfinementFileIdentity
	ClientFile             ConfinementFileIdentity
	Arguments              [][]byte
	Environment            [][]byte
	FDProfile              FDProfile
	ChildSecurity          ConfinementSecurityProfile
	NoFileLimit            uint32
	LaunchProfileSHA256    Digest
	GateSeccompSHA256      Digest
	GateArgvSHA256         Digest
	GateEnvironmentSHA256  Digest
	GateReadyFDSetSHA256   Digest
	GateProfileFDSetSHA256 Digest
}

// ConfinementReleaseAuthority contains the immutable measurements published
// for one first-party PostgreSQL 16 helper release.
type ConfinementReleaseAuthority struct {
	HelperSHA256        Digest
	GateSHA256          Digest
	PGDumpSHA256        Digest
	PGRestoreSHA256     Digest
	PSQLSHA256          Digest
	LaunchProfileSHA256 Digest
	GateSeccompSHA256   Digest
}

type ConfinementGateStatusKind uint8

const (
	ConfinementGateStatusReady          ConfinementGateStatusKind = 1
	ConfinementGateStatusProfileApplied ConfinementGateStatusKind = 2
	ConfinementGateStatusFatal          ConfinementGateStatusKind = 3
)

type ConfinementFatalStage uint8

const (
	ConfinementFatalSetProcessGroup       ConfinementFatalStage = 1
	ConfinementFatalSetLimit              ConfinementFatalStage = 2
	ConfinementFatalInitialParentDeath    ConfinementFatalStage = 3
	ConfinementFatalInitialParentIdentity ConfinementFatalStage = 4
	ConfinementFatalIntentRebuild         ConfinementFatalStage = 5
	ConfinementFatalReadyFrame            ConfinementFatalStage = 6
	ConfinementFatalReleaseRead           ConfinementFatalStage = 7
	ConfinementFatalPostReleaseParent     ConfinementFatalStage = 8
	ConfinementFatalLastCapability        ConfinementFatalStage = 9
	ConfinementFatalBoundingDrop          ConfinementFatalStage = 10
	ConfinementFatalGroups                ConfinementFatalStage = 11
	ConfinementFatalGIDs                  ConfinementFatalStage = 12
	ConfinementFatalUIDs                  ConfinementFatalStage = 13
	ConfinementFatalCapabilityClear       ConfinementFatalStage = 14
	ConfinementFatalNoNewPrivileges       ConfinementFatalStage = 15
	ConfinementFatalParentDeathRearm      ConfinementFatalStage = 16
	ConfinementFatalFinalParentIdentity   ConfinementFatalStage = 17
	ConfinementFatalClientFile            ConfinementFatalStage = 18
	ConfinementFatalSeccomp               ConfinementFatalStage = 19
	ConfinementFatalCloseRangeOrFDSet     ConfinementFatalStage = 20
	ConfinementFatalProfileFrame          ConfinementFatalStage = 21
	ConfinementFatalExec                  ConfinementFatalStage = 22
)

type ConfinementGateStatus struct {
	Schema       uint32
	Sequence     uint32
	Kind         ConfinementGateStatusKind
	Nonce        Nonce
	IntentSHA256 Digest
	Ready        *ConfinementProcessIdentity
	Profile      *ConfinementSecurityProfile
	FDSetSHA256  Digest
	FatalStage   ConfinementFatalStage
	FatalErrno   uint32
}

type ConfinementStatePhase uint8

const (
	ConfinementPhaseCreated         ConfinementStatePhase = 1
	ConfinementPhaseGateDurable     ConfinementStatePhase = 2
	ConfinementPhaseGateReleased    ConfinementStatePhase = 3
	ConfinementPhaseProfileApplied  ConfinementStatePhase = 4
	ConfinementPhaseChildDurable    ConfinementStatePhase = 5
	ConfinementPhaseIOComplete      ConfinementStatePhase = 6
	ConfinementPhaseTerminal        ConfinementStatePhase = 7
	ConfinementPhaseReaped          ConfinementStatePhase = 8
	ConfinementPhaseRecoveryRetired ConfinementStatePhase = 9
)

type ConfinementTerminalKind uint8

const (
	ConfinementTerminalExited                 ConfinementTerminalKind = 1
	ConfinementTerminalSignaled               ConfinementTerminalKind = 2
	ConfinementTerminalGateFatal              ConfinementTerminalKind = 3
	ConfinementTerminalUnavailableNotParent   ConfinementTerminalKind = 4
	ConfinementTerminalUnavailableBootChanged ConfinementTerminalKind = 5
)

type ConfinementTerminal struct {
	Kind           ConfinementTerminalKind
	ExitCode       uint32
	Signal         uint32
	CoreDumped     bool
	RawWaitStatus  uint32
	Wait4Reaped    bool
	EvidenceSHA256 Digest
}

type ConfinementFatalEvidence struct {
	Stage       ConfinementFatalStage
	Errno       uint32
	Sequence    uint32
	FD4EOF      bool
	FrameSHA256 Digest
}

type ConfinementStreamEvidence struct {
	Bytes  uint64
	SHA256 Digest
	EOF    bool
}

type ConfinementIOEvidence struct {
	Stdin  ConfinementStreamEvidence
	Stdout ConfinementStreamEvidence
	Stderr ConfinementStreamEvidence
}

type ConfinementStateShape struct {
	EncodedSizeBytes   uint64
	Sequence           uint64
	Phase              ConfinementStatePhase
	ProgressPhase      ConfinementStatePhase
	LaunchIntentSHA256 Digest
	GateReady          bool
	ReadyFrameSHA256   Digest
	GateRelease        bool
	GateProfile        bool
	ProfileFrameSHA256 Digest
	GateFatal          *ConfinementFatalEvidence
	Child              bool
	IO                 bool
	IOEvidence         *ConfinementIOEvidence
	Terminal           *ConfinementTerminal
	ExecFD4EOF         bool
	ExecEvidence       uint8
}

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

func appendOptionalDigest(body []byte, present bool, digest Digest) []byte {
	if present {
		return append(body, digest[:]...)
	}
	return append(body, make([]byte, len(Digest{}))...)
}

func appendStreamEvidence(body []byte, evidence ConfinementStreamEvidence) []byte {
	body = appendUint64(body, evidence.Bytes)
	body = append(body, evidence.SHA256[:]...)
	return appendBool(body, evidence.EOF)
}

func appendUint32(body []byte, value uint32) []byte {
	return append(
		body,
		byte(value>>24),
		byte(value>>16),
		byte(value>>8),
		byte(value),
	)
}

func appendUint64(body []byte, value uint64) []byte {
	return append(
		body,
		byte(value>>56),
		byte(value>>48),
		byte(value>>40),
		byte(value>>32),
		byte(value>>24),
		byte(value>>16),
		byte(value>>8),
		byte(value),
	)
}

func appendBool(body []byte, value bool) []byte {
	if value {
		return append(body, 1)
	}
	return append(body, 0)
}

func domainSeparatedDigest(domain string, body []byte) Digest {
	preimage := make([]byte, 0, len(domain)+1+len(body))
	preimage = append(preimage, domain...)
	preimage = append(preimage, 0)
	preimage = append(preimage, body...)
	return Digest(sha256.Sum256(preimage))
}

func validateExactFile(identity ConfinementFileIdentity, expectedPath string, expectedMode uint32) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.Path != expectedPath || identity.UID != 0 || identity.GID != 0 || identity.Mode != expectedMode {
		return invalidConfinement("postgres helper fixed file identity is invalid")
	}
	return nil
}

func validateClientFile(identity ConfinementFileIdentity, expectedPath string) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.Path != expectedPath || identity.UID != 0 || identity.GID != 0 || identity.Mode&0o6022 != 0 {
		return invalidConfinement("postgres helper client file identity is invalid")
	}
	return nil
}

func validText(value string) bool {
	return value != "" && len(value) <= confinementMaximumTextBytes && utf8.ValidString(value) &&
		!strings.ContainsRune(value, 0)
}

func validGroups(groups []uint32) bool {
	if len(groups) > confinementMaximumGroups {
		return false
	}
	for index := 1; index < len(groups); index++ {
		if groups[index-1] >= groups[index] {
			return false
		}
	}
	return true
}

func validArguments(arguments [][]byte) bool {
	if len(arguments) == 0 || len(arguments) > confinementMaximumArguments {
		return false
	}
	total := uint64(0)
	for _, argument := range arguments {
		if len(argument) == 0 || len(argument) > confinementMaximumArgumentBytes || bytes.IndexByte(argument, 0) >= 0 {
			return false
		}
		total += uint64(len(argument))
		if total > confinementMaximumArgumentsBytes {
			return false
		}
	}
	return true
}

func validClientArguments(operation Operation, arguments [][]byte) bool {
	values := make([]string, len(arguments))
	for index := range arguments {
		values[index] = string(arguments[index])
	}
	switch operation {
	case OperationProbePGDump:
		return equalArgumentValues(values, []string{"pg_dump", "--version"})
	case OperationProbePGRestore:
		return equalArgumentValues(values, []string{"pg_restore", "--version"})
	case OperationProbePSQL:
		return equalArgumentValues(values, []string{"psql", "--version"})
	case OperationServerMajor:
		return validPSQLArguments(
			values,
			"SELECT pg_catalog.current_setting('server_version_num')::integer / 10000;",
		)
	case OperationDump:
		return validDumpArguments(values)
	case OperationRestoreList:
		return equalArgumentValues(values, []string{"pg_restore", "--list", "--no-password"})
	case OperationTerminateDBConnections:
		return validPSQLArguments(
			values,
			"SELECT pg_catalog.coalesce(pg_catalog.bool_and("+
				"pg_catalog.pg_terminate_backend(a.pid)), true) "+
				"FROM pg_catalog.pg_stat_activity AS a "+
				"WHERE a.datname = pg_catalog.current_database() "+
				"AND a.pid <> pg_catalog.pg_backend_pid();",
		)
	case OperationAssertZeroDBConnections:
		return validPSQLArguments(
			values,
			"SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity AS a "+
				"WHERE a.datname = pg_catalog.current_database() "+
				"AND a.pid <> pg_catalog.pg_backend_pid();",
		)
	case OperationRestoreApply:
		return validRestoreApplyArguments(values)
	case OperationPostRestoreVerify:
		return validPSQLArguments(values, "SELECT pg_catalog.current_database();")
	default:
		return false
	}
}

func validDumpArguments(arguments []string) bool {
	if len(arguments) != 10 || !equalArgumentValues(arguments[:8], []string{
		"pg_dump",
		"--format=custom",
		"--compress=0",
		"--no-owner",
		"--no-acl",
		"--host=/var/run/postgresql",
		"--username=postgres",
		"--no-password",
	}) {
		return false
	}
	return validGeneratedArgument(arguments[8], "--role=") &&
		validGeneratedArgument(arguments[9], "--dbname=")
}

func validRestoreApplyArguments(arguments []string) bool {
	if len(arguments) != 12 || !equalArgumentValues(arguments[:10], []string{
		"pg_restore",
		"--clean",
		"--if-exists",
		"--no-owner",
		"--no-acl",
		"--exit-on-error",
		"--single-transaction",
		"--host=/var/run/postgresql",
		"--username=postgres",
		"--no-password",
	}) {
		return false
	}
	return validGeneratedArgument(arguments[10], "--role=") &&
		validGeneratedArgument(arguments[11], "--dbname=")
}

func validPSQLArguments(arguments []string, sql string) bool {
	if len(arguments) != 11 || !equalArgumentValues(arguments[:9], []string{
		"psql",
		"--no-psqlrc",
		"--quiet",
		"--tuples-only",
		"--no-align",
		"--set=ON_ERROR_STOP=1",
		"--host=/var/run/postgresql",
		"--username=postgres",
		"--no-password",
	}) {
		return false
	}
	return validGeneratedArgument(arguments[9], "--dbname=") && arguments[10] == "--command="+sql
}

func validGeneratedArgument(value, prefix string) bool {
	return strings.HasPrefix(value, prefix) && postgresidentity.ValidGenerated(strings.TrimPrefix(value, prefix))
}

func equalArgumentValues(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validEnvironment(environment [][]byte) bool {
	expected := Environment()
	if len(environment) != len(expected) {
		return false
	}
	for index := range expected {
		if len(environment[index]) == 0 || len(environment[index]) > 256 ||
			!bytes.Equal(environment[index], []byte(expected[index])) {
			return false
		}
	}
	return true
}

func invalidConfinement(message string) error {
	return grounderrs.New(grounderrs.KindValidationFailed, message)
}
