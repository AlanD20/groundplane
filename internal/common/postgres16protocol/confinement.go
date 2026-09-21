package postgres16protocol

import (
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

func invalidConfinement(message string) error {
	return grounderrs.New(grounderrs.KindValidationFailed, message)
}
