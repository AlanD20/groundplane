// Package postgres16protocol owns the side-effect-free public contract shared
// by the managed PostgreSQL 16 helper and its future Docker caller.
package postgres16protocol

import (
	"encoding/hex"
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/postgresidentity"
	grounderrs "github.com/AlanD20/groundplane/pkg/errs"
)

const (
	ProtocolVersion        uint32 = 1
	AdapterContractVersion uint32 = 1
	PostgreSQLMajor        uint32 = 16

	PlatformOS           = "linux"
	PlatformArchitecture = "arm64"
	PlatformVariant      = "v8"

	HelperDirectoryPath = "/usr/local/libexec"
	HelperPath          = "/usr/local/libexec/groundplane-postgres16-helper"
	ClientGatePath      = "/usr/local/libexec/groundplane-postgres16-client-gate"
	StateDirectoryPath  = "/run/groundplane-postgres16"

	PGDumpPath    = "/usr/local/bin/pg_dump"
	PGRestorePath = "/usr/local/bin/pg_restore"
	PSQLPath      = "/usr/local/bin/psql"

	HelperDirectoryUID  uint32 = 0
	HelperDirectoryGID  uint32 = 0
	HelperDirectoryMode uint32 = 0o755
	HelperUID           uint32 = 0
	HelperGID           uint32 = 0
	HelperMode          uint32 = 0o555
	ClientGateUID       uint32 = 0
	ClientGateGID       uint32 = 0
	ClientGateMode      uint32 = 0o500
	StateDirectoryUID   uint32 = 0
	StateDirectoryGID   uint32 = 0
	StateDirectoryMode  uint32 = 0o700
	StateFileUID        uint32 = 0
	StateFileGID        uint32 = 0
	StateFileMode       uint32 = 0o600
	PostgreSQLUID       uint32 = 70
	PostgreSQLGID       uint32 = 70

	PathValue = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

	ProbeStdoutLimitBytes     uint64 = 32 * 1024
	ProbeStderrLimitBytes     uint64 = 32 * 1024
	DiagnosticLimitBytes      uint64 = 32 * 1024
	MaximumRestoreSourceBytes uint64 = 5_497_558_138_880
	ClientNoFileLimit         uint32 = 64
)

const protocolVersionArgument = "1"

var exactEnvironment = [...]string{
	"PATH=" + PathValue,
	"HOME=/nonexistent",
	"LC_ALL=C",
	"TZ=UTC",
}

var dockerExecPrefix = [...]string{
	"/usr/bin/env",
	"-i",
	"PATH=" + PathValue,
	"HOME=/nonexistent",
	"LC_ALL=C",
	"TZ=UTC",
	HelperPath,
}

type Digest [32]byte

type Nonce [32]byte

type ExitCode uint8

const (
	ExitSuccess             ExitCode = 0
	ExitRequestInvalid      ExitCode = 1
	ExitEnvironmentInvalid  ExitCode = 2
	ExitRecoveryRequired    ExitCode = 3
	ExitLaunchFailed        ExitCode = 4
	ExitDeadlineExceeded    ExitCode = 5
	ExitChildFailed         ExitCode = 6
	ExitCallerIdentity      ExitCode = 7
	ExitIOFailed            ExitCode = 8
	ExitInputIntegrity      ExitCode = 9
	ExitStreamLimitExceeded ExitCode = 10
	ExitProofMismatch       ExitCode = 11
	ExitInternalFailure     ExitCode = 12
)

type Operation uint8

const (
	OperationProbePGDump             Operation = 1
	OperationProbePGRestore          Operation = 2
	OperationProbePSQL               Operation = 3
	OperationServerMajor             Operation = 4
	OperationDump                    Operation = 5
	OperationRestoreList             Operation = 6
	OperationTerminateDBConnections  Operation = 7
	OperationAssertZeroDBConnections Operation = 8
	OperationRestoreApply            Operation = 9
	OperationPostRestoreVerify       Operation = 10
	OperationStop                    Operation = 11
)

type Request struct {
	Operation        Operation
	Nonce            Nonce
	DeadlineUnixNano uint64
	Database         string
	Role             string
	SourceSize       uint64
	SourceSHA256     Digest
}

type InputMode uint8

const (
	InputNone          InputMode = 1
	InputRestoreSource InputMode = 2
)

type OutputMode uint8

const (
	OutputProof        OutputMode = 1
	OutputArtifact     OutputMode = 2
	OutputDiscardCount OutputMode = 3
)

type StreamPolicy struct {
	Input       InputMode
	Output      OutputMode
	OutputLimit uint64
	StderrLimit uint64
}

type FDTarget uint8

const (
	FDReadOnlyDevNull       FDTarget = 1
	FDValidatedRestoreInput FDTarget = 2
	FDValidatedResult       FDTarget = 3
	FDArtifactOutput        FDTarget = 4
	FDBoundedDiagnostic     FDTarget = 5
	FDDiscardCount          FDTarget = 6
)

type FDProfile struct {
	Version uint32
	Stdin   FDTarget
	Stdout  FDTarget
	Stderr  FDTarget
}

func (code ExitCode) Valid() bool {
	return code <= ExitInternalFailure
}

func ClassifyProcessExit(normalExit bool, status uint32) (ExitCode, error) {
	if !normalExit {
		return ExitRecoveryRequired, nil
	}
	if status > uint32(ExitInternalFailure) {
		return 0, invalid("postgres helper process exit status is reserved")
	}
	code := ExitCode(status)
	if !code.Valid() {
		return 0, invalid("postgres helper process exit status is invalid")
	}
	return code, nil
}

func Environment() []string {
	return slices.Clone(exactEnvironment[:])
}

func DockerExecPrefix() []string {
	return slices.Clone(dockerExecPrefix[:])
}

func ValidEnvironment(environment []string) bool {
	return slices.Equal(environment, exactEnvironment[:])
}

func ParseNonce(value string) (Nonce, error) {
	var nonce Nonce
	decoded, err := parseLowerHex(value, len(nonce))
	if err != nil {
		return Nonce{}, err
	}
	copy(nonce[:], decoded)
	if nonce == (Nonce{}) {
		return Nonce{}, invalid("postgres helper nonce is zero")
	}
	return nonce, nil
}

func ParseDigest(value string) (Digest, error) {
	var digest Digest
	decoded, err := parseLowerHex(value, len(digest))
	if err != nil {
		return Digest{}, err
	}
	copy(digest[:], decoded)
	return digest, nil
}

func (nonce Nonce) String() string {
	return hex.EncodeToString(nonce[:])
}

func (digest Digest) String() string {
	return hex.EncodeToString(digest[:])
}

func ParseArguments(arguments []string) (Request, error) {
	if len(arguments) == 0 {
		return Request{}, invalid("postgres helper operation is missing")
	}
	switch arguments[0] {
	case "run":
		return parseRunArguments(arguments)
	case "stop":
		return parseStopArguments(arguments)
	default:
		return Request{}, invalid("postgres helper operation is invalid")
	}
}

func ParseDockerExecCommand(command []string) (Request, error) {
	if len(command) < len(dockerExecPrefix) || !slices.Equal(command[:len(dockerExecPrefix)], dockerExecPrefix[:]) {
		return Request{}, invalid("postgres helper Docker Exec prefix is invalid")
	}
	return ParseArguments(command[len(dockerExecPrefix):])
}

func (request Request) Arguments() ([]string, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	deadline := strconv.FormatUint(request.DeadlineUnixNano, 10)
	if request.Operation == OperationStop {
		return []string{"stop", protocolVersionArgument, request.Nonce.String(), deadline}, nil
	}
	arguments := []string{
		"run",
		protocolVersionArgument,
		request.Nonce.String(),
		deadline,
		request.Operation.String(),
	}
	switch request.Operation {
	case OperationProbePGDump, OperationProbePGRestore, OperationProbePSQL:
		arguments = append(arguments, strconv.FormatUint(uint64(PostgreSQLMajor), 10))
	case OperationServerMajor:
		arguments = append(arguments, strconv.FormatUint(uint64(PostgreSQLMajor), 10), request.Database)
	case OperationDump:
		arguments = append(arguments, request.Database, request.Role)
	case OperationRestoreList:
		arguments = append(arguments, strconv.FormatUint(request.SourceSize, 10), request.SourceSHA256.String())
	case OperationTerminateDBConnections, OperationAssertZeroDBConnections, OperationPostRestoreVerify:
		arguments = append(arguments, request.Database)
	case OperationRestoreApply:
		arguments = append(
			arguments,
			strconv.FormatUint(request.SourceSize, 10),
			request.SourceSHA256.String(),
			request.Database,
			request.Role,
		)
	default:
		return nil, invalid("postgres helper run operation is invalid")
	}
	return arguments, nil
}

func (request Request) DockerExecCommand() ([]string, error) {
	arguments, err := request.Arguments()
	if err != nil {
		return nil, err
	}
	command := make([]string, 0, len(dockerExecPrefix)+len(arguments))
	command = append(command, dockerExecPrefix[:]...)
	return append(command, arguments...), nil
}

func (request Request) Validate() error {
	if request.Nonce == (Nonce{}) || request.DeadlineUnixNano == 0 {
		return invalid("postgres helper request identity is invalid")
	}
	if request.Operation == OperationStop {
		if request.Database != "" || request.Role != "" || request.SourceSize != 0 ||
			request.SourceSHA256 != (Digest{}) {
			return invalid("postgres helper stop request has inapplicable fields")
		}
		return nil
	}
	if !request.Operation.ValidRun() {
		return invalid("postgres helper run operation is invalid")
	}
	needsDatabase := request.Operation == OperationServerMajor || request.Operation == OperationDump ||
		request.Operation == OperationTerminateDBConnections ||
		request.Operation == OperationAssertZeroDBConnections ||
		request.Operation == OperationRestoreApply || request.Operation == OperationPostRestoreVerify
	needsRole := request.Operation == OperationDump || request.Operation == OperationRestoreApply
	needsSource := request.Operation == OperationRestoreList || request.Operation == OperationRestoreApply
	if needsDatabase != (request.Database != "") ||
		needsDatabase && !postgresidentity.ValidGenerated(request.Database) {
		return invalid("postgres helper database is invalid")
	}
	if needsRole != (request.Role != "") || needsRole && !postgresidentity.ValidGenerated(request.Role) {
		return invalid("postgres helper role is invalid")
	}
	if needsSource {
		if request.SourceSize > MaximumRestoreSourceBytes {
			return invalid("postgres helper source size is invalid")
		}
	} else if request.SourceSize != 0 || request.SourceSHA256 != (Digest{}) {
		return invalid("postgres helper request has inapplicable source fields")
	}
	return nil
}

func (operation Operation) ValidRun() bool {
	return operation >= OperationProbePGDump && operation <= OperationPostRestoreVerify
}

func (operation Operation) String() string {
	switch operation {
	case OperationProbePGDump:
		return "probe-pg-dump"
	case OperationProbePGRestore:
		return "probe-pg-restore"
	case OperationProbePSQL:
		return "probe-psql"
	case OperationServerMajor:
		return "server-major"
	case OperationDump:
		return "dump"
	case OperationRestoreList:
		return "restore-list"
	case OperationTerminateDBConnections:
		return "terminate-db-connections"
	case OperationAssertZeroDBConnections:
		return "assert-zero-db-connections"
	case OperationRestoreApply:
		return "restore-apply"
	case OperationPostRestoreVerify:
		return "post-restore-verify"
	case OperationStop:
		return "stop"
	default:
		return ""
	}
}

func ClientPath(operation Operation) (string, error) {
	switch operation {
	case OperationProbePGDump, OperationDump:
		return PGDumpPath, nil
	case OperationProbePGRestore, OperationRestoreList, OperationRestoreApply:
		return PGRestorePath, nil
	case OperationProbePSQL, OperationServerMajor, OperationTerminateDBConnections,
		OperationAssertZeroDBConnections, OperationPostRestoreVerify:
		return PSQLPath, nil
	default:
		return "", invalid("postgres helper operation has no client path")
	}
}

func ProfileFor(operation Operation) (FDProfile, error) {
	profile := FDProfile{Version: 1, Stderr: FDBoundedDiagnostic}
	switch operation {
	case OperationProbePGDump, OperationProbePGRestore, OperationProbePSQL:
		profile.Stdin = FDReadOnlyDevNull
		profile.Stdout = FDValidatedResult
	case OperationServerMajor, OperationTerminateDBConnections, OperationAssertZeroDBConnections,
		OperationPostRestoreVerify:
		profile.Stdin = FDReadOnlyDevNull
		profile.Stdout = FDValidatedResult
	case OperationDump:
		profile.Stdin = FDReadOnlyDevNull
		profile.Stdout = FDArtifactOutput
	case OperationRestoreList, OperationRestoreApply:
		profile.Stdin = FDValidatedRestoreInput
		profile.Stdout = FDDiscardCount
	default:
		return FDProfile{}, invalid("postgres helper operation has no fd profile")
	}
	return profile, nil
}

func PolicyFor(operation Operation) (StreamPolicy, error) {
	policy := StreamPolicy{Input: InputNone, Output: OutputProof, StderrLimit: DiagnosticLimitBytes}
	switch operation {
	case OperationProbePGDump, OperationProbePGRestore, OperationProbePSQL:
		policy.OutputLimit = ProbeStdoutLimitBytes
		policy.StderrLimit = ProbeStderrLimitBytes
	case OperationServerMajor,
		OperationTerminateDBConnections, OperationAssertZeroDBConnections, OperationPostRestoreVerify:
		policy.OutputLimit = DiagnosticLimitBytes
	case OperationDump:
		policy.Output = OutputArtifact
	case OperationRestoreList:
		policy.Input = InputRestoreSource
		policy.Output = OutputDiscardCount
	case OperationRestoreApply:
		policy.Input = InputRestoreSource
		policy.Output = OutputDiscardCount
		policy.OutputLimit = DiagnosticLimitBytes
	default:
		return StreamPolicy{}, invalid("postgres helper operation has no stream policy")
	}
	return policy, nil
}

func parseRunArguments(arguments []string) (Request, error) {
	if len(arguments) < 5 || arguments[1] != protocolVersionArgument {
		return Request{}, invalid("postgres helper run header is invalid")
	}
	nonce, err := ParseNonce(arguments[2])
	if err != nil {
		return Request{}, err
	}
	deadline, err := parsePositiveUint(arguments[3])
	if err != nil {
		return Request{}, err
	}
	operation, err := parseRunOperation(arguments[4])
	if err != nil {
		return Request{}, err
	}
	request := Request{Operation: operation, Nonce: nonce, DeadlineUnixNano: deadline}
	suffix := arguments[5:]
	switch operation {
	case OperationProbePGDump, OperationProbePGRestore, OperationProbePSQL:
		if len(suffix) != 1 || suffix[0] != strconv.FormatUint(uint64(PostgreSQLMajor), 10) {
			return Request{}, invalid("postgres helper probe version is invalid")
		}
	case OperationServerMajor:
		if len(suffix) != 2 || suffix[0] != strconv.FormatUint(uint64(PostgreSQLMajor), 10) {
			return Request{}, invalid("postgres helper server-major suffix is invalid")
		}
		request.Database = suffix[1]
	case OperationDump:
		if len(suffix) != 2 {
			return Request{}, invalid("postgres helper dump suffix is invalid")
		}
		request.Database, request.Role = suffix[0], suffix[1]
	case OperationRestoreList:
		if len(suffix) != 2 {
			return Request{}, invalid("postgres helper restore-list suffix is invalid")
		}
		request.SourceSize, err = parseCanonicalUint(suffix[0])
		if err == nil {
			request.SourceSHA256, err = ParseDigest(suffix[1])
		}
	case OperationTerminateDBConnections, OperationAssertZeroDBConnections, OperationPostRestoreVerify:
		if len(suffix) != 1 {
			return Request{}, invalid("postgres helper database suffix is invalid")
		}
		request.Database = suffix[0]
	case OperationRestoreApply:
		if len(suffix) != 4 {
			return Request{}, invalid("postgres helper restore-apply suffix is invalid")
		}
		request.SourceSize, err = parseCanonicalUint(suffix[0])
		if err == nil {
			request.SourceSHA256, err = ParseDigest(suffix[1])
		}
		request.Database, request.Role = suffix[2], suffix[3]
	default:
		return Request{}, invalid("postgres helper run operation is invalid")
	}
	if err != nil {
		return Request{}, err
	}
	if err := request.Validate(); err != nil {
		return Request{}, err
	}
	return request, nil
}

func parseStopArguments(arguments []string) (Request, error) {
	if len(arguments) != 4 || arguments[1] != protocolVersionArgument {
		return Request{}, invalid("postgres helper stop suffix is invalid")
	}
	nonce, err := ParseNonce(arguments[2])
	if err != nil {
		return Request{}, err
	}
	deadline, err := parsePositiveUint(arguments[3])
	if err != nil {
		return Request{}, err
	}
	request := Request{Operation: OperationStop, Nonce: nonce, DeadlineUnixNano: deadline}
	return request, request.Validate()
}

func parseRunOperation(value string) (Operation, error) {
	for operation := OperationProbePGDump; operation <= OperationPostRestoreVerify; operation++ {
		if operation.String() == value {
			return operation, nil
		}
	}
	return 0, invalid("postgres helper run operation is invalid")
}

func parsePositiveUint(value string) (uint64, error) {
	if value == "" || len(value) > 1 && value[0] == '0' {
		return 0, invalid("postgres helper integer is not canonical")
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 {
		return 0, invalid("postgres helper integer is invalid")
	}
	return parsed, nil
}

func parseCanonicalUint(value string) (uint64, error) {
	if value == "0" {
		return 0, nil
	}
	return parsePositiveUint(value)
}

func parseLowerHex(value string, size int) ([]byte, error) {
	if len(value) != size*2 {
		return nil, invalid("postgres helper hexadecimal value has invalid length")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || hex.EncodeToString(decoded) != value {
		return nil, invalid("postgres helper hexadecimal value is not canonical")
	}
	return decoded, nil
}

func invalid(message string) error {
	return grounderrs.New(grounderrs.KindValidationFailed, message)
}
