package postgres16helper

import (
	"context"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	grounderrs "github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Runtime accepts release measurements only from the verified launcher that
// constructed it. In particular, a Docker Exec argument or environment value
// cannot choose the trusted helper, gate, client, or process profile.
type Runtime struct {
	authority postgres16protocol.ConfinementReleaseAuthority
}

func NewRuntime(authority postgres16protocol.ConfinementReleaseAuthority) (Runtime, error) {
	if err := authority.Validate(); err != nil {
		return Runtime{}, err
	}
	expectedSeccomp, err := GateSeccompSHA256()
	if err != nil || expectedSeccomp != authority.GateSeccompSHA256 ||
		authority.LaunchProfileSHA256 != postgres16protocol.ManagedLaunchProfileSHA256() {
		return Runtime{}, supervisorError()
	}
	return Runtime{authority: authority}, nil
}

type PreparedOperation struct {
	Request      postgres16protocol.Request
	Arguments    [][]byte
	Environment  [][]byte
	Profile      postgres16protocol.FDProfile
	StreamPolicy postgres16protocol.StreamPolicy
	Helper       postgres16protocol.ConfinementFileIdentity
	Gate         postgres16protocol.ConfinementFileIdentity
	Client       postgres16protocol.ConfinementFileIdentity
	stateDir     *os.File
	helperFile   *os.File
	gateFile     *os.File
	clientFile   *os.File
}

func (operation *PreparedOperation) Close() error {
	var first error
	for _, file := range []*os.File{operation.clientFile, operation.gateFile, operation.helperFile, operation.stateDir} {
		if file != nil {
			if err := file.Close(); first == nil {
				first = err
			}
		}
	}
	operation.clientFile = nil
	operation.gateFile = nil
	operation.helperFile = nil
	operation.stateDir = nil
	return first
}

func (runtime Runtime) Prepare(
	ctx context.Context, arguments []string,
) (_ *PreparedOperation, code postgres16protocol.ExitCode, failure error) {
	if !postgres16protocol.ValidEnvironment(os.Environ()) {
		return nil, postgres16protocol.ExitEnvironmentInvalid, supervisorError()
	}
	if os.Getuid() != 0 || os.Geteuid() != 0 || os.Getgid() != 0 || os.Getegid() != 0 {
		return nil, postgres16protocol.ExitCallerIdentity, supervisorError()
	}
	groups, err := os.Getgroups()
	if err != nil {
		return nil, postgres16protocol.ExitCallerIdentity, supervisorError()
	}
	// Docker Exec may repeat the primary root group as a supplementary group.
	// Reject foreign groups, then establish the helper's empty group set.
	for _, group := range groups {
		if group != 0 {
			return nil, postgres16protocol.ExitCallerIdentity, supervisorError()
		}
	}
	if len(groups) != 0 {
		if err := syscall.Setgroups(nil); err != nil {
			return nil, postgres16protocol.ExitCallerIdentity, supervisorError()
		}
	}
	request, err := postgres16protocol.ParseArguments(arguments)
	if err != nil {
		return nil, postgres16protocol.ExitRequestInvalid, err
	}
	if request.DeadlineUnixNano > uint64(^uint64(0)>>1) ||
		!time.Now().Before(time.Unix(0, int64(request.DeadlineUnixNano))) || ctx.Err() != nil {
		return nil, postgres16protocol.ExitDeadlineExceeded, supervisorError()
	}
	stateDir, err := openStateDirectory()
	if err != nil {
		return nil, postgres16protocol.ExitEnvironmentInvalid, err
	}
	operation := &PreparedOperation{Request: request, stateDir: stateDir}
	defer func() {
		if failure != nil {
			_ = operation.Close()
		}
	}()
	if request.Operation == postgres16protocol.OperationStop {
		return operation, postgres16protocol.ExitSuccess, nil
	}
	// Stop must be able to signal a live child; every other operation owns the
	// private namespace exclusively, including evidence retirement.
	if err := unix.Flock(int(stateDir.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	if request.Operation == postgres16protocol.OperationEvidence ||
		request.Operation == postgres16protocol.OperationRetire ||
		request.Operation == postgres16protocol.OperationRecoveryInventory {
		return operation, postgres16protocol.ExitSuccess, nil
	}
	operation.Arguments, err = clientArguments(request)
	if err != nil {
		return nil, postgres16protocol.ExitRequestInvalid, err
	}
	for _, entry := range postgres16protocol.Environment() {
		operation.Environment = append(operation.Environment, []byte(entry))
	}
	operation.Profile, err = postgres16protocol.ProfileFor(request.Operation)
	if err != nil {
		return nil, postgres16protocol.ExitRequestInvalid, err
	}
	operation.StreamPolicy, err = postgres16protocol.PolicyFor(request.Operation)
	if err != nil {
		return nil, postgres16protocol.ExitRequestInvalid, err
	}
	operation.helperFile, operation.Helper, err = inspectReleaseFile(postgres16protocol.HelperPath)
	if err != nil || operation.Helper.SHA256 != runtime.authority.HelperSHA256 ||
		operation.Helper.Mode != postgres16protocol.HelperMode {
		return nil, postgres16protocol.ExitEnvironmentInvalid, supervisorError()
	}
	operation.gateFile, operation.Gate, err = inspectReleaseFile(postgres16protocol.ClientGatePath)
	if err != nil || operation.Gate.SHA256 != runtime.authority.GateSHA256 ||
		operation.Gate.Mode != postgres16protocol.ClientGateMode {
		return nil, postgres16protocol.ExitEnvironmentInvalid, supervisorError()
	}
	path, err := postgres16protocol.ClientPath(request.Operation)
	if err != nil {
		return nil, postgres16protocol.ExitRequestInvalid, err
	}
	operation.clientFile, operation.Client, err = inspectReleaseFile(path)
	if err != nil || operation.Client.SHA256 != runtime.clientDigest(request.Operation) ||
		operation.Client.Mode&0o6022 != 0 {
		return nil, postgres16protocol.ExitEnvironmentInvalid, supervisorError()
	}
	return operation, postgres16protocol.ExitSuccess, nil
}

func (runtime Runtime) clientDigest(operation postgres16protocol.Operation) postgres16protocol.Digest {
	switch operation {
	case postgres16protocol.OperationProbePGDump, postgres16protocol.OperationDump:
		return runtime.authority.PGDumpSHA256
	case postgres16protocol.OperationProbePGRestore, postgres16protocol.OperationRestoreList,
		postgres16protocol.OperationRestoreApply:
		return runtime.authority.PGRestoreSHA256
	default:
		return runtime.authority.PSQLSHA256
	}
}

func openStateDirectory() (*os.File, error) {
	rootFD, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, supervisorError()
	}
	defer unix.Close(rootFD)
	fd, err := unix.Openat2(rootFD,
		strings.TrimPrefix(postgres16protocol.StateDirectoryPath, "/"), &unix.OpenHow{
			Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
			Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
		})
	if err != nil {
		return nil, supervisorError()
	}
	var metadata unix.Stat_t
	if err := unix.Fstat(fd, &metadata); err != nil || metadata.Mode&unix.S_IFMT != unix.S_IFDIR ||
		metadata.Uid != postgres16protocol.StateDirectoryUID ||
		metadata.Gid != postgres16protocol.StateDirectoryGID ||
		metadata.Mode&0o7777 != postgres16protocol.StateDirectoryMode {
		_ = unix.Close(fd)
		return nil, supervisorError()
	}
	return os.NewFile(uintptr(fd), postgres16protocol.StateDirectoryPath), nil
}

func supervisorError() error {
	return grounderrs.New(grounderrs.KindInternal, "postgres helper supervisor proof is unavailable")
}
