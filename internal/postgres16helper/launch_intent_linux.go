package postgres16helper

import (
	"crypto/sha256"
	"os"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

func (runtime Runtime) launchIntent(
	operation *PreparedOperation,
) (privateGateLaunch, postgres16protocol.ConfinementProcessIdentity, error) {
	parent, err := inspectProcess(os.Getpid(), operation.Helper)
	if err != nil {
		return privateGateLaunch{}, parent, err
	}
	if err := parent.ValidateRootSupervisor(runtime.authority); err != nil {
		return privateGateLaunch{}, parent, err
	}
	lastCapability, err := readLastCapability()
	if err != nil {
		return privateGateLaunch{}, parent, err
	}
	security := postgres16protocol.ConfinementSecurityProfile{
		RealUID:            postgres16protocol.PostgreSQLUID,
		EffectiveUID:       postgres16protocol.PostgreSQLUID,
		SavedUID:           postgres16protocol.PostgreSQLUID,
		FilesystemUID:      postgres16protocol.PostgreSQLUID,
		RealGID:            postgres16protocol.PostgreSQLGID,
		EffectiveGID:       postgres16protocol.PostgreSQLGID,
		SavedGID:           postgres16protocol.PostgreSQLGID,
		FilesystemGID:      postgres16protocol.PostgreSQLGID,
		LastCapability:     lastCapability,
		NoNewPrivileges:    true,
		SeccompSHA256:      runtime.authority.GateSeccompSHA256,
		ForkSyscallsDenied: true,
	}
	intent := postgres16protocol.ConfinementLaunchIntent{
		Schema: 1, Nonce: operation.Request.Nonce, Operation: operation.Request.Operation,
		DeadlineUnixNano: operation.Request.DeadlineUnixNano,
		Supervisor:       parent, GateFile: operation.Gate, ClientFile: operation.Client,
		Arguments: operation.Arguments, Environment: operation.Environment,
		FDProfile: operation.Profile, ChildSecurity: security,
		NoFileLimit:            postgres16protocol.ClientNoFileLimit,
		LaunchProfileSHA256:    runtime.authority.LaunchProfileSHA256,
		GateSeccompSHA256:      runtime.authority.GateSeccompSHA256,
		GateArgvSHA256:         digestNullTerminated([]string{postgres16protocol.ClientGatePath}),
		GateEnvironmentSHA256:  digestNullTerminated(postgres16protocol.Environment()),
		GateReadyFDSetSHA256:   fdSetDigest([]int{0, 1, 2, 3, 4, 5, 6}),
		GateProfileFDSetSHA256: fdSetDigest([]int{0, 1, 2, 4, 5, 6}),
	}
	launch := privateGateLaunch{Schema: 1, Authority: runtime.authority, Intent: intent}
	if err := intent.Validate(runtime.authority); err != nil {
		return privateGateLaunch{}, parent, err
	}
	return launch, parent, nil
}

func digestNullTerminated(values []string) postgres16protocol.Digest {
	hash := sha256.New()
	for _, value := range values {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	var result postgres16protocol.Digest
	copy(result[:], hash.Sum(nil))
	return result
}

type gateProcess struct {
	process *os.Process
	pidfd   int
	status  *os.File
	release *os.File
	stdout  *os.File
	stderr  *os.File
}

func (process *gateProcess) Close() {
	if process == nil {
		return
	}
	for _, file := range []*os.File{process.status, process.release, process.stdout, process.stderr} {
		if file != nil {
			_ = file.Close()
		}
	}
	if process.pidfd >= 0 {
		_ = unix.Close(process.pidfd)
		process.pidfd = -1
	}
}

func startGate(
	operation *PreparedOperation, capsule []byte, clientInput *os.File,
) (_ *gateProcess, failure error) {
	memfd, err := unix.MemfdCreate("postgres16-gate-intent", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, supervisorError()
	}
	intentFile := os.NewFile(uintptr(memfd), "postgres16-gate-intent")
	defer intentFile.Close()
	if err := writeAll(intentFile, capsule); err != nil {
		return nil, err
	}
	if _, err := intentFile.Seek(0, 0); err != nil {
		return nil, supervisorError()
	}
	seals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if _, err := unix.FcntlInt(uintptr(memfd), unix.F_ADD_SEALS, seals); err != nil {
		return nil, supervisorError()
	}
	statusRead, statusWrite, err := os.Pipe()
	if err != nil {
		return nil, supervisorError()
	}
	defer statusWrite.Close()
	defer func() {
		if failure != nil {
			_ = statusRead.Close()
		}
	}()
	releaseRead, releaseWrite, err := os.Pipe()
	if err != nil {
		return nil, supervisorError()
	}
	defer releaseRead.Close()
	defer func() {
		if failure != nil {
			_ = releaseWrite.Close()
		}
	}()
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		return nil, supervisorError()
	}
	defer stdoutWrite.Close()
	defer func() {
		if failure != nil {
			_ = stdoutRead.Close()
		}
	}()
	stderrRead, stderrWrite, err := os.Pipe()
	if err != nil {
		return nil, supervisorError()
	}
	defer stderrWrite.Close()
	defer func() {
		if failure != nil {
			_ = stderrRead.Close()
		}
	}()
	pidfd := -1
	process, err := os.StartProcess(postgres16protocol.ClientGatePath,
		[]string{postgres16protocol.ClientGatePath}, &os.ProcAttr{
			Env: postgres16protocol.Environment(),
			Files: []*os.File{
				clientInput, stdoutWrite, stderrWrite, intentFile,
				statusWrite, releaseRead, operation.clientFile,
			},
			Sys: &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL, PidFD: &pidfd},
		})
	if err != nil {
		if pidfd >= 0 {
			_ = unix.Close(pidfd)
		}
		return nil, supervisorError()
	}
	return &gateProcess{
		process: process, pidfd: pidfd, status: statusRead,
		release: releaseWrite, stdout: stdoutRead, stderr: stderrRead,
	}, nil
}
