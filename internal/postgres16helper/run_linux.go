package postgres16helper

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

// Run executes a single closed PostgreSQL operation under the injected release
// authority. The command entry remains closed until release packaging supplies
// this authority; an unauthenticated Docker Exec cannot construct Runtime.
func (runtime Runtime) Run(ctx context.Context, arguments []string) (
	postgres16protocol.ExitCode, error,
) {
	operation, code, err := runtime.Prepare(ctx, arguments)
	if err != nil {
		return code, err
	}
	defer operation.Close()
	if operation.Request.Operation == postgres16protocol.OperationStop {
		return runtime.stop(ctx, operation)
	}
	if operation.Request.Operation == postgres16protocol.OperationRecoveryInventory {
		return runtime.recoveryInventory(ctx, operation)
	}
	if operation.Request.Operation == postgres16protocol.OperationEvidence ||
		operation.Request.Operation == postgres16protocol.OperationRetire {
		return runtime.executionEvidence(ctx, operation)
	}
	if err := runtime.inspectRecovery(ctx, operation.stateDir); err != nil {
		return postgres16protocol.ExitRecoveryRequired, err
	}
	deadline := time.Unix(0, int64(operation.Request.DeadlineUnixNano))
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	input, stdinEvidence, err := prepareClientInput(ctx, operation)
	if err != nil {
		return postgres16protocol.ExitInputIntegrity, err
	}
	defer input.Close()
	launch, parent, err := runtime.launchIntent(operation)
	if err != nil {
		return postgres16protocol.ExitCallerIdentity, err
	}
	encodedIntent, intentSHA256, err := marshalGateLaunch(launch)
	if err != nil || len(encodedIntent) == 0 {
		return postgres16protocol.ExitInternalFailure, supervisorError()
	}
	capsule, err := encodeGateCapsule(launch, intentSHA256)
	if err != nil {
		return postgres16protocol.ExitInternalFailure, err
	}
	journal, err := OpenStateJournal(operation.stateDir, operation.Request.Nonce)
	if err != nil {
		return postgres16protocol.ExitRecoveryRequired, err
	}
	defer journal.Close()
	if _, exists := journal.Current(); exists {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	state := postgres16protocol.ConfinementStateShape{
		Sequence: 1, Phase: postgres16protocol.ConfinementPhaseCreated,
		ProgressPhase:      postgres16protocol.ConfinementPhaseCreated,
		LaunchIntentSHA256: intentSHA256,
	}
	if err := journal.Append(state); err != nil {
		return postgres16protocol.ExitRecoveryRequired, err
	}
	state, _ = journal.Current()
	gate, err := startGate(operation, capsule, input)
	if err != nil {
		return postgres16protocol.ExitRecoveryRequired, err
	}
	defer gate.Close()
	defer gate.process.Release()
	if gate.pidfd < 0 {
		_ = gate.release.Close()
		_, _ = waitGate(gate.process.Pid)
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	if err := storeProcessRecord(operation.stateDir, operation.Request.Nonce,
		intentSHA256, gate.process.Pid); err != nil {
		return runtime.abortGate(gate, err)
	}
	if err := gate.status.SetReadDeadline(deadline); err != nil {
		return runtime.abortGate(gate, supervisorError())
	}
	ready, readySHA256, err := readGateStatus(gate.status, gate.process.Pid, launch, intentSHA256, parent)
	if err != nil {
		return runtime.abortGate(gate, supervisorError())
	}
	if ready.Kind == postgres16protocol.ConfinementGateStatusFatal {
		return runtime.recordGateFatal(ctx, gate, journal, state, ready, readySHA256)
	}
	if ready.Kind != postgres16protocol.ConfinementGateStatusReady {
		return runtime.abortGate(gate, supervisorError())
	}
	state = advanceState(state, postgres16protocol.ConfinementPhaseGateDurable)
	state.GateReady = true
	state.ReadyFrameSHA256 = readySHA256
	if err := journal.Append(state); err != nil {
		return runtime.abortGate(gate, err)
	}
	state, _ = journal.Current()
	state = advanceState(state, postgres16protocol.ConfinementPhaseGateReleased)
	state.GateRelease = true
	if err := journal.Append(state); err != nil {
		return runtime.abortGate(gate, err)
	}
	state, _ = journal.Current()
	if _, err := gate.release.Write([]byte{0xa5}); err != nil {
		return runtime.abortGate(gate, err)
	}
	profile, profileSHA256, err := readGateStatus(gate.status, gate.process.Pid, launch, intentSHA256, parent)
	if err != nil {
		return runtime.abortGate(gate, supervisorError())
	}
	if profile.Kind == postgres16protocol.ConfinementGateStatusFatal {
		return runtime.recordGateFatal(ctx, gate, journal, state, profile, profileSHA256)
	}
	if profile.Kind != postgres16protocol.ConfinementGateStatusProfileApplied {
		return runtime.abortGate(gate, supervisorError())
	}
	state = advanceState(state, postgres16protocol.ConfinementPhaseProfileApplied)
	state.GateProfile = true
	state.ProfileFrameSHA256 = profileSHA256
	if err := journal.Append(state); err != nil {
		return runtime.abortGate(gate, err)
	}
	state, _ = journal.Current()
	processRecord, err := loadProcessRecord(operation.stateDir, operation.Request.Nonce)
	if err != nil || processRecord.IntentSHA256 != intentSHA256 || processRecord.PID != uint32(gate.process.Pid) {
		return runtime.abortGate(gate, supervisorError())
	}
	if err := storeExecIntent(operation.stateDir, operation.Request, state, processRecord); err != nil {
		return runtime.abortGate(gate, err)
	}
	stdoutDone := make(chan streamResult, 1)
	stderrDone := make(chan streamResult, 1)
	outputLimit := operation.StreamPolicy.OutputLimit
	if outputLimit == 0 {
		outputLimit = postgres16protocol.MaximumRestoreSourceBytes
	}
	var outputTarget io.Writer
	if operation.StreamPolicy.Output == postgres16protocol.OutputArtifact {
		outputTarget = os.Stdout
	}
	go func() {
		stdoutDone <- copyClientOutput(gate.stdout, outputLimit, outputTarget,
			operation.StreamPolicy.Output == postgres16protocol.OutputProof)
	}()
	go func() {
		stderrDone <- copyClientOutput(gate.stderr, operation.StreamPolicy.StderrLimit, nil, false)
	}()
	if _, err := gate.release.Write([]byte{0x5a}); err != nil {
		return runtime.abortGate(gate, err)
	}
	var trailing [1]byte
	n, err := gate.status.Read(trailing[:])
	if n == 1 && err == nil {
		status, frameSHA256, readErr := readGateStatus(
			io.MultiReader(bytes.NewReader(trailing[:]), gate.status),
			gate.process.Pid, launch, intentSHA256, parent,
		)
		if readErr == nil && status.Kind == postgres16protocol.ConfinementGateStatusFatal {
			return runtime.recordGateFatal(ctx, gate, journal, state, status, frameSHA256)
		}
		return runtime.abortGate(gate, supervisorError())
	}
	if n != 0 || !errors.Is(err, io.EOF) {
		return runtime.abortGate(gate, supervisorError())
	}
	state = advanceState(state, postgres16protocol.ConfinementPhaseChildDurable)
	state.Child = true
	state.ExecFD4EOF = true
	state.ExecEvidence = 1
	if err := journal.Append(state); err != nil {
		return runtime.abortGate(gate, err)
	}
	state, _ = journal.Current()
	var stdout, stderr streamResult
	for completed := 0; completed < 2; completed++ {
		select {
		case stdout = <-stdoutDone:
			if stdout.err != nil {
				return runtime.abortGate(gate, stdout.err)
			}
		case stderr = <-stderrDone:
			if stderr.err != nil {
				return runtime.abortGate(gate, stderr.err)
			}
		case <-ctx.Done():
			return runtime.abortGate(gate, ctx.Err())
		}
	}
	state = advanceState(state, postgres16protocol.ConfinementPhaseIOComplete)
	state.IO = true
	state.IOEvidence = &postgres16protocol.ConfinementIOEvidence{
		Stdin: stdinEvidence, Stdout: stdout.evidence, Stderr: stderr.evidence,
	}
	if err := journal.Append(state); err != nil {
		return runtime.abortGate(gate, err)
	}
	state, _ = journal.Current()
	raw, err := observeExit(ctx, gate.pidfd, gate.process.Pid)
	if err != nil {
		return runtime.abortGate(gate, err)
	}
	state, err = recordTerminal(journal, state, raw)
	if err != nil {
		return postgres16protocol.ExitRecoveryRequired, err
	}
	reaped, err := waitGate(gate.process.Pid)
	if err != nil || reaped != raw {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	state.Sequence++
	state.Phase = postgres16protocol.ConfinementPhaseReaped
	state.Terminal.Wait4Reaped = true
	state.Terminal.RawWaitStatus = uint32(reaped)
	state.Terminal.EvidenceSHA256, err = state.TerminalEvidenceSHA256()
	if err != nil || journal.Append(state) != nil {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	if !raw.Exited() || raw.ExitStatus() != 0 {
		return postgres16protocol.ExitChildFailed, supervisorError()
	}
	if !validOperationProof(operation.Request, stdout.proof) {
		return postgres16protocol.ExitProofMismatch, supervisorError()
	}
	if operation.StreamPolicy.Output == postgres16protocol.OutputProof {
		if err := writeAll(os.Stdout, stdout.proof); err != nil {
			return postgres16protocol.ExitIOFailed, err
		}
	}
	return postgres16protocol.ExitSuccess, nil
}

func advanceState(
	previous postgres16protocol.ConfinementStateShape,
	phase postgres16protocol.ConfinementStatePhase,
) postgres16protocol.ConfinementStateShape {
	previous.Sequence++
	previous.Phase = phase
	previous.ProgressPhase = phase
	return previous
}

func (runtime Runtime) abortGate(gate *gateProcess, cause error) (
	postgres16protocol.ExitCode, error,
) {
	if gate.pidfd >= 0 {
		_ = unix.PidfdSendSignal(gate.pidfd, unix.SIGKILL, nil, 0)
	}
	_ = gate.release.Close()
	_, _ = waitGate(gate.process.Pid)
	return postgres16protocol.ExitRecoveryRequired, cause
}

func validOperationProof(request postgres16protocol.Request, output []byte) bool {
	value := strings.TrimSpace(string(output))
	switch request.Operation {
	case postgres16protocol.OperationProbePGDump:
		return strings.HasPrefix(value, "pg_dump (PostgreSQL) 16")
	case postgres16protocol.OperationProbePGRestore:
		return strings.HasPrefix(value, "pg_restore (PostgreSQL) 16")
	case postgres16protocol.OperationProbePSQL:
		return strings.HasPrefix(value, "psql (PostgreSQL) 16")
	case postgres16protocol.OperationServerMajor:
		return value == "16"
	case postgres16protocol.OperationTerminateDBConnections:
		return value == "t"
	case postgres16protocol.OperationAssertZeroDBConnections:
		return value == "0"
	case postgres16protocol.OperationPostRestoreVerify:
		return value == request.Database
	case postgres16protocol.OperationDump, postgres16protocol.OperationRestoreList,
		postgres16protocol.OperationRestoreApply:
		return len(output) == 0
	default:
		return false
	}
}
