package postgres16helper

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
)

func (runtime Runtime) recordGateFatal(
	ctx context.Context, gate *gateProcess, journal *StateJournal,
	state postgres16protocol.ConfinementStateShape,
	status postgres16protocol.ConfinementGateStatus,
	frameSHA256 postgres16protocol.Digest,
) (postgres16protocol.ExitCode, error) {
	if status.Kind != postgres16protocol.ConfinementGateStatusFatal {
		return runtime.abortGate(gate, supervisorError())
	}
	var trailing [1]byte
	n, err := gate.status.Read(trailing[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		return runtime.abortGate(gate, supervisorError())
	}
	raw, err := observeExit(ctx, gate.pidfd, gate.process.Pid)
	if err != nil {
		return runtime.abortGate(gate, err)
	}
	state.Sequence++
	state.Phase = postgres16protocol.ConfinementPhaseTerminal
	state.GateFatal = &postgres16protocol.ConfinementFatalEvidence{
		Stage: status.FatalStage, Errno: status.FatalErrno, Sequence: status.Sequence,
		FD4EOF: true, FrameSHA256: frameSHA256,
	}
	terminal := &postgres16protocol.ConfinementTerminal{Kind: postgres16protocol.ConfinementTerminalGateFatal}
	if raw.Exited() {
		terminal.ExitCode = uint32(raw.ExitStatus())
	} else if raw.Signaled() {
		terminal.Signal = uint32(raw.Signal())
		terminal.CoreDumped = raw.CoreDump()
	} else {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	state.Terminal = terminal
	terminal.EvidenceSHA256, err = state.TerminalEvidenceSHA256()
	if err != nil || journal.Append(state) != nil {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	state, _ = journal.Current()
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
	return postgres16protocol.ExitLaunchFailed, supervisorError()
}
