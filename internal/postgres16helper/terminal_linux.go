package postgres16helper

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

func observeExit(ctx context.Context, pidfd, pid int) (unix.WaitStatus, error) {
	if pidfd < 0 || pid <= 0 {
		return 0, supervisorError()
	}
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
		count, err := unix.Poll(fds, 1000)
		if err == unix.EINTR || count == 0 {
			continue
		}
		if err != nil || fds[0].Revents&unix.POLLIN == 0 {
			return 0, supervisorError()
		}
		return readZombieStatus(pid)
	}
}

func readZombieStatus(pid int) (unix.WaitStatus, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil || len(data) == 0 || len(data) > 4096 {
		return 0, supervisorError()
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 || end+2 >= len(data) {
		return 0, supervisorError()
	}
	fields := strings.Fields(string(data[end+2:]))
	if len(fields) <= 49 || fields[0] != "Z" {
		return 0, supervisorError()
	}
	code, err := strconv.ParseUint(fields[49], 10, 32)
	if err != nil || code > 0xffff {
		return 0, supervisorError()
	}
	status := unix.WaitStatus(code)
	if !status.Exited() && !status.Signaled() {
		return 0, supervisorError()
	}
	return status, nil
}

func waitGate(pid int) (unix.WaitStatus, error) {
	if pid <= 0 {
		return 0, supervisorError()
	}
	for {
		var status unix.WaitStatus
		got, err := unix.Wait4(pid, &status, 0, nil)
		if err == unix.EINTR {
			continue
		}
		if err != nil || got != pid {
			return 0, supervisorError()
		}
		return status, nil
	}
}

func recordTerminal(
	journal *StateJournal, state postgres16protocol.ConfinementStateShape,
	raw unix.WaitStatus,
) (postgres16protocol.ConfinementStateShape, error) {
	state.Sequence++
	state.Phase = postgres16protocol.ConfinementPhaseTerminal
	terminal := &postgres16protocol.ConfinementTerminal{}
	if raw.Exited() {
		terminal.Kind = postgres16protocol.ConfinementTerminalExited
		terminal.ExitCode = uint32(raw.ExitStatus())
	} else if raw.Signaled() {
		terminal.Kind = postgres16protocol.ConfinementTerminalSignaled
		terminal.Signal = uint32(raw.Signal())
		terminal.CoreDumped = raw.CoreDump()
	} else {
		return postgres16protocol.ConfinementStateShape{}, supervisorError()
	}
	state.Terminal = terminal
	var err error
	terminal.EvidenceSHA256, err = state.TerminalEvidenceSHA256()
	if err != nil {
		return postgres16protocol.ConfinementStateShape{}, err
	}
	if err := journal.Append(state); err != nil {
		return postgres16protocol.ConfinementStateShape{}, err
	}
	return journal.last, nil
}
