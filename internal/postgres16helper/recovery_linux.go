package postgres16helper

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

type retainedFiles struct {
	state   bool
	process bool
	exec    bool
}

type retainedRecovery struct {
	nonce postgres16protocol.Nonce
	files retainedFiles
}

// inspectRecovery runs before a new client operation. It never treats silence,
// an absent PID, or a newer process at the same PID as proof that an attempted
// restore may run again. Unknown outcomes retain their root-only records and
// deny ordinary work until the owning Agent/Controller reconciles them.
func (runtime Runtime) inspectRecovery(ctx context.Context, directory *os.File) error {
	records, err := scanRetainedRecovery(ctx, directory)
	if err != nil {
		return err
	}
	for _, record := range records {
		nonce, files := record.nonce, record.files
		journal, err := OpenExistingStateJournal(directory, nonce)
		if err != nil {
			return err
		}
		state, exists := journal.Current()
		if !exists {
			_ = journal.Close()
			return supervisorError()
		}
		if state.Phase == postgres16protocol.ConfinementPhaseReaped {
			err = validateRetainedLinks(directory, nonce, files, state)
			_ = journal.Close()
			if err != nil {
				return err
			}
			continue
		}
		if err := validateRetainedLinks(directory, nonce, files, state); err != nil {
			_ = journal.Close()
			return err
		}
		if files.process && state.Phase < postgres16protocol.ConfinementPhaseTerminal {
			if err := retireUnavailable(ctx, directory, nonce, journal, state); err != nil {
				_ = journal.Close()
				return err
			}
		}
		_ = journal.Close()
		return supervisorError()
	}
	return nil
}

func scanRetainedRecovery(ctx context.Context, directory *os.File) ([]retainedRecovery, error) {
	entries, err := directory.ReadDir(4097)
	if err != nil && !errors.Is(err, io.EOF) || len(entries) > 4096 {
		return nil, supervisorError()
	}
	retired := make(map[string]bool)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !strings.HasSuffix(entry.Name(), ".retiring") {
			continue
		}
		nonce, err := postgres16protocol.ParseNonce(strings.TrimSuffix(entry.Name(), ".retiring"))
		if err != nil {
			return nil, supervisorError()
		}
		evidence, found, err := readRetirement(directory, nonce)
		if err != nil || !found || finishRetirement(ctx, directory, evidence) != nil {
			return nil, supervisorError()
		}
		retired[nonce.String()] = true
	}
	byNonce := make(map[postgres16protocol.Nonce]retainedFiles)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".retiring") {
			continue
		}
		separator := strings.LastIndexByte(name, '.')
		if separator < 0 {
			return nil, supervisorError()
		}
		suffix := name[separator:]
		if suffix != ".state" && suffix != ".process" && suffix != ".exec" {
			return nil, supervisorError()
		}
		if entry.IsDir() || separator != 64 {
			return nil, supervisorError()
		}
		if retired[name[:separator]] {
			continue
		}
		nonce, err := postgres16protocol.ParseNonce(name[:separator])
		if err != nil {
			return nil, supervisorError()
		}
		files := byNonce[nonce]
		switch suffix {
		case ".state":
			files.state = true
		case ".process":
			files.process = true
		case ".exec":
			files.exec = true
		}
		byNonce[nonce] = files
	}
	if len(byNonce) > postgres16protocol.MaximumRecoveryInventoryRecords {
		return nil, supervisorError()
	}
	records := make([]retainedRecovery, 0, len(byNonce))
	for nonce, files := range byNonce {
		if !files.state || files.exec && !files.process {
			return nil, supervisorError()
		}
		records = append(records, retainedRecovery{nonce: nonce, files: files})
	}
	sort.Slice(records, func(left, right int) bool {
		return bytes.Compare(records[left].nonce[:], records[right].nonce[:]) < 0
	})
	return records, nil
}

func validateRetainedLinks(
	directory *os.File, nonce postgres16protocol.Nonce, files retainedFiles,
	state postgres16protocol.ConfinementStateShape,
) error {
	if !files.process {
		if files.exec || state.ProgressPhase >= postgres16protocol.ConfinementPhaseGateDurable {
			return supervisorError()
		}
		return nil
	}
	process, err := loadProcessRecord(directory, nonce)
	if err != nil || process.IntentSHA256 != state.LaunchIntentSHA256 {
		return supervisorError()
	}
	if !files.exec {
		if state.ProgressPhase >= postgres16protocol.ConfinementPhaseChildDurable {
			return supervisorError()
		}
		return nil
	}
	intent, err := loadExecIntent(directory, nonce)
	if err != nil || intent.IntentSHA256 != state.LaunchIntentSHA256 ||
		intent.PID != process.PID || intent.StartTicks != process.StartTicks || intent.BootID != process.BootID ||
		intent.JournalSequence > state.Sequence ||
		state.ProgressPhase < postgres16protocol.ConfinementPhaseProfileApplied ||
		intent.ProfileFrameSHA256 != state.ProfileFrameSHA256 {
		return supervisorError()
	}
	return nil
}

func retireUnavailable(
	ctx context.Context, directory *os.File, nonce postgres16protocol.Nonce,
	journal *StateJournal, state postgres16protocol.ConfinementStateShape,
) error {
	process, err := loadProcessRecord(directory, nonce)
	if err != nil {
		return supervisorError()
	}
	currentBoot, err := readCurrentBootID()
	if err != nil {
		return err
	}
	kind := postgres16protocol.ConfinementTerminalUnavailableNotParent
	if process.BootID != currentBoot {
		kind = postgres16protocol.ConfinementTerminalUnavailableBootChanged
	} else {
		pidfd, err := unix.PidfdOpen(int(process.PID), 0)
		if err == nil {
			defer unix.Close(pidfd)
			lifetime, err := processLifetime(int(process.PID))
			if err != nil {
				return supervisorError()
			}
			if lifetime.StartTicks == process.StartTicks && lifetime.BootID == process.BootID {
				fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
				count, pollErr := unix.Poll(fds, 0)
				if pollErr != nil || count > 0 && fds[0].Revents&unix.POLLIN == 0 {
					return supervisorError()
				}
				if count == 0 {
					if err := unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0); err != nil {
						return supervisorError()
					}
					bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					for bounded.Err() == nil {
						count, pollErr = unix.Poll(fds, 100)
						if pollErr != nil && !errors.Is(pollErr, unix.EINTR) {
							return supervisorError()
						}
						if count > 0 && fds[0].Revents&unix.POLLIN != 0 {
							break
						}
					}
					if count == 0 || fds[0].Revents&unix.POLLIN == 0 {
						return supervisorError()
					}
				}
			}
		} else if !errors.Is(err, unix.ESRCH) {
			return supervisorError()
		}
	}
	state.Sequence++
	state.Phase = postgres16protocol.ConfinementPhaseRecoveryRetired
	state.Terminal = &postgres16protocol.ConfinementTerminal{Kind: kind}
	state.GateFatal = nil
	state.Terminal.EvidenceSHA256, err = state.TerminalEvidenceSHA256()
	if err != nil {
		return err
	}
	return journal.Append(state)
}

func readCurrentBootID() (postgres16protocol.ConfinementBootID, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return postgres16protocol.ConfinementBootID{}, supervisorError()
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(strings.TrimSpace(string(data)), "-", ""))
	if err != nil || len(decoded) != 16 {
		return postgres16protocol.ConfinementBootID{}, supervisorError()
	}
	var boot postgres16protocol.ConfinementBootID
	copy(boot[:], decoded)
	return boot, nil
}
