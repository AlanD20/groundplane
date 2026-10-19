package agentworkjournal

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"golang.org/x/sys/unix"
)

// The bound includes unfinished .next writes. Existing attempts can still save
// acknowledgements and retire at capacity; no active evidence is evicted.
const MaximumAttempts = 1024

func (journal *Journal) attempts(ctx context.Context) (map[string]bool, error) {
	directory, err := journal.root.Open(".")
	if err != nil {
		return nil, fileError(err)
	}
	defer directory.Close() // Read-only enumeration descriptor.
	attempts := make(map[string]bool)
	entries := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		names, readErr := directory.Readdirnames(64)
		for _, name := range names {
			entries++
			if entries > 2*MaximumAttempts {
				return nil, invalid()
			}
			committed := strings.HasSuffix(name, ".json")
			suffix := ".next"
			if committed {
				suffix = ".json"
			}
			attemptID := strings.TrimSuffix(name, suffix)
			if attemptID == name || ids.Validate(ids.KindAttempt, attemptID) != nil {
				return nil, invalid()
			}
			file, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0)
			if err != nil {
				return nil, err
			}
			if err := file.Close(); err != nil {
				return nil, fileError(err)
			}
			attempts[attemptID] = attempts[attemptID] || committed
			if len(attempts) > MaximumAttempts {
				return nil, invalid()
			}
		}
		if errors.Is(readErr, io.EOF) {
			return attempts, nil
		}
		if readErr != nil {
			return nil, fileError(readErr)
		}
	}
}

// Scripts enumerates committed delivery records for reconciliation. Partial
// writes reserve capacity but never become permission to execute or send.
func (journal *Journal) Scripts(ctx context.Context) ([]ScriptRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return nil, err
	}
	records := make([]ScriptRecord, 0, len(attempts))
	for attemptID, committed := range attempts {
		if !committed {
			continue
		}
		state, err := journal.readRecord(ctx, attemptID)
		if err != nil {
			return nil, err
		}
		if state == nil {
			return nil, conflict()
		}
		record, err := state.script()
		if err != nil {
			return nil, err
		}
		if record != nil {
			records = append(records, *record)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Attempt.ID < records[j].Attempt.ID })
	return records, nil
}

// Retire requires the terminal Attempt returned by authenticated Controller
// settlement, never a locally predicted outcome or an expired lease. The caller
// must finish result acknowledgement before releasing this delivery evidence.
func (journal *Journal) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	if terminal.Validate() != nil || terminal.Executor.Executor != workexecution.Agent ||
		terminal.FinishedAt == nil || terminal.Result == nil {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	record, err := journal.readRecord(ctx, terminal.ID)
	if err != nil {
		return err
	}
	if record != nil {
		pending, err := record.pending()
		if err != nil {
			return err
		}
		if !workexecution.SameAssignment(record.Attempt, terminal) || pending ||
			terminal.FinishedAt.Before(record.UpdatedAt) {
			return conflict()
		}
		if backing := record.Backing; backing != nil {
			receipt := backing.Completion
			if backing.Failure != nil {
				receipt = backing.Failure.Receipt
			}
			if !sameBackingReceipt(receipt, terminal) {
				return conflict()
			}
			if backing.Failure != nil && terminal.StartedAt != nil {
				// A failure ACK settles delivery, not helper, file or hook
				// execution. Retain the boundary for explicit cleanup proof.
				return nil
			}
		}
		if recovery := record.BackingRecovery; recovery != nil {
			if !sameBackingReceipt(recovery.Receipt, terminal) {
				return conflict()
			}
			if terminal.Outcome != workexecution.Succeeded {
				return nil
			}
			if err := journal.retireRecoveredBacking(ctx, *recovery); err != nil {
				return err
			}
		}
		if recovery := record.AttachProvisionRecovery; recovery != nil {
			if recovery.Receipt == nil || recovery.Receipt.Outcome != terminal.Outcome ||
				!recovery.Receipt.FinishedAt.Equal(
					*terminal.FinishedAt,
				) || *recovery.Receipt.Result != *terminal.Result {
				return conflict()
			}
		}
		if provision := record.EnvironmentProvision; provision != nil {
			if provision.Receipt == nil || provision.Receipt.Outcome != terminal.Outcome ||
				!provision.Receipt.FinishedAt.Equal(
					*terminal.FinishedAt,
				) || *provision.Receipt.Result != *terminal.Result {
				return conflict()
			}
		}
		if backup := record.Backup; backup != nil {
			saved := backup.Completion
			if saved == nil || saved.Outcome != terminal.Outcome || !saved.FinishedAt.Equal(*terminal.FinishedAt) ||
				*saved.Result != *terminal.Result {
				return conflict()
			}
		}
		if failure := record.AttachProvisionFailure; failure != nil {
			if failure.Receipt == nil || failure.Receipt.Outcome != terminal.Outcome ||
				!failure.Receipt.FinishedAt.Equal(*terminal.FinishedAt) || *failure.Receipt.Result != *terminal.Result {
				return conflict()
			}
			if record.AttachProvision != nil {
				// Keep command/process evidence until provisioning inspection and
				// settlement account for all authorized effects.
				return nil
			}
		} else if provision := record.AttachProvision; provision != nil {
			saved := provision.Completion
			if saved == nil || saved.Outcome != terminal.Outcome || !saved.FinishedAt.Equal(*terminal.FinishedAt) ||
				*saved.Result != *terminal.Result {
				return conflict()
			}
		}
		if recovery := record.AttachRecovery; recovery != nil {
			if recovery.Receipt == nil || recovery.Receipt.Outcome != terminal.Outcome ||
				!recovery.Receipt.FinishedAt.Equal(
					*terminal.FinishedAt,
				) || *recovery.Receipt.Result != *terminal.Result {
				return conflict()
			}
		}
		if failure := record.AttachFailure; failure != nil {
			if failure.Receipt == nil || failure.Receipt.Outcome != terminal.Outcome ||
				!failure.Receipt.FinishedAt.Equal(*terminal.FinishedAt) ||
				*failure.Receipt.Result != *terminal.Result {
				return conflict()
			}
			if record.AttachRemoval != nil && record.AttachRemoval.Report.Progress.InFlight {
				// Acknowledging failure releases delivery capacity, not the command
				// identities needed for an explicit interrupted-command inspection.
				// Recovery retirement must provide its own Controller receipt.
				return nil
			}
		} else if removal := record.AttachRemoval; removal != nil {
			checkpoint, err := removal.Report.CheckpointAuthority(record.Attempt, removal.Authority)
			if err != nil {
				return err
			}
			if !removal.Acknowledged || removal.Completion == nil || !removal.Completion.FinishedAt.Equal(*terminal.FinishedAt) ||
				removal.Report.Progress.InFlight || removal.Report.Progress.Completed != removal.Authority.Commands ||
				terminal.Outcome != workexecution.Succeeded || *terminal.Result != checkpoint.Evidence {
				return conflict()
			}
		}
		if failure := record.EnvironmentFailure; failure != nil {
			if failure.Receipt == nil || failure.Receipt.Outcome != terminal.Outcome ||
				!failure.Receipt.FinishedAt.Equal(*terminal.FinishedAt) || *failure.Receipt.Result != *terminal.Result {
				return conflict()
			}
		} else if removal := record.EnvironmentRemoval; removal != nil {
			input, err := removal.input(record.Attempt)
			if err != nil {
				return err
			}
			checkpoint, err := removal.Report.Checkpoint(input)
			if err != nil {
				return err
			}
			if !removal.Acknowledged || removal.Completion == nil || !removal.Completion.FinishedAt.Equal(*terminal.FinishedAt) ||
				removal.Report.Progress.Phase != hierarchyexecution.EnvironmentRemoved ||
				terminal.Outcome != workexecution.Succeeded || *terminal.Result != checkpoint.Evidence {
				return conflict()
			}
		}
		if entry := record.EntryConfiguration; entry != nil {
			// The failed forward boundary remains until restoration is proved.
			receipt := entry.Completion
			if entry.Failure != nil {
				receipt = entry.Failure.Receipt
			}
			if receipt == nil || entry.Failure == nil && !entry.Acknowledged || receipt.Outcome != terminal.Outcome ||
				!receipt.FinishedAt.Equal(*terminal.FinishedAt) || *receipt.Result != *terminal.Result ||
				!workexecution.SameAssignment(*receipt, terminal) || terminal.StartedAt == nil ||
				!receipt.StartedAt.Equal(*terminal.StartedAt) {
				return conflict()
			}
			if entry.Failure != nil && entry.Progress.Sequence != 0 {
				// Terminal failure acknowledges delivery, not the end of an
				// authorized file/workload effect. Keep its inspection boundary.
				return nil
			}
		}
		if recovery := record.EntryRecovery; recovery != nil {
			receipt := recovery.Receipt
			if receipt == nil || receipt.Outcome != terminal.Outcome || receipt.FinishedAt == nil ||
				receipt.Result == nil ||
				!receipt.FinishedAt.Equal(*terminal.FinishedAt) ||
				*receipt.Result != *terminal.Result ||
				!workexecution.SameAssignment(*receipt, terminal) ||
				terminal.StartedAt == nil ||
				!receipt.StartedAt.Equal(*terminal.StartedAt) {
				return conflict()
			}
			if receipt.Outcome != workexecution.Succeeded {
				return nil
			}
		}
		if record.Component != nil || record.ComponentRecovery != nil {
			keep, err := journal.retireComponent(ctx, *record, terminal)
			if err != nil || keep {
				return err
			}
		}
		if recovery := record.NetworkRecovery; recovery != nil {
			receipt := recovery.Receipt
			if recovery.Failure != nil {
				receipt = recovery.Failure.Receipt
			}
			if receipt == nil || receipt.Outcome != terminal.Outcome ||
				!receipt.FinishedAt.Equal(*terminal.FinishedAt) || *receipt.Result != *terminal.Result {
				return conflict()
			}
			if terminal.Outcome == workexecution.Succeeded {
				if err := journal.retireRecoveredNetwork(ctx, *recovery); err != nil {
					return err
				}
			}
		}
		if network := record.Network; network != nil {
			// A checkpoint ack is not a terminal receipt. Unresolved storage
			// effects must remain available for inspection after reconnect.
			receipt := network.Completion
			if network.Failure != nil {
				receipt = network.Failure.Receipt
			}
			if receipt == nil || receipt.Outcome != terminal.Outcome ||
				!receipt.FinishedAt.Equal(*terminal.FinishedAt) || *receipt.Result != *terminal.Result {
				return conflict()
			}
			if network.Failure != nil {
				failure, err := networkexecution.DecodeFailure(terminal, network.Failure.Evidence, *terminal.Result)
				if err != nil {
					return err
				}
				if failure.Checkpoint != nil && failure.Checkpoint.Sequence%2 == 1 {
					return nil
				}
			}
		}
		if recovery := record.VolumeRecovery; recovery != nil {
			receipt := recovery.Receipt
			if recovery.Failure != nil {
				receipt = recovery.Failure.Receipt
			}
			if receipt == nil || receipt.Outcome != terminal.Outcome ||
				!receipt.FinishedAt.Equal(*terminal.FinishedAt) || *receipt.Result != *terminal.Result {
				return conflict()
			}
			if terminal.Outcome == workexecution.Succeeded {
				if err := journal.retireRecoveredVolume(ctx, *recovery); err != nil {
					return err
				}
			}
		}
		if recovery := record.LifecycleRecovery; recovery != nil {
			receipt := recovery.Receipt
			if receipt == nil || receipt.FinishedAt == nil || receipt.Result == nil ||
				receipt.Outcome != terminal.Outcome || !receipt.FinishedAt.Equal(*terminal.FinishedAt) || *receipt.Result != *terminal.Result {
				return conflict()
			}
		}
		if lifecycle := record.Lifecycle; lifecycle != nil {
			receipt := lifecycle.Completion
			if lifecycle.Failure != nil {
				receipt = lifecycle.Failure.Receipt
			}
			if receipt == nil || receipt.Outcome != terminal.Outcome ||
				!receipt.FinishedAt.Equal(*terminal.FinishedAt) ||
				*receipt.Result != *terminal.Result {
				return conflict()
			}
			if lifecycle.Failure != nil {
				failure, err := lifecycleexecution.DecodeFailureStages(
					terminal,
					lifecycle.Stages,
					lifecycle.Failure.Evidence,
					*terminal.Result,
				)
				if err != nil {
					return err
				}
				// Interrupted effects remain available to recovery inspection.
				if lifecycle.Progress.BeganAt != nil ||
					failure.Checkpoint != nil && failure.Checkpoint.Sequence%2 == 1 {
					return nil
				}
			}
		}
		if volume := record.Volume; volume != nil {
			// A checkpoint ack is not a terminal receipt. Unresolved storage
			// effects must remain available for inspection after reconnect.
			receipt := volume.Completion
			if volume.Failure != nil {
				receipt = volume.Failure.Receipt
			}
			if receipt == nil || receipt.Outcome != terminal.Outcome ||
				!receipt.FinishedAt.Equal(*terminal.FinishedAt) || *receipt.Result != *terminal.Result {
				return conflict()
			}
			if volume.Failure != nil {
				failure, err := volumeexecution.DecodeFailure(terminal, volume.Failure.Evidence, *terminal.Result)
				if err != nil {
					return err
				}
				if failure.Checkpoint != nil && failure.Checkpoint.Sequence%2 == 1 {
					return nil
				}
			}
		}
		if record.Release != nil && record.Release.Progress.Phase == releaseexecution.Complete {
			if record.Completion == nil || record.Release.Ack == nil ||
				record.Completion.Outcome != terminal.Outcome || !record.Completion.FinishedAt.Equal(*terminal.FinishedAt) ||
				*terminal.Result != record.Release.Ack.Evidence {
				return conflict()
			}
		}
		if failure := record.ScriptFailure; failure != nil {
			if failure.Receipt == nil || failure.Receipt.Outcome != terminal.Outcome ||
				!failure.Receipt.FinishedAt.Equal(*terminal.FinishedAt) || *failure.Receipt.Result != *terminal.Result {
				return conflict()
			}
		}
		if recovery := record.ScriptRecovery; recovery != nil && record.ScriptFailure == nil {
			if recovery.Receipt == nil || recovery.Receipt.Outcome != terminal.Outcome ||
				!recovery.Receipt.FinishedAt.Equal(
					*terminal.FinishedAt,
				) || *recovery.Receipt.Result != *terminal.Result {
				return conflict()
			}
		}
		if script, err := record.script(); err != nil {
			return err
		} else if script != nil && record.Release == nil && record.Recovery == nil && record.ScriptFailure == nil {
			if record.ScriptCompletion == nil || record.ScriptCompletion.Outcome != terminal.Outcome ||
				!record.ScriptCompletion.FinishedAt.Equal(*terminal.FinishedAt) || *record.ScriptCompletion.Result != *terminal.Result {
				return conflict()
			}
		}
		if recovery := record.Recovery; recovery != nil {
			if recovery.Report.Progress.Phase != releaseexecution.RecoveryComplete || recovery.Report.Ack == nil ||
				terminal.Outcome != workexecution.Succeeded || *terminal.Result != recovery.Report.Ack.Evidence ||
				recovery.Completion == nil || recovery.Completion.Outcome != terminal.Outcome ||
				!recovery.Completion.FinishedAt.Equal(*terminal.FinishedAt) {
				return conflict()
			}
			if err := journal.retireRecoveredRelease(ctx, record.Attempt, *recovery); err != nil {
				return err
			}
		}
	}
	return journal.removeRecord(ctx, terminal.ID)
}

// removeRecord requires the journal lock and an independently checked terminal
// authority. It is not a general-purpose deletion API.
func (journal *Journal) removeRecord(ctx context.Context, attemptID string) error {
	// Remove uncommitted replacement first. A crash leaves the committed
	// report intact and retirement can be retried with the same terminal proof.
	for _, suffix := range []string{".next", ".json"} {
		name := attemptID + suffix
		file, err := journal.openLeaf(ctx, name, unix.O_RDONLY, 0)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return fileError(err)
		}
		if err := journal.root.Remove(name); err != nil {
			return fileError(err)
		}
	}
	return fileError(journal.directory.Sync())
}
