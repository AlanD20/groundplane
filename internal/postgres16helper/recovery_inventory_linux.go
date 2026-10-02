//go:build linux

package postgres16helper

import (
	"context"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
)

func (runtime Runtime) recoveryInventory(
	ctx context.Context,
	operation *PreparedOperation,
) (postgres16protocol.ExitCode, error) {
	deadline := time.Unix(0, int64(operation.Request.DeadlineUnixNano))
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	records, err := runtime.collectRecoveryInventory(bounded, operation.stateDir)
	if err != nil {
		if bounded.Err() != nil {
			return postgres16protocol.ExitDeadlineExceeded, bounded.Err()
		}
		return postgres16protocol.ExitRecoveryRequired, err
	}
	if err := bounded.Err(); err != nil {
		return postgres16protocol.ExitDeadlineExceeded, err
	}
	encoded, err := postgres16protocol.MarshalRecoveryInventory(
		postgres16protocol.RecoveryInventory{Records: records},
	)
	if err != nil {
		return postgres16protocol.ExitRecoveryRequired, err
	}
	if err := writeAll(os.Stdout, encoded); err != nil {
		return postgres16protocol.ExitIOFailed, err
	}
	return postgres16protocol.ExitSuccess, nil
}

func (runtime Runtime) collectRecoveryInventory(
	ctx context.Context,
	directory *os.File,
) ([]postgres16protocol.RecoveryRecord, error) {
	retained, err := scanRetainedRecovery(ctx, directory)
	if err != nil {
		return nil, err
	}
	records := make([]postgres16protocol.RecoveryRecord, 0, len(retained))
	for _, retainedRecord := range retained {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		nonce, files := retainedRecord.nonce, retainedRecord.files
		journal, err := OpenExistingStateJournal(directory, nonce)
		if err != nil {
			return nil, err
		}
		state, exists := journal.Current()
		if !exists || state.Validate() != nil || validateRetainedLinks(directory, nonce, files, state) != nil {
			_ = journal.Close()
			return nil, supervisorError()
		}
		if files.process && state.Phase < postgres16protocol.ConfinementPhaseTerminal {
			if err := retireUnavailable(ctx, directory, nonce, journal, state); err != nil {
				_ = journal.Close()
				return nil, err
			}
			state, exists = journal.Current()
			if !exists || state.Validate() != nil ||
				validateRetainedLinks(directory, nonce, files, state) != nil {
				_ = journal.Close()
				return nil, supervisorError()
			}
		}
		if err := journal.Close(); err != nil {
			return nil, supervisorError()
		}
		// Without the durable exec intent there is no recorded operation or
		// request digest for the Controller to bind. The state remains retained,
		// but the whole inventory is unavailable rather than calling it clean.
		if !files.exec {
			return nil, supervisorError()
		}
		intent, err := loadExecIntent(directory, nonce)
		if err != nil {
			return nil, err
		}
		record, err := postgres16protocol.NewRecoveryRecord(
			nonce,
			files.process,
			files.exec,
			intent.Operation,
			intent.RequestSHA256,
			state,
		)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}
