package postgres16helper

import (
	"context"
	"errors"
	"io"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

func (runtime Runtime) executionEvidence(
	ctx context.Context,
	operation *PreparedOperation,
) (postgres16protocol.ExitCode, error) {
	deadline := time.Unix(0, int64(operation.Request.DeadlineUnixNano))
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return postgres16protocol.ExitDeadlineExceeded, err
	}
	request := operation.Request
	if request.Operation == postgres16protocol.OperationRetire {
		if err := retireExecution(bounded, operation.stateDir, request); err != nil {
			if bounded.Err() != nil {
				return postgres16protocol.ExitDeadlineExceeded, bounded.Err()
			}
			return postgres16protocol.ExitRecoveryRequired, err
		}
		return postgres16protocol.ExitSuccess, nil
	}
	evidence, err := readExecutionEvidence(operation.stateDir, request.Nonce, request.RequestSHA256)
	if err != nil {
		return postgres16protocol.ExitRecoveryRequired, err
	}
	if err := bounded.Err(); err != nil {
		return postgres16protocol.ExitDeadlineExceeded, err
	}
	encoded, err := postgres16protocol.MarshalExecutionEvidence(evidence)
	if err != nil || writeAll(os.Stdout, encoded) != nil {
		return postgres16protocol.ExitIOFailed, supervisorError()
	}
	return postgres16protocol.ExitSuccess, nil
}

func readExecutionEvidence(directory *os.File, nonce postgres16protocol.Nonce,
	requestSHA postgres16protocol.Digest,
) (postgres16protocol.ExecutionEvidence, error) {
	state, _, err := readRetainedExecution(directory, nonce, requestSHA)
	if err != nil {
		return postgres16protocol.ExecutionEvidence{}, err
	}
	evidence := postgres16protocol.ExecutionEvidence{Nonce: nonce, RequestSHA256: requestSHA, State: state}
	return evidence, evidence.Validate()
}

func readRetainedExecution(directory *os.File, nonce postgres16protocol.Nonce,
	requestSHA postgres16protocol.Digest,
) (postgres16protocol.ConfinementStateShape, execIntent, error) {
	journal, err := OpenExistingStateJournal(directory, nonce)
	if err != nil {
		return postgres16protocol.ConfinementStateShape{}, execIntent{}, err
	}
	defer journal.Close()
	state, found := journal.Current()
	if !found ||
		validateRetainedLinks(directory, nonce, retainedFiles{state: true, process: true, exec: true}, state) != nil {
		return postgres16protocol.ConfinementStateShape{}, execIntent{}, supervisorError()
	}
	intent, err := loadExecIntent(directory, nonce)
	if err != nil || intent.RequestSHA256 != requestSHA {
		return postgres16protocol.ConfinementStateShape{}, execIntent{}, supervisorError()
	}
	return state, intent, nil
}

func readRetirableRecoveryRecord(directory *os.File, nonce postgres16protocol.Nonce,
	requestSHA postgres16protocol.Digest,
) (postgres16protocol.RecoveryRecord, error) {
	state, intent, err := readRetainedExecution(directory, nonce, requestSHA)
	if err != nil {
		return postgres16protocol.RecoveryRecord{}, err
	}
	record, err := postgres16protocol.NewRecoveryRecord(
		nonce, true, true, intent.Operation, intent.RequestSHA256, state,
	)
	if err != nil || !record.Retirable() {
		return postgres16protocol.RecoveryRecord{}, supervisorError()
	}
	return record, nil
}

func retireExecution(ctx context.Context, directory *os.File, request postgres16protocol.Request) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	marker, found, err := readRetirement(directory, request.Nonce)
	if err != nil {
		return err
	}
	if found {
		if marker.Record.RequestSHA256 != request.RequestSHA256 {
			return supervisorError()
		}
		return finishRetirement(ctx, directory, marker)
	}
	absent := true
	for _, suffix := range []string{".state", ".process", ".exec"} {
		if err := ctx.Err(); err != nil {
			return err
		}
		var metadata unix.Stat_t
		err := unix.Fstatat(int(directory.Fd()), request.Nonce.String()+suffix, &metadata, unix.AT_SYMLINK_NOFOLLOW)
		if err == nil {
			absent = false
		} else if !errors.Is(err, unix.ENOENT) {
			return supervisorError()
		}
	}
	if absent {
		// Retire never claims execution success. The caller already holds the
		// acknowledged terminal proof; an exact repeated cleanup may be absent.
		return nil
	}
	record, err := readRetirableRecoveryRecord(directory, request.Nonce, request.RequestSHA256)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	marker = postgres16protocol.RecoveryRetirement{Record: record}
	encoded, err := postgres16protocol.MarshalRecoveryRetirement(marker)
	if err != nil {
		return err
	}
	name := request.Nonce.String() + ".retiring"
	fd, err := unix.Openat2(int(directory.Fd()), name, &unix.OpenHow{
		Flags:   unix.O_WRONLY | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Mode:    uint64(postgres16protocol.StateFileMode),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return supervisorError()
	}
	file := os.NewFile(uintptr(fd), name)
	writeErr := writeAll(file, encoded)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil || unix.Fsync(int(directory.Fd())) != nil {
		return supervisorError()
	}
	return finishRetirement(ctx, directory, marker)
}

func readRetirement(
	directory *os.File,
	nonce postgres16protocol.Nonce,
) (postgres16protocol.RecoveryRetirement, bool, error) {
	name := nonce.String() + ".retiring"
	fd, err := unix.Openat2(int(directory.Fd()), name, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if errors.Is(err, unix.ENOENT) {
		return postgres16protocol.RecoveryRetirement{}, false, nil
	}
	if err != nil {
		return postgres16protocol.RecoveryRetirement{}, false, supervisorError()
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	var metadata unix.Stat_t
	if unix.Fstat(fd, &metadata) != nil || metadata.Mode&unix.S_IFMT != unix.S_IFREG ||
		metadata.Mode&0o7777 != postgres16protocol.StateFileMode || metadata.Uid != 0 || metadata.Gid != 0 ||
		metadata.Nlink != 1 || metadata.Size <= 0 || metadata.Size > postgres16protocol.MaximumRecoveryRetirementEncodedBytes {
		return postgres16protocol.RecoveryRetirement{}, false, supervisorError()
	}
	encoded, err := io.ReadAll(io.LimitReader(file, postgres16protocol.MaximumRecoveryRetirementEncodedBytes+1))
	if err != nil {
		return postgres16protocol.RecoveryRetirement{}, false, supervisorError()
	}
	retirement, err := postgres16protocol.ParseRecoveryRetirement(encoded)
	if err != nil || retirement.Record.Nonce != nonce {
		return postgres16protocol.RecoveryRetirement{}, false, supervisorError()
	}
	return retirement, true, nil
}

func finishRetirement(ctx context.Context, directory *os.File, retirement postgres16protocol.RecoveryRetirement) error {
	if !retirement.Record.Retirable() {
		return supervisorError()
	}
	record := retirement.Record
	for _, suffix := range []string{".exec", ".process", ".state"} {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := record.Nonce.String() + suffix
		var metadata unix.Stat_t
		err := unix.Fstatat(int(directory.Fd()), name, &metadata, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(err, unix.ENOENT) {
			continue
		}
		if err != nil || metadata.Mode&unix.S_IFMT != unix.S_IFREG || metadata.Uid != 0 || metadata.Gid != 0 ||
			metadata.Mode&0o7777 != postgres16protocol.StateFileMode || metadata.Nlink != 1 ||
			unix.Unlinkat(int(directory.Fd()), name, 0) != nil {
			return supervisorError()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if unix.Fsync(int(directory.Fd())) != nil ||
		unix.Unlinkat(int(directory.Fd()), record.Nonce.String()+".retiring", 0) != nil ||
		unix.Fsync(int(directory.Fd())) != nil {
		return supervisorError()
	}
	return nil
}
