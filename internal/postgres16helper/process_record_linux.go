package postgres16helper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

const processRecordSize = 100

type processRecord struct {
	PID          uint32
	StartTicks   uint64
	BootID       postgres16protocol.ConfinementBootID
	IntentSHA256 postgres16protocol.Digest
}

func processRecordName(nonce postgres16protocol.Nonce) string {
	return nonce.String() + ".process"
}

func storeProcessRecord(
	directory *os.File, nonce postgres16protocol.Nonce, intentSHA256 postgres16protocol.Digest,
	pid int, executable postgres16protocol.ConfinementFileIdentity,
) error {
	identity, err := inspectProcess(pid, executable)
	if err != nil || identity.PID != uint32(pid) {
		return supervisorError()
	}
	record := processRecord{
		PID: identity.PID, StartTicks: identity.StartTicks,
		BootID: identity.BootID, IntentSHA256: intentSHA256,
	}
	fd, err := unix.Openat2(int(directory.Fd()), processRecordName(nonce), &unix.OpenHow{
		Flags:   unix.O_WRONLY | unix.O_CREAT | unix.O_EXCL | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Mode:    uint64(postgres16protocol.StateFileMode),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return supervisorError()
	}
	file := os.NewFile(uintptr(fd), processRecordName(nonce))
	defer file.Close()
	encoded := encodeProcessRecord(record)
	if writeAll(file, encoded[:]) != nil || file.Sync() != nil || unix.Fsync(int(directory.Fd())) != nil {
		return supervisorError()
	}
	return nil
}

func loadProcessRecord(directory *os.File, nonce postgres16protocol.Nonce) (processRecord, error) {
	fd, err := unix.Openat2(int(directory.Fd()), processRecordName(nonce), &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return processRecord{}, supervisorError()
	}
	file := os.NewFile(uintptr(fd), processRecordName(nonce))
	defer file.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG ||
		stat.Mode&0o7777 != postgres16protocol.StateFileMode ||
		stat.Uid != postgres16protocol.StateFileUID || stat.Gid != postgres16protocol.StateFileGID ||
		stat.Nlink != 1 || stat.Size != processRecordSize {
		return processRecord{}, supervisorError()
	}
	var encoded [processRecordSize]byte
	if _, err := io.ReadFull(file, encoded[:]); err != nil {
		return processRecord{}, supervisorError()
	}
	checksum := sha256.Sum256(encoded[:68])
	if string(encoded[:8]) != "GPPG16P1" || !bytes.Equal(checksum[:], encoded[68:]) {
		return processRecord{}, supervisorError()
	}
	record := processRecord{
		PID:        binary.BigEndian.Uint32(encoded[8:12]),
		StartTicks: binary.BigEndian.Uint64(encoded[12:20]),
	}
	copy(record.BootID[:], encoded[20:36])
	copy(record.IntentSHA256[:], encoded[36:68])
	if record.PID == 0 || record.StartTicks == 0 || record.BootID == (postgres16protocol.ConfinementBootID{}) ||
		record.IntentSHA256 == (postgres16protocol.Digest{}) {
		return processRecord{}, supervisorError()
	}
	return record, nil
}

func encodeProcessRecord(record processRecord) [processRecordSize]byte {
	var encoded [processRecordSize]byte
	copy(encoded[:8], "GPPG16P1")
	binary.BigEndian.PutUint32(encoded[8:12], record.PID)
	binary.BigEndian.PutUint64(encoded[12:20], record.StartTicks)
	copy(encoded[20:36], record.BootID[:])
	copy(encoded[36:68], record.IntentSHA256[:])
	checksum := sha256.Sum256(encoded[:68])
	copy(encoded[68:], checksum[:])
	return encoded
}

func (runtime Runtime) stop(
	ctx context.Context, operation *PreparedOperation,
) (postgres16protocol.ExitCode, error) {
	record, err := loadProcessRecord(operation.stateDir, operation.Request.Nonce)
	if err != nil {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	pidfd, err := unix.PidfdOpen(int(record.PID), 0)
	if err != nil {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	defer unix.Close(pidfd)
	if !sameRecordedProcess(record, pidfd) {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	if err := unix.PidfdSendSignal(pidfd, unix.SIGTERM, nil, 0); err != nil {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	deadline := time.Unix(0, int64(operation.Request.DeadlineUnixNano))
	for time.Now().Before(deadline) && ctx.Err() == nil {
		fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
		if n, err := unix.Poll(fds, 500); err == nil && n > 0 && fds[0].Revents&unix.POLLIN != 0 {
			return postgres16protocol.ExitSuccess, nil
		}
	}
	if !sameRecordedProcess(record, pidfd) {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	if err := unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0); err != nil {
		return postgres16protocol.ExitRecoveryRequired, supervisorError()
	}
	return postgres16protocol.ExitDeadlineExceeded, supervisorError()
}

func sameRecordedProcess(record processRecord, pidfd int) bool {
	identity, err := processLifetime(int(record.PID))
	if err != nil || identity.StartTicks != record.StartTicks || identity.BootID != record.BootID {
		return false
	}
	fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, 0)
	return err == nil && n == 0
}
