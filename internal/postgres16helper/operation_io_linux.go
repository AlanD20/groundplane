package postgres16helper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"golang.org/x/sys/unix"
)

type streamResult struct {
	evidence postgres16protocol.ConfinementStreamEvidence
	proof    []byte
	err      error
}

func prepareClientInput(
	ctx context.Context, operation *PreparedOperation,
) (*os.File, postgres16protocol.ConfinementStreamEvidence, error) {
	if operation.StreamPolicy.Input == postgres16protocol.InputNone {
		file, err := os.OpenFile("/dev/null", os.O_RDONLY, 0)
		if err != nil {
			return nil, postgres16protocol.ConfinementStreamEvidence{}, supervisorError()
		}
		return file, postgres16protocol.ConfinementStreamEvidence{
			SHA256: postgres16protocol.Digest(sha256.Sum256(nil)), EOF: true,
		}, nil
	}
	fd, err := unix.Openat(int(operation.stateDir.Fd()), ".",
		unix.O_TMPFILE|unix.O_RDWR|unix.O_CLOEXEC, postgres16protocol.StateFileMode)
	if err != nil {
		return nil, postgres16protocol.ConfinementStreamEvidence{}, supervisorError()
	}
	file := os.NewFile(uintptr(fd), "postgres16-restore-source")
	closeOnError := func() (*os.File, postgres16protocol.ConfinementStreamEvidence, error) {
		_ = file.Close()
		return nil, postgres16protocol.ConfinementStreamEvidence{}, supervisorError()
	}
	hash := sha256.New()
	var counted uint64
	buffer := make([]byte, 32*1024)
	for counted < operation.Request.SourceSize {
		if ctx.Err() != nil {
			return closeOnError()
		}
		remaining := operation.Request.SourceSize - counted
		wanted := len(buffer)
		if remaining < uint64(wanted) {
			wanted = int(remaining)
		}
		n, err := readBeforeDeadline(ctx, int(os.Stdin.Fd()), buffer[:wanted])
		if err != nil || n <= 0 || writeAll(file, buffer[:n]) != nil {
			return closeOnError()
		}
		_, _ = hash.Write(buffer[:n])
		counted += uint64(n)
	}
	var extra [1]byte
	n, err := readBeforeDeadline(ctx, int(os.Stdin.Fd()), extra[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		return closeOnError()
	}
	var digest postgres16protocol.Digest
	copy(digest[:], hash.Sum(nil))
	if digest != operation.Request.SourceSHA256 || file.Sync() != nil {
		return closeOnError()
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return closeOnError()
	}
	return file, postgres16protocol.ConfinementStreamEvidence{
		Bytes: counted, SHA256: digest, EOF: true,
	}, nil
}

func readBeforeDeadline(ctx context.Context, fd int, buffer []byte) (int, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		timeout := 1000
		if deadline, okay := ctx.Deadline(); okay {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return 0, context.DeadlineExceeded
			}
			if remaining < time.Second {
				timeout = int(remaining.Milliseconds())
				if timeout < 1 {
					timeout = 1
				}
			}
		}
		fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN | unix.POLLHUP}}
		n, err := unix.Poll(fds, timeout)
		if err == unix.EINTR || n == 0 {
			continue
		}
		if err != nil || fds[0].Revents&unix.POLLNVAL != 0 {
			return 0, supervisorError()
		}
		read, err := unix.Read(fd, buffer)
		if err == unix.EINTR || err == unix.EAGAIN {
			continue
		}
		if err != nil {
			return 0, err
		}
		if read == 0 {
			return 0, io.EOF
		}
		return read, nil
	}
}

func copyClientOutput(
	reader *os.File, limit uint64, target io.Writer, retainProof bool,
) streamResult {
	hash := sha256.New()
	var counted uint64
	var proof bytes.Buffer
	buffer := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			if counted > limit || uint64(n) > limit-counted {
				return streamResult{err: supervisorError()}
			}
			counted += uint64(n)
			_, _ = hash.Write(buffer[:n])
			if retainProof {
				_, _ = proof.Write(buffer[:n])
			}
			if target != nil && writeAll(target, buffer[:n]) != nil {
				return streamResult{err: supervisorError()}
			}
		}
		if errors.Is(err, io.EOF) {
			return streamResult{
				evidence: streamEvidence(counted, hash, true), proof: proof.Bytes(),
			}
		}
		if err != nil {
			return streamResult{err: supervisorError()}
		}
	}
}

func streamEvidence(counted uint64, hash hash.Hash, eof bool) postgres16protocol.ConfinementStreamEvidence {
	var digest postgres16protocol.Digest
	copy(digest[:], hash.Sum(nil))
	return postgres16protocol.ConfinementStreamEvidence{Bytes: counted, SHA256: digest, EOF: eof}
}
