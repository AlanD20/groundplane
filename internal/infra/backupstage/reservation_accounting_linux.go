package backupstage

import (
	"context"
	"encoding/binary"
	"golang.org/x/sys/unix"
	"math"
)

func availableBytes(ctx context.Context, rootFD int, ops linuxOperations) (uint64, error) {
	var stat unix.Statfs_t
	if err := ops.fstatfs(rootFD, &stat); err != nil {
		return 0, systemError("inspect staging filesystem capacity", err)
	}
	if stat.Bsize <= 0 {
		return 0, internalError("staging filesystem reported an invalid block size")
	}
	blockSize := uint64(stat.Bsize)
	if stat.Bavail > math.MaxUint64/blockSize {
		return 0, internalError("staging filesystem capacity overflow")
	}
	return stat.Bavail * blockSize, nil
}

func writeReservation(ctx context.Context, fd int, value uint64, ops linuxOperations) error {
	var encoded [reservationValueLen]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	binary.BigEndian.PutUint64(encoded[8:], ^value)
	if err := unix.Ftruncate(fd, reservationValueLen); err != nil {
		return systemError("truncate capacity reservation", err)
	}
	return pwriteFull(ctx, fd, encoded[:], 0, ops)
}

func readReservation(ctx context.Context, fd int) (uint64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return 0, systemError("inspect capacity reservation", err)
	}
	if stat.Size < reservationValueLen || stat.Size%reservationValueLen != 0 {
		return 0, internalError("capacity reservation state is corrupt")
	}
	remaining := uint64(0)
	for offset := int64(0); offset < stat.Size; offset += reservationValueLen {
		if err := contextError(ctx); err != nil {
			return 0, err
		}
		var encoded [reservationValueLen]byte
		n, err := unix.Pread(fd, encoded[:], offset)
		if err != nil {
			return 0, systemError("read capacity reservation", err)
		}
		value, complement := binary.BigEndian.Uint64(encoded[:8]), binary.BigEndian.Uint64(encoded[8:])
		if n != len(encoded) || complement != ^value {
			return 0, internalError("capacity reservation state is corrupt")
		}
		if offset == 0 {
			remaining = value
			continue
		}
		if value > remaining {
			return 0, internalError("capacity reservation consumption exceeds its bound")
		}
		remaining -= value
	}
	return remaining, nil
}

func appendReservationConsumption(ctx context.Context, fd int, consumed uint64, ops linuxOperations) error {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return systemError("inspect capacity reservation before update", err)
	}
	if stat.Size < reservationValueLen || stat.Size%reservationValueLen != 0 {
		return internalError("capacity reservation state is corrupt before update")
	}
	var encoded [reservationValueLen]byte
	binary.BigEndian.PutUint64(encoded[:], consumed)
	binary.BigEndian.PutUint64(encoded[8:], ^consumed)
	writeErr := pwriteFull(ctx, fd, encoded[:], stat.Size, ops)
	if writeErr == nil {
		writeErr = storageOperationError("sync consumed capacity reservation", ops.fdatasync(fd))
	}
	if writeErr == nil {
		return nil
	}
	poisonErr := rawOperationError("poison failed capacity reservation",
		unix.Ftruncate(fd, stat.Size+reservationValueLen+1))
	if poisonErr == nil {
		poisonErr = storageOperationError("sync poisoned capacity reservation", ops.fdatasync(fd))
	}
	return joinPrivate(writeErr, poisonErr)
}

func pwriteFull(ctx context.Context, fd int, content []byte, offset int64, ops linuxOperations) error {
	for len(content) != 0 {
		if err := contextError(ctx); err != nil {
			return err
		}
		written, err := ops.pwrite(fd, content, offset)
		if written < 0 || written > len(content) {
			return internalError("capacity reservation pwrite returned an invalid count")
		}
		if written != 0 {
			content = content[written:]
			offset += int64(written)
		}
		if err != nil {
			return storageSystemError("write capacity reservation", err)
		}
		if written == 0 {
			return internalError("capacity reservation pwrite made no progress")
		}
	}
	return nil
}
