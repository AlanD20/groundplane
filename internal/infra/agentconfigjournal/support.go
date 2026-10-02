package agentconfigjournal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

func (journal *Journal) openLeaf(
	ctx context.Context,
	name string,
	flags int,
	mode uint32,
	maximumBytes int,
) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(
		int(journal.directory.Fd()),
		name,
		flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC,
		mode,
	)
	if err != nil {
		return nil, fileError(&os.PathError{Op: "openat", Path: name, Err: err})
	}
	file := os.NewFile(uintptr(fd), name)
	info, err := file.Stat()
	if err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || !info.Mode().IsRegular() || stat.Uid != 0 || stat.Nlink != 1 ||
			stat.Mode&0o7777 != 0o600 || info.Size() > int64(maximumBytes) {
			err = invalidRecord()
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, fileError(err)
	}
	return file, nil
}

func openPrivateRoot(ctx context.Context, path, base string, create bool) (*os.Root, *os.File, error) {
	root, err := os.OpenRoot("/")
	if err != nil {
		return nil, nil, fileError(err)
	}
	parts, current := strings.Split(strings.TrimPrefix(path, "/"), "/"), ""
	for _, part := range parts {
		if err := ctx.Err(); err != nil {
			_ = root.Close()
			return nil, nil, err
		}
		current += "/" + part
		before, err := root.Lstat(part)
		if errors.Is(err, fs.ErrNotExist) && current == path && !create {
			if err := root.Close(); err != nil {
				return nil, nil, fileError(err)
			}
			return nil, nil, nil
		}
		if errors.Is(err, fs.ErrNotExist) && create && (current == base || current == path) {
			if err := root.Mkdir(part, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
				_ = root.Close()
				return nil, nil, fileError(err)
			}
			parent, syncErr := root.Open(".")
			if syncErr == nil {
				syncErr = errors.Join(parent.Sync(), parent.Close())
			}
			if syncErr != nil {
				_ = root.Close()
				return nil, nil, fileError(syncErr)
			}
			before, err = root.Lstat(part)
		}
		if err != nil {
			_ = root.Close()
			return nil, nil, fileError(err)
		}
		stat, ok := before.Sys().(*syscall.Stat_t)
		ancestor := current != base && current != path
		if !ok || !before.IsDir() || stat.Uid != 0 ||
			(!ancestor && stat.Mode&0o7777 != 0o700) || (ancestor && stat.Mode&0o022 != 0) {
			_ = root.Close()
			return nil, nil, invalidRecord()
		}
		next, err := root.OpenRoot(part)
		if err != nil {
			_ = root.Close()
			return nil, nil, fileError(err)
		}
		after, statErr := next.Stat(".")
		closeErr := root.Close()
		if statErr != nil || closeErr != nil || !os.SameFile(before, after) {
			_ = next.Close()
			return nil, nil, invalidRecord()
		}
		root = next
	}
	directory, err := root.Open(".")
	if err != nil {
		_ = root.Close()
		return nil, nil, fileError(err)
	}
	return root, directory, nil
}

func validExpected(expected *agentpb.BackupConfigContentAuthority) bool {
	return expected != nil && executionplan.RejectUnknown(expected) == nil && len(expected.ManifestSha256) == 32 &&
		len(expected.MetadataSnapshotSha256) == 32 && expected.EntryCount <= backupconfig.MaxEntries &&
		expected.TotalSelectedValueBytes <= backupconfig.MaxTotalSelectedValueBytes && expected.ManifestSizeBytes > 0 &&
		expected.ManifestSizeBytes <= backupconfig.MaxManifestBytes && expected.SourceSizeBytes > 0 &&
		expected.SourceSizeBytes <= backupconfig.MaxSourceBytes && expected.SourceSizeBytes%backupconfig.TarBlockBytes == 0
}

func encodeAuthority(
	binding backupconfigtransfer.Binding,
	step *agentpb.BackupStepAuthority,
) ([]byte, error) {
	content, err := proto.MarshalOptions{Deterministic: true}.Marshal(step)
	if err != nil {
		return nil, fileError(err)
	}
	fields := []string{binding.TaskID, binding.AssignmentID, binding.StepID, binding.ExecutionID, binding.TransferID}
	size := 2 + 4 + len(content)
	for _, field := range fields {
		size += 2 + len(field)
	}
	if size > maximumAuthorityLen {
		return nil, invalidRecord()
	}
	raw, offset := make([]byte, size), 2
	raw[0], raw[1] = authoritySchema, byte(binding.Direction)
	for _, field := range fields {
		binary.BigEndian.PutUint16(raw[offset:offset+2], uint16(len(field)))
		offset += 2
		copy(raw[offset:], field)
		offset += len(field)
	}
	binary.BigEndian.PutUint32(raw[offset:offset+4], uint32(len(content)))
	copy(raw[offset+4:], content)
	return raw, nil
}

func recordName(sequence uint64) string {
	return fmt.Sprintf("%0*d%s", sequenceDigits, sequence, committedSuffix)
}
func nextName(sequence uint64) string {
	return fmt.Sprintf("%0*d%s", sequenceDigits, sequence, uncommittedSuffix)
}

func parseRecordName(name string) (uint64, bool, bool) {
	committed, suffix := strings.HasSuffix(name, committedSuffix), committedSuffix
	if !committed {
		suffix = uncommittedSuffix
		if !strings.HasSuffix(name, suffix) {
			return 0, false, false
		}
	}
	digits := strings.TrimSuffix(name, suffix)
	if len(digits) != sequenceDigits {
		return 0, false, false
	}
	sequence, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || recordName(sequence) != digits+committedSuffix {
		return 0, false, false
	}
	return sequence, committed, true
}

func invalidRecord() error {
	return errs.New(errs.KindValidationFailed, "Agent Config journal record is invalid")
}
func conflict(message string) error { return errs.New(errs.KindStateConflict, message) }
func fileError(err error) error {
	if err == nil {
		return nil
	}
	return errs.Wrap(errs.KindStorageUnavailable, err)
}
