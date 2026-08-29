//go:build linux

package scriptrunner

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"golang.org/x/sys/unix"
)

func (store *bodyStore) PrepareEntries(
	assignmentID string,
	executionID string,
	entries []*agentpb.ScriptEntryArtifact,
) error {
	if store == nil || store.tasksFD < 0 || ids.Validate(ids.KindAssignment, assignmentID) != nil ||
		!validBodyExecutionID(executionID) {
		return errs.New(errs.KindInternal, "Script Entry store: preparation request is invalid")
	}
	assignmentFD, err := openDirectory(store.tasksFD, assignmentID, store.ownerUID, store.ownerGID, store.device)
	if err != nil {
		return err
	}
	defer unix.Close(assignmentFD)
	executionFD, err := openDirectory(assignmentFD, executionID, store.ownerUID, store.ownerGID, store.device)
	if err != nil {
		return err
	}
	defer unix.Close(executionFD)
	for _, entry := range entries {
		if entry.Binding.Kind != agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE {
			continue
		}
		leaf := entryArtifactLeaf(entry.Binding)
		found, err := store.existingEntry(executionFD, leaf, entry)
		if err != nil {
			return err
		}
		if !found {
			if err := store.publishEntry(executionFD, leaf, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (store *bodyStore) existingEntry(
	executionFD int,
	leaf string,
	entry *agentpb.ScriptEntryArtifact,
) (bool, error) {
	fd, err := openBeneath(executionFD, leaf, unix.O_RDONLY|unix.O_CLOEXEC)
	if errors.Is(err, unix.ENOENT) {
		return false, nil
	}
	if err != nil {
		return false, bodyStoreError("open existing Entry", err)
	}
	file := os.NewFile(uintptr(fd), leaf)
	if file == nil {
		unix.Close(fd)
		return false, errs.New(errs.KindInternal, "Script Entry store: own existing descriptor")
	}
	defer file.Close()
	if _, err := verifiedEntryFile(fd, store.device, entry.Binding); err != nil {
		return false, err
	}
	existing, err := io.ReadAll(io.LimitReader(file, int64(len(entry.Value))+1))
	if err != nil {
		clear(existing)
		return false, bodyStoreError("read existing Entry", err)
	}
	defer clear(existing)
	if !bytes.Equal(existing, entry.Value) {
		return false, errs.New(errs.KindStateConflict, "Script Entry store: existing artifact differs")
	}
	return true, nil
}

func (store *bodyStore) publishEntry(
	executionFD int,
	leaf string,
	entry *agentpb.ScriptEntryArtifact,
) error {
	temporary, err := temporaryEntryName()
	if err != nil {
		return err
	}
	fd, err := unix.Openat(
		executionFD,
		temporary,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return bodyStoreError("create temporary Entry", err)
	}
	published := false
	defer func() {
		_ = unix.Close(fd)
		if !published {
			_ = unix.Unlinkat(executionFD, temporary, 0)
		}
	}()
	for remaining := entry.Value; len(remaining) != 0; {
		written, writeErr := unix.Write(fd, remaining)
		if writeErr != nil {
			return bodyStoreError("write temporary Entry", writeErr)
		}
		if written <= 0 {
			return errs.New(errs.KindInternal, "Script Entry store: write did not advance")
		}
		remaining = remaining[written:]
	}
	if err := unix.Fchown(fd, int(entry.Binding.Uid), int(entry.Binding.Gid)); err != nil {
		return bodyStoreError("set Entry ownership", err)
	}
	if err := unix.Fchmod(fd, entry.Binding.Mode); err != nil {
		return bodyStoreError("set Entry mode", err)
	}
	stat, err := verifiedEntryFile(fd, store.device, entry.Binding)
	if err != nil {
		return err
	}
	if stat.Size != int64(len(entry.Value)) {
		return errs.New(errs.KindInternal, "Script Entry store: temporary size differs")
	}
	if err := unix.Fsync(fd); err != nil {
		return bodyStoreError("sync temporary Entry", err)
	}
	if err := unix.Renameat2(executionFD, temporary, executionFD, leaf, unix.RENAME_NOREPLACE); err != nil {
		return bodyStoreError("publish Entry", err)
	}
	published = true
	if err := unix.Fsync(executionFD); err != nil {
		_ = unix.Unlinkat(executionFD, leaf, 0)
		_ = unix.Fsync(executionFD)
		return bodyStoreError("sync Entry directory", err)
	}
	return nil
}

func (store *bodyStore) RemoveEntries(
	assignmentID string,
	executionID string,
	entries []*agentpb.ScriptEntryArtifact,
) error {
	if store == nil || store.tasksFD < 0 || ids.Validate(ids.KindAssignment, assignmentID) != nil ||
		!validBodyExecutionID(executionID) {
		return errs.New(errs.KindInternal, "Script Entry store: cleanup request is invalid")
	}
	assignmentFD, err := openDirectory(store.tasksFD, assignmentID, store.ownerUID, store.ownerGID, store.device)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(assignmentFD)
	executionFD, err := openDirectory(assignmentFD, executionID, store.ownerUID, store.ownerGID, store.device)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(executionFD)
	for _, entry := range entries {
		if entry.Binding.Kind != agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE {
			continue
		}
		leaf := entryArtifactLeaf(entry.Binding)
		fd, err := openBeneath(executionFD, leaf, unix.O_RDONLY|unix.O_CLOEXEC)
		if errors.Is(err, unix.ENOENT) {
			continue
		}
		if err != nil {
			return bodyStoreError("open Entry during cleanup", err)
		}
		file := os.NewFile(uintptr(fd), leaf)
		if file == nil {
			unix.Close(fd)
			return errs.New(errs.KindInternal, "Script Entry store: own cleanup descriptor")
		}
		if _, err := verifiedEntryFile(fd, store.device, entry.Binding); err != nil {
			_ = file.Close()
			return err
		}
		content, readErr := io.ReadAll(io.LimitReader(file, int64(len(entry.Value))+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			clear(content)
			return bodyStoreError("read Entry during cleanup", errors.Join(readErr, closeErr))
		}
		matches := bytes.Equal(content, entry.Value)
		clear(content)
		if !matches {
			return errs.New(errs.KindStateConflict, "Script Entry store: artifact changed before cleanup")
		}
		if err := unix.Unlinkat(executionFD, leaf, 0); err != nil {
			return bodyStoreError("remove Entry", err)
		}
	}
	if err := unix.Fsync(executionFD); err != nil {
		return bodyStoreError("sync Entry removal", err)
	}
	return nil
}

func (store *bodyStore) ProveExecutionAbsent(assignmentID, executionID string) error {
	if store == nil || store.tasksFD < 0 || ids.Validate(ids.KindAssignment, assignmentID) != nil ||
		!validBodyExecutionID(executionID) {
		return errs.New(errs.KindInternal, "Script Entry store: absence proof request is invalid")
	}
	assignmentFD, err := openDirectory(store.tasksFD, assignmentID, store.ownerUID, store.ownerGID, store.device)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(assignmentFD)
	executionFD, err := openDirectory(assignmentFD, executionID, store.ownerUID, store.ownerGID, store.device)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err == nil {
		_ = unix.Close(executionFD)
		return errs.New(errs.KindStateConflict, "Script Entry store: execution directory remains")
	}
	return err
}

func entryArtifactLeaf(binding *agentpb.ScriptRunnerEntryBinding) string {
	digest := sha256.Sum256([]byte(binding.EntryId + "\x00" + binding.ValueGenerationId))
	return "entry-" + hex.EncodeToString(digest[:])
}

func verifiedEntryFile(
	fd int,
	device uint64,
	binding *agentpb.ScriptRunnerEntryBinding,
) (*unix.Stat_t, error) {
	stat := &unix.Stat_t{}
	if err := unix.Fstat(fd, stat); err != nil {
		return nil, bodyStoreError("inspect Entry", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o7777 != binding.Mode || stat.Nlink != 1 ||
		uint64(stat.Dev) != device || stat.Uid != binding.Uid || stat.Gid != binding.Gid {
		return nil, errs.New(errs.KindStateConflict, "Script Entry store: filesystem evidence is invalid")
	}
	return stat, nil
}

func temporaryEntryName() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", bodyStoreError("allocate temporary Entry identity", err)
	}
	return ".entry.tmp." + hex.EncodeToString(value), nil
}
