//go:build linux

package scriptrunner

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
	"golang.org/x/sys/unix"
)

const (
	bodyLeaf       = "body"
	bodyStoreTasks = "tasks"
	directoryMode  = 0o700
	bodyMode       = 0o400
)

type bodyIdentity struct {
	Device uint64
	Inode  uint64
	UID    uint32
	GID    uint32
	Size   uint32
	SHA256 [sha256.Size]byte
	Leaf   string
}

type preparedBody struct {
	hostPath     string
	assignmentID string
	executionID  string
	identity     bodyIdentity
}

type bodyStore struct {
	tasksFD  int
	rootPath string
	device   uint64
	ownerUID uint32
	ownerGID uint32
}

func newBodyStore(stateRoot string, ownerUID uint32, ownerGID uint32) (*bodyStore, error) {
	if !filepath.IsAbs(stateRoot) || filepath.Clean(stateRoot) != stateRoot {
		return nil, errs.New(errs.KindInternal, "Script body store: state root is invalid")
	}
	rootFD, err := unix.Open(stateRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, bodyStoreError("open state root", err)
	}
	defer unix.Close(rootFD)
	rootStat, err := verifiedDirectory(rootFD, ownerUID, ownerGID, 0)
	if err != nil {
		return nil, err
	}
	tasksFD, err := openOrCreateDirectory(rootFD, bodyStoreTasks, ownerUID, ownerGID, uint64(rootStat.Dev))
	if err != nil {
		return nil, err
	}
	return &bodyStore{
		tasksFD: tasksFD, rootPath: filepath.Join(stateRoot, bodyStoreTasks),
		device: uint64(rootStat.Dev), ownerUID: ownerUID, ownerGID: ownerGID,
	}, nil
}

func (store *bodyStore) Close() error {
	if store == nil || store.tasksFD < 0 {
		return nil
	}
	err := unix.Close(store.tasksFD)
	store.tasksFD = -1
	if err != nil {
		return bodyStoreError("close task root", err)
	}
	return nil
}

func (store *bodyStore) Prepare(
	assignmentID string,
	executionID string,
	body []byte,
	uid uint32,
	gid uint32,
) (preparedBody, error) {
	if store == nil || store.tasksFD < 0 || ids.Validate(ids.KindAssignment, assignmentID) != nil ||
		!validBodyExecutionID(executionID) || len(body) == 0 {
		return preparedBody{}, errs.New(errs.KindInternal, "Script body store: preparation request is invalid")
	}
	assignmentFD, err := openOrCreateDirectory(
		store.tasksFD,
		assignmentID,
		store.ownerUID,
		store.ownerGID,
		store.device,
	)
	if err != nil {
		return preparedBody{}, err
	}
	defer unix.Close(assignmentFD)
	executionFD, err := openOrCreateDirectory(assignmentFD, executionID, store.ownerUID, store.ownerGID, store.device)
	if err != nil {
		return preparedBody{}, err
	}
	defer unix.Close(executionFD)

	identity, found, err := store.existingBody(executionFD, body, uid, gid)
	if err != nil {
		return preparedBody{}, err
	}
	if !found {
		identity, err = store.publishBody(executionFD, body, uid, gid)
		if err != nil {
			return preparedBody{}, err
		}
	}
	return preparedBody{
		hostPath:     filepath.Join(store.rootPath, assignmentID, executionID, bodyLeaf),
		assignmentID: assignmentID, executionID: executionID, identity: identity,
	}, nil
}

func (store *bodyStore) existingBody(
	executionFD int,
	body []byte,
	uid uint32,
	gid uint32,
) (bodyIdentity, bool, error) {
	fd, err := openBeneath(executionFD, bodyLeaf, unix.O_RDONLY|unix.O_CLOEXEC)
	if errors.Is(err, unix.ENOENT) {
		return bodyIdentity{}, false, nil
	}
	if err != nil {
		return bodyIdentity{}, false, bodyStoreError("open existing body", err)
	}
	file := os.NewFile(uintptr(fd), bodyLeaf)
	if file == nil {
		unix.Close(fd)
		return bodyIdentity{}, false, errs.New(errs.KindInternal, "Script body store: own existing body descriptor")
	}
	defer file.Close()
	stat, err := verifiedBodyFile(fd, store.device, uid, gid)
	if err != nil {
		return bodyIdentity{}, false, err
	}
	existing, err := io.ReadAll(io.LimitReader(file, int64(len(body))+1))
	if err != nil {
		clear(existing)
		return bodyIdentity{}, false, bodyStoreError("read existing body", err)
	}
	defer clear(existing)
	if !bytes.Equal(existing, body) {
		return bodyIdentity{}, false, errs.New(errs.KindStateConflict, "Script body store: existing body differs")
	}
	digest := sha256.Sum256(body)
	return bodyIdentity{
		Device: uint64(stat.Dev), Inode: stat.Ino, UID: stat.Uid, GID: stat.Gid,
		Size: uint32(len(body)), SHA256: digest, Leaf: bodyLeaf,
	}, true, nil
}

func (store *bodyStore) publishBody(
	executionFD int,
	body []byte,
	uid uint32,
	gid uint32,
) (bodyIdentity, error) {
	temporary, err := temporaryBodyName()
	if err != nil {
		return bodyIdentity{}, err
	}
	fd, err := unix.Openat(
		executionFD,
		temporary,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return bodyIdentity{}, bodyStoreError("create temporary body", err)
	}
	published := false
	defer func() {
		_ = unix.Close(fd)
		if !published {
			_ = unix.Unlinkat(executionFD, temporary, 0)
		}
	}()
	for remaining := body; len(remaining) != 0; {
		written, writeErr := unix.Write(fd, remaining)
		if writeErr != nil {
			return bodyIdentity{}, bodyStoreError("write temporary body", writeErr)
		}
		if written <= 0 {
			return bodyIdentity{}, errs.New(
				errs.KindInternal,
				"Script body store: temporary body write did not advance",
			)
		}
		remaining = remaining[written:]
	}
	if err := unix.Fchown(fd, int(uid), int(gid)); err != nil {
		return bodyIdentity{}, bodyStoreError("set body ownership", err)
	}
	if err := unix.Fchmod(fd, bodyMode); err != nil {
		return bodyIdentity{}, bodyStoreError("set body mode", err)
	}
	stat, err := verifiedBodyFile(fd, store.device, uid, gid)
	if err != nil {
		return bodyIdentity{}, err
	}
	if stat.Size != int64(len(body)) {
		return bodyIdentity{}, errs.New(errs.KindInternal, "Script body store: temporary body size differs")
	}
	if err := unix.Fsync(fd); err != nil {
		return bodyIdentity{}, bodyStoreError("sync temporary body", err)
	}
	if err := unix.Renameat2(executionFD, temporary, executionFD, bodyLeaf, unix.RENAME_NOREPLACE); err != nil {
		return bodyIdentity{}, bodyStoreError("publish body", err)
	}
	published = true
	if err := unix.Fsync(executionFD); err != nil {
		_ = unix.Unlinkat(executionFD, bodyLeaf, 0)
		_ = unix.Fsync(executionFD)
		return bodyIdentity{}, bodyStoreError("sync execution directory", err)
	}
	digest := sha256.Sum256(body)
	return bodyIdentity{
		Device: uint64(stat.Dev), Inode: stat.Ino, UID: stat.Uid, GID: stat.Gid,
		Size: uint32(len(body)), SHA256: digest, Leaf: bodyLeaf,
	}, nil
}

func (store *bodyStore) Remove(prepared preparedBody) error {
	if store == nil || store.tasksFD < 0 || prepared.assignmentID == "" || prepared.executionID == "" ||
		prepared.identity.Leaf != bodyLeaf {
		return errs.New(errs.KindInternal, "Script body store: cleanup evidence is invalid")
	}
	assignmentFD, err := openDirectory(
		store.tasksFD,
		prepared.assignmentID,
		store.ownerUID,
		store.ownerGID,
		store.device,
	)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(assignmentFD)
	executionFD, err := openDirectory(assignmentFD, prepared.executionID, store.ownerUID, store.ownerGID, store.device)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	defer unix.Close(executionFD)
	bodyFD, err := openBeneath(executionFD, bodyLeaf, unix.O_RDONLY|unix.O_CLOEXEC)
	if err == nil {
		stat, statErr := verifiedBodyFile(bodyFD, store.device, prepared.identity.UID, prepared.identity.GID)
		if statErr != nil {
			_ = unix.Close(bodyFD)
			return statErr
		}
		if uint64(stat.Dev) != prepared.identity.Device || stat.Ino != prepared.identity.Inode {
			_ = unix.Close(bodyFD)
			return errs.New(errs.KindStateConflict, "Script body store: body identity changed before cleanup")
		}
		file := os.NewFile(uintptr(bodyFD), bodyLeaf)
		if file == nil {
			_ = unix.Close(bodyFD)
			return errs.New(errs.KindInternal, "Script body store: own cleanup body descriptor")
		}
		content, readErr := io.ReadAll(io.LimitReader(file, int64(prepared.identity.Size)+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			clear(content)
			return bodyStoreError("read body during cleanup", errors.Join(readErr, closeErr))
		}
		digest := sha256.Sum256(content)
		sizeMatches := len(content) == int(prepared.identity.Size)
		clear(content)
		if !sizeMatches || digest != prepared.identity.SHA256 {
			return errs.New(errs.KindStateConflict, "Script body store: body content changed before cleanup")
		}
		if err := unix.Unlinkat(executionFD, bodyLeaf, 0); err != nil {
			return bodyStoreError("remove body", err)
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return bodyStoreError("inspect body during cleanup", err)
	}
	if err := unix.Fsync(executionFD); err != nil {
		return bodyStoreError("sync body removal", err)
	}
	if fd, err := openBeneath(executionFD, bodyLeaf, unix.O_RDONLY|unix.O_CLOEXEC); !errors.Is(err, unix.ENOENT) {
		if err == nil {
			_ = unix.Close(fd)
			return errs.New(errs.KindInternal, "Script body store: body remains after cleanup")
		}
		return bodyStoreError("prove body absence", err)
	}
	if err := unix.Unlinkat(assignmentFD, prepared.executionID, unix.AT_REMOVEDIR); err != nil {
		return bodyStoreError("remove execution directory", err)
	}
	if err := unix.Fsync(assignmentFD); err != nil {
		return bodyStoreError("sync execution directory removal", err)
	}
	if fd, err := openDirectory(assignmentFD, prepared.executionID, store.ownerUID, store.ownerGID, store.device); !errors.Is(
		err,
		unix.ENOENT,
	) {
		if err == nil {
			_ = unix.Close(fd)
			return errs.New(errs.KindInternal, "Script body store: execution directory remains after cleanup")
		}
		return err
	}
	if err := unix.Unlinkat(store.tasksFD, prepared.assignmentID, unix.AT_REMOVEDIR); err != nil &&
		!errors.Is(err, unix.ENOTEMPTY) && !errors.Is(err, unix.EEXIST) {
		return bodyStoreError("remove empty assignment directory", err)
	}
	return nil
}

func openOrCreateDirectory(parentFD int, name string, ownerUID uint32, ownerGID uint32, device uint64) (int, error) {
	err := unix.Mkdirat(parentFD, name, directoryMode)
	if err != nil && !errors.Is(err, unix.EEXIST) {
		return -1, bodyStoreError("create private directory", err)
	}
	return openDirectory(parentFD, name, ownerUID, ownerGID, device)
}

func openDirectory(parentFD int, name string, ownerUID uint32, ownerGID uint32, device uint64) (int, error) {
	fd, err := openBeneath(parentFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC)
	if err != nil {
		return -1, err
	}
	if _, err := verifiedDirectory(fd, ownerUID, ownerGID, device); err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func openBeneath(parentFD int, name string, flags int) (int, error) {
	return unix.Openat2(parentFD, name, &unix.OpenHow{
		Flags: uint64(flags),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS |
			unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_XDEV,
	})
}

func verifiedDirectory(fd int, ownerUID uint32, ownerGID uint32, expectedDevice uint64) (*unix.Stat_t, error) {
	stat := &unix.Stat_t{}
	if err := unix.Fstat(fd, stat); err != nil {
		return nil, bodyStoreError("inspect private directory", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0o7777 != directoryMode ||
		stat.Uid != ownerUID || stat.Gid != ownerGID || expectedDevice != 0 && uint64(stat.Dev) != expectedDevice {
		return nil, errs.New(errs.KindInternal, "Script body store: private directory evidence is invalid")
	}
	return stat, nil
}

func verifiedBodyFile(fd int, device uint64, uid uint32, gid uint32) (*unix.Stat_t, error) {
	stat := &unix.Stat_t{}
	if err := unix.Fstat(fd, stat); err != nil {
		return nil, bodyStoreError("inspect body", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o7777 != bodyMode || stat.Nlink != 1 ||
		uint64(stat.Dev) != device || stat.Uid != uid || stat.Gid != gid {
		return nil, errs.New(errs.KindInternal, "Script body store: body filesystem evidence is invalid")
	}
	return stat, nil
}

func temporaryBodyName() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", bodyStoreError("allocate temporary body identity", err)
	}
	return ".body.tmp." + hex.EncodeToString(value), nil
}

func validBodyExecutionID(value string) bool {
	if len(value) != 26 {
		return false
	}
	parsed, err := ulid.ParseStrict(value)
	return err == nil && parsed.String() == value
}

func bodyStoreError(operation string, err error) error {
	if errors.Is(err, syscall.EXDEV) || errors.Is(err, unix.ELOOP) {
		return errs.New(errs.KindInternal, "Script body store: path policy rejected")
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("script body store: %s: %w", operation, err))
}
