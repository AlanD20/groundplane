//go:build linux

package environmentdirectoryhelper

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (Creator) EnsureManagedVolumes(
	ctx context.Context,
	request ManagedVolumeEnsureRequest,
) error {
	return ensureManagedVolumeDirectories(ctx, request)
}

func (Creator) RemoveManagedVolume(
	ctx context.Context,
	request ManagedVolumeDirectoryRemoveRequest,
) (ManagedVolumeDirectoryRemoveResult, error) {
	return removeManagedVolume(ctx, request, 0)
}

// openEnvironmentDirectory returns both the stable Project directory and its
// Environment child. Callers own and must close both descriptors.
func openEnvironmentDirectory(volumeRoot, volumeDirectory string, expectedUID uint32) (int, int, error) {
	scope, err := environmentpath.Parse(volumeRoot, volumeDirectory)
	if err != nil {
		return -1, -1, err
	}
	rootFD, err := unix.Open(volumeRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, -1, errs.Wrap(errs.KindInternal, fmt.Errorf("open Environment volume root: %w", err))
	}
	if err := validateDirectory(rootFD, expectedUID); err != nil {
		_ = unix.Close(rootFD)
		return -1, -1, err
	}
	components := []string{scope.TenantID, scope.ProjectID}
	if scope.ProjectKind == environmentpath.ProjectKindBacking {
		components[0] = "platform"
	}
	parentFD := rootFD
	for _, component := range components {
		nextFD, openErr := openDirectoryAt(parentFD, component)
		closeErr := unix.Close(parentFD)
		if openErr != nil {
			return -1, -1, errs.Wrap(errs.KindInternal, fmt.Errorf("open Environment directory ancestor: %w", openErr))
		}
		if closeErr != nil {
			_ = unix.Close(nextFD)
			return -1, -1, errs.Wrap(errs.KindInternal, closeErr)
		}
		if err := validateDirectory(nextFD, expectedUID); err != nil {
			_ = unix.Close(nextFD)
			return -1, -1, err
		}
		parentFD = nextFD
	}
	environmentFD, err := openDirectoryAt(parentFD, scope.EnvironmentID)
	if err != nil {
		_ = unix.Close(parentFD)
		return -1, -1, errs.Wrap(errs.KindInternal, fmt.Errorf("open Environment directory: %w", err))
	}
	if err := validateDirectory(environmentFD, expectedUID); err != nil {
		_ = unix.Close(environmentFD)
		_ = unix.Close(parentFD)
		return -1, -1, err
	}
	return parentFD, environmentFD, nil
}
func ensureManagedVolumeDirectories(
	ctx context.Context,
	request ManagedVolumeEnsureRequest,
) error {
	return ensureManagedVolumeDirectoriesAs(ctx, request, 0)
}

func ensureManagedVolumeDirectoriesAs(
	ctx context.Context,
	request ManagedVolumeEnsureRequest,
	expectedUID uint32,
) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "Managed volume directory context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ids.Validate(ids.KindTask, request.TaskID) != nil ||
		ids.Validate(ids.KindOperation, request.OperationID) != nil ||
		len(request.IntentSHA256) != 32 ||
		len(request.Volumes) == 0 {
		return errs.New(errs.KindValidationFailed, "managed volume directory request identity is invalid")
	}
	parentFD, environmentFD, err := openEnvironmentDirectory(request.VolumeRoot, request.VolumeDir, expectedUID)
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)
	defer unix.Close(environmentFD)
	for _, volume := range request.Volumes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateManagedVolumeIdentity(volume); err != nil {
			return err
		}
		if err := ensureManagedVolumeDirectory(ctx, environmentFD, request, volume); err != nil {
			return err
		}
	}
	return nil
}

type volumeCreationEvidence struct {
	Schema      uint32 `json:"schema"`
	VolumeID    string `json:"volume_id"`
	OperationID string `json:"operation_id"`
	TaskID      string `json:"task_id"`
	ComposeKey  string `json:"compose_key"`
	Intent      string `json:"intent_sha256"`
}

type volumeRemovalFrame struct {
	Path  string `json:"path"`
	After string `json:"after"`
}

type volumeRemovalCursor struct {
	VolumeID   string               `json:"volume_id"`
	ComposeKey string               `json:"compose_key"`
	Intent     string               `json:"intent_sha256"`
	Frames     []volumeRemovalFrame `json:"frames"`
}

func removeManagedVolume(
	ctx context.Context,
	request ManagedVolumeDirectoryRemoveRequest,
	expectedUID uint32,
) (ManagedVolumeDirectoryRemoveResult, error) {
	if ctx == nil {
		return ManagedVolumeDirectoryRemoveResult{}, errs.New(
			errs.KindInternal,
			"managed volume removal context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return ManagedVolumeDirectoryRemoveResult{}, err
	}
	if ids.Validate(ids.KindTask, request.TaskID) != nil ||
		ids.Validate(ids.KindOperation, request.OperationID) != nil ||
		ids.Validate(ids.KindVolume, request.VolumeID) != nil ||
		!validComposeKey(request.ComposeKey) ||
		len(request.IntentSHA256) != 32 ||
		len(request.Cursor) > 16*1024 {
		return ManagedVolumeDirectoryRemoveResult{}, errs.New(
			errs.KindValidationFailed,
			"managed volume removal identity is invalid",
		)
	}
	parentFD, environmentFD, err := openEnvironmentDirectory(request.VolumeRoot, request.VolumeDir, expectedUID)
	if err != nil {
		return ManagedVolumeDirectoryRemoveResult{}, err
	}
	defer unix.Close(parentFD)
	defer unix.Close(environmentFD)
	leafFD, err := openDirectoryAt(environmentFD, request.ComposeKey)
	if errors.Is(err, syscall.ENOENT) {
		return ManagedVolumeDirectoryRemoveResult{Complete: true}, nil
	}
	if err != nil {
		return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(
			errs.KindValidationFailed,
			fmt.Errorf("open managed volume directory: %w", err),
		)
	}
	defer unix.Close(leafFD)
	var leafStat unix.Stat_t
	if err := unix.Fstat(leafFD, &leafStat); err != nil {
		return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(errs.KindInternal, err)
	}
	cursor, err := decodeRemovalCursor(request.Cursor, request.VolumeID, request.ComposeKey, request.IntentSHA256)
	if err != nil {
		return ManagedVolumeDirectoryRemoveResult{}, err
	}
	var mutations uint32
	for cursor.Frames != nil {
		if err := ctx.Err(); err != nil {
			return ManagedVolumeDirectoryRemoveResult{}, err
		}
		current := &cursor.Frames[len(cursor.Frames)-1]
		currentFD, openErr := openRelativeDirectory(leafFD, current.Path)
		if openErr != nil {
			return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(
				errs.KindInternal,
				fmt.Errorf("open managed volume traversal directory: %w", openErr),
			)
		}
		names, readErr := readDirectoryNames(currentFD)
		if readErr != nil {
			return ManagedVolumeDirectoryRemoveResult{}, readErr
		}
		nextName := ""
		for _, name := range names {
			if name > current.After {
				nextName = name
				break
			}
		}
		if nextName == "" {
			if len(cursor.Frames) == 1 {
				if err := unix.Unlinkat(environmentFD, request.ComposeKey, unix.AT_REMOVEDIR); err != nil &&
					!errors.Is(err, syscall.ENOENT) {
					return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(
						errs.KindInternal,
						fmt.Errorf("remove managed volume directory: %w", err),
					)
				}
				if err := unix.Fsync(environmentFD); err != nil {
					return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(errs.KindInternal, err)
				}
				mutations++
				return ManagedVolumeDirectoryRemoveResult{MutationCount: mutations, Complete: true}, nil
			}
			parentFrame := cursor.Frames[len(cursor.Frames)-2]
			childName := path.Base(current.Path)
			parentDirectoryFD, parentErr := openRelativeDirectory(leafFD, parentFrame.Path)
			if parentErr != nil {
				return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(errs.KindInternal, parentErr)
			}
			removeErr := unix.Unlinkat(parentDirectoryFD, childName, unix.AT_REMOVEDIR)
			if !errors.Is(removeErr, syscall.ENOENT) {
				if removeErr != nil {
					_ = unix.Close(parentDirectoryFD)
					return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(
						errs.KindInternal,
						fmt.Errorf("remove managed volume child directory: %w", removeErr),
					)
				}
				if fsyncErr := unix.Fsync(parentDirectoryFD); fsyncErr != nil {
					_ = unix.Close(parentDirectoryFD)
					return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(errs.KindInternal, fsyncErr)
				}
			}
			_ = unix.Close(parentDirectoryFD)
			mutations++
			cursor.Frames = cursor.Frames[:len(cursor.Frames)-1]
			cursor.Frames[len(cursor.Frames)-1].After = childName
			if mutations >= 128 {
				result := ManagedVolumeDirectoryRemoveResult{MutationCount: mutations}
				result.NextCursor, err = encodeRemovalCursor(cursor)
				return result, err
			}
			continue
		}
		currentFD, openErr = openRelativeDirectory(leafFD, current.Path)
		if openErr != nil {
			return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(errs.KindInternal, openErr)
		}
		var entryStat unix.Stat_t
		statErr := unix.Fstatat(currentFD, nextName, &entryStat, unix.AT_SYMLINK_NOFOLLOW)
		_ = unix.Close(currentFD)
		if errors.Is(statErr, syscall.ENOENT) {
			current.After = nextName
			continue
		}
		if statErr != nil {
			return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(
				errs.KindInternal,
				fmt.Errorf("inspect managed volume entry: %w", statErr),
			)
		}
		if entryStat.Mode&unix.S_IFMT == unix.S_IFDIR {
			if entryStat.Dev != leafStat.Dev {
				return ManagedVolumeDirectoryRemoveResult{}, errs.New(
					errs.KindValidationFailed,
					"managed volume traversal crosses a mount boundary",
				)
			}
			cursor.Frames = append(cursor.Frames, volumeRemovalFrame{
				Path: path.Join(current.Path, nextName),
			})
			continue
		}
		current.After = nextName
		currentFD, openErr = openRelativeDirectory(leafFD, current.Path)
		if openErr != nil {
			return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(errs.KindInternal, openErr)
		}
		unlinkErr := unix.Unlinkat(currentFD, nextName, 0)
		if !errors.Is(unlinkErr, syscall.ENOENT) {
			if unlinkErr != nil {
				_ = unix.Close(currentFD)
				return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(
					errs.KindInternal,
					fmt.Errorf("remove managed volume entry: %w", unlinkErr),
				)
			}
			if fsyncErr := unix.Fsync(currentFD); fsyncErr != nil {
				_ = unix.Close(currentFD)
				return ManagedVolumeDirectoryRemoveResult{}, errs.Wrap(errs.KindInternal, fsyncErr)
			}
		}
		_ = unix.Close(currentFD)
		mutations++
		result := ManagedVolumeDirectoryRemoveResult{MutationCount: mutations}
		if mutations >= 128 {
			result.NextCursor, err = encodeRemovalCursor(cursor)
			return result, err
		}
	}
	return ManagedVolumeDirectoryRemoveResult{}, errs.New(errs.KindInternal, "managed volume removal cursor is empty")
}

func decodeRemovalCursor(encoded []byte, volumeID, composeKey string, intent []byte) (volumeRemovalCursor, error) {
	wantIntent := hex.EncodeToString(intent)
	if len(encoded) == 0 {
		return volumeRemovalCursor{VolumeID: volumeID, ComposeKey: composeKey, Intent: wantIntent,
			Frames: []volumeRemovalFrame{{Path: ""}}}, nil
	}
	var cursor volumeRemovalCursor
	if len(encoded) > 16*1024 || json.Unmarshal(encoded, &cursor) != nil || cursor.VolumeID != volumeID ||
		cursor.ComposeKey != composeKey || cursor.Intent != wantIntent || len(cursor.Frames) == 0 {
		return volumeRemovalCursor{}, errs.New(errs.KindValidationFailed, "managed volume removal cursor is invalid")
	}
	for _, frame := range cursor.Frames {
		if frame.Path != "" && (path.IsAbs(frame.Path) || path.Clean(frame.Path) != frame.Path) {
			return volumeRemovalCursor{}, errs.New(
				errs.KindValidationFailed,
				"managed volume removal cursor path is invalid",
			)
		}
		if frame.Path != "" {
			for _, component := range strings.Split(frame.Path, "/") {
				if component == "." || component == ".." || component == "" || strings.ContainsRune(component, '\x00') {
					return volumeRemovalCursor{}, errs.New(
						errs.KindValidationFailed,
						"managed volume removal cursor path is invalid",
					)
				}
			}
		}
	}
	return cursor, nil
}

func encodeRemovalCursor(cursor volumeRemovalCursor) ([]byte, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(encoded) > 16*1024 {
		return nil, errs.New(errs.KindInternal, "managed volume removal cursor exceeds its bound")
	}
	return encoded, nil
}

func openRelativeDirectory(rootFD int, relative string) (int, error) {
	if relative == "" {
		return openDirectoryAt(rootFD, ".")
	}
	currentFD, err := openDirectoryAt(rootFD, ".")
	if err != nil {
		return -1, err
	}
	for _, component := range strings.Split(relative, "/") {
		nextFD, openErr := openDirectoryAt(currentFD, component)
		_ = unix.Close(currentFD)
		if openErr != nil {
			return -1, openErr
		}
		currentFD = nextFD
	}
	return currentFD, nil
}

func readDirectoryNames(fd int) ([]string, error) {
	directory := os.NewFile(uintptr(fd), "managed-volume-directory")
	if directory == nil {
		_ = unix.Close(fd)
		return nil, errs.New(errs.KindInternal, "open managed volume directory stream failed")
	}
	names, err := directory.Readdirnames(-1)
	closeErr := directory.Close()
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if closeErr != nil {
		return nil, errs.Wrap(errs.KindInternal, closeErr)
	}
	sort.Strings(names)
	return names, nil
}

func validateManagedVolumeIdentity(volume ManagedVolume) error {
	if ids.Validate(ids.KindVolume, volume.ID) != nil || !validComposeKey(volume.Key) {
		return errs.New(errs.KindValidationFailed, "managed volume identity is invalid")
	}
	return nil
}

func validComposeKey(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 255 || strings.ContainsRune(value, '/') ||
		strings.ContainsRune(value, '\x00') {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '.' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func ensureManagedVolumeDirectory(
	ctx context.Context,
	environmentFD int,
	request ManagedVolumeEnsureRequest,
	volume ManagedVolume,
) error {
	intent := hex.EncodeToString(request.IntentSHA256)
	evidence := volumeCreationEvidence{
		Schema: 1, VolumeID: volume.ID, OperationID: request.OperationID,
		TaskID: request.TaskID, ComposeKey: volume.Key, Intent: intent,
	}
	marker, err := json.Marshal(evidence)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	sibling := ".gp-volume-create-" + volume.ID + "-" + request.OperationID
	leafFD, err := openDirectoryAt(environmentFD, volume.Key)
	if err == nil {
		defer unix.Close(leafFD)
		markerErr := verifyMarker(leafFD, marker)
		if markerErr == nil {
			return finalizePublishedVolume(leafFD, environmentFD)
		}
		if !errors.Is(markerErr, syscall.ENOENT) {
			return errs.New(errs.KindValidationFailed, "managed volume destination is not task-owned")
		}
		return nil
	}
	if errors.Is(err, syscall.EACCES) {
		var leafStat unix.Stat_t
		if statErr := unix.Fstatat(environmentFD, volume.Key, &leafStat, unix.AT_SYMLINK_NOFOLLOW); statErr != nil {
			return errs.Wrap(
				errs.KindInternal,
				fmt.Errorf("inspect unreadable managed volume destination: %w", statErr),
			)
		}
		if leafStat.Mode&unix.S_IFMT != unix.S_IFDIR {
			return errs.New(errs.KindValidationFailed, "managed volume destination is not a directory")
		}
		return nil
	}
	if !errors.Is(err, syscall.ENOENT) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("open managed volume destination: %w", err))
	}
	siblingFD, siblingErr := openDirectoryAt(environmentFD, sibling)
	created := false
	if errors.Is(siblingErr, syscall.ENOENT) {
		if mkdirErr := unix.Mkdirat(environmentFD, sibling, 0o700); mkdirErr != nil &&
			!errors.Is(mkdirErr, syscall.EEXIST) {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("create managed volume private sibling: %w", mkdirErr))
		}
		siblingFD, siblingErr = openDirectoryAt(environmentFD, sibling)
		created = siblingErr == nil
	}
	if siblingErr != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("open managed volume private sibling: %w", siblingErr))
	}
	if created {
		if err := writeExclusiveMarker(siblingFD, marker); err != nil {
			_ = unix.Close(siblingFD)
			return err
		}
	} else if err := verifyMarker(siblingFD, marker); err != nil {
		_ = unix.Close(siblingFD)
		return errs.New(errs.KindValidationFailed, "managed volume private sibling is not task-owned")
	}
	if err := unix.Fchmod(siblingFD, 0o700); err != nil {
		_ = unix.Close(siblingFD)
		return errs.Wrap(errs.KindInternal, fmt.Errorf("secure published managed volume directory: %w", err))
	}
	if err := unix.Fsync(siblingFD); err != nil {
		_ = unix.Close(siblingFD)
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := unix.Close(siblingFD); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := unix.Fsync(environmentFD); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	err = unix.Renameat2(environmentFD, sibling, environmentFD, volume.Key, unix.RENAME_NOREPLACE)
	if errors.Is(err, syscall.EEXIST) {
		leafFD, openErr := openDirectoryAt(environmentFD, volume.Key)
		if openErr != nil {
			return errs.New(
				errs.KindValidationFailed,
				"managed volume destination appeared without task-owned evidence",
			)
		}
		defer unix.Close(leafFD)
		if markerErr := verifyMarker(leafFD, marker); markerErr != nil {
			return errs.New(errs.KindValidationFailed, "managed volume destination is not task-owned")
		}
		if cleanupErr := removePrivateSibling(environmentFD, sibling, marker); cleanupErr != nil {
			return cleanupErr
		}
		return finalizePublishedVolume(leafFD, environmentFD)
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("publish managed volume directory: %w", err))
	}
	leafFD, err = openDirectoryAt(environmentFD, volume.Key)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("reopen published managed volume directory: %w", err))
	}
	defer unix.Close(leafFD)
	if err := verifyMarker(leafFD, marker); err != nil {
		return errs.New(errs.KindInternal, "published managed volume directory lost its ownership marker")
	}
	return finalizePublishedVolume(leafFD, environmentFD)
}
