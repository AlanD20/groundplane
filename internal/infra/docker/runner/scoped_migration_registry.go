package runner

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/ids"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// scrubLegacyRegistry runs after the owning Runner and its daemon are both
// quiescent. Change only keys for the shared Groundplane registry.
func scrubLegacyRegistry(plan corerunner.Plan) error {
	const filename = "config.json"
	directory := runnerDockerConfig(plan)
	if err := observeSlotClaim(plan); err != nil {
		return err
	}
	slot, err := os.OpenRoot(plan.Paths.SlotRoot)
	if err != nil {
		return errs.Wrap(errs.KindStateConflict, err)
	}
	defer slot.Close()
	relative, err := filepath.Rel(plan.Paths.SlotRoot, directory)
	if err != nil || !filepath.IsLocal(relative) {
		return errs.New(errs.KindStateConflict, "Runner legacy Docker directory escaped its owned slot")
	}
	priorDir, err := slot.Lstat(relative)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !ownedLegacyRegistryPath(priorDir, plan, true) {
		return errs.New(errs.KindStateConflict, "Runner legacy Docker directory ownership changed")
	}
	root, err := slot.OpenRoot(relative)
	if err != nil {
		return errs.Wrap(errs.KindStateConflict, err)
	}
	defer root.Close()
	dir, err := root.Open(".")
	if err != nil {
		return errs.Wrap(errs.KindStateConflict, err)
	}
	defer dir.Close()
	actualDir, err := dir.Stat()
	if err != nil || !os.SameFile(priorDir, actualDir) {
		return errs.New(errs.KindStateConflict, "Runner legacy Docker directory changed")
	}
	file, err := root.OpenFile(filename, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindStateConflict, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !ownedLegacyRegistryPath(info, plan, false) {
		return errs.New(errs.KindStateConflict, "Runner legacy Docker credentials ownership changed")
	}
	const maximumConfig = 64 << 10
	value, err := io.ReadAll(io.LimitReader(file, maximumConfig+1))
	defer clear(value)
	if err != nil || len(value) > maximumConfig {
		return errs.New(errs.KindStateConflict, "Runner legacy Docker configuration cannot be verified")
	}
	next, changed, err := withoutLegacyRegistry(value)
	defer clear(next)
	if err != nil || !changed {
		return err
	}
	stageName := ".scoped-" + ids.NewULID()
	stage, err := root.OpenFile(stageName, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer root.Remove(stageName)
	_, writeErr := stage.Write(next)
	ownerErr := stage.Chown(int(plan.Identity.UID), int(plan.Identity.GID))
	syncErr := stage.Sync()
	closeErr := stage.Close()
	if err := errors.Join(writeErr, ownerErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	live, err := root.Lstat(filename)
	if err != nil || !os.SameFile(info, live) {
		return errs.New(errs.KindStateConflict, "Runner legacy Docker credentials changed during cutover")
	}
	if err := root.Rename(stageName, filename); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := dir.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func ownedLegacyRegistryPath(info os.FileInfo, plan corerunner.Plan, directory bool) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != plan.Identity.UID || stat.Gid != plan.Identity.GID {
		return false
	}
	if directory {
		return info.IsDir() && info.Mode().Perm() == 0o700
	}
	return info.Mode().IsRegular() && info.Mode().Perm() == 0o600 && stat.Nlink == 1
}

func withoutLegacyRegistry(value []byte) ([]byte, bool, error) {
	var config map[string]json.RawMessage
	if err := json.Unmarshal(value, &config); err != nil || config == nil {
		return nil, false, errs.New(errs.KindStateConflict, "Runner legacy Docker configuration cannot be verified")
	}
	defer func() {
		for _, field := range config {
			clear(field)
		}
	}()
	changed := false
	for name, raw := range config {
		if !strings.EqualFold(name, "auths") && !strings.EqualFold(name, "credHelpers") {
			continue
		}
		var table map[string]json.RawMessage
		if err := json.Unmarshal(raw, &table); err != nil {
			return nil, false, errs.New(
				errs.KindStateConflict,
				"Runner legacy registry configuration cannot be verified",
			)
		}
		removed := false
		for key, entry := range table {
			if legacyRegistryKey(key) {
				removed = true
				clear(entry)
				delete(table, key)
			}
		}
		var encodeErr error
		if removed {
			clear(raw)
			config[name], encodeErr = json.Marshal(table)
			changed = true
		}
		for _, entry := range table {
			clear(entry)
		}
		if encodeErr != nil {
			return nil, false, errs.Wrap(errs.KindInternal, encodeErr)
		}
	}
	if !changed {
		return nil, false, nil
	}
	result, err := json.Marshal(config)
	if err != nil {
		return nil, false, errs.Wrap(errs.KindInternal, err)
	}
	return result, true, nil
}
