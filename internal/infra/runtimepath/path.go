// Package runtimepath owns secure traversal and preparation of
// Controller-managed runtime directories.
package runtimepath

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareDirectory validates every component in relative, optionally creates
// missing components, and applies mode 0700 from managedFrom onward. Earlier
// components are trusted host ancestors whose modes must remain unchanged.
func PrepareDirectory(
	ctx context.Context,
	root *os.Root,
	relative string,
	expectedUID uint32,
	managedFrom int,
	create bool,
) (bool, error) {
	components := strings.Split(relative, "/")
	if relative == "" || strings.HasPrefix(relative, "/") || managedFrom < 0 || managedFrom >= len(components) {
		return false, errs.New(errs.CodeInternal, "runtime path: invalid directory policy")
	}

	current := ""
	for index, component := range components {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if component == "" || component == "." || component == ".." {
			return false, errs.New(errs.CodeInternal, "runtime path: invalid directory component")
		}
		current = path.Join(current, component)
		info, err := root.Lstat(current)
		if errors.Is(err, fs.ErrNotExist) {
			if !create {
				return false, nil
			}
			if err := root.Mkdir(current, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
				return false, errs.Wrap(errs.CodeInternal, fmt.Errorf("runtime path: create directory: %w", err))
			}
			info, err = root.Lstat(current)
		}
		if err != nil {
			return false, errs.Wrap(errs.CodeInternal, fmt.Errorf("runtime path: inspect directory: %w", err))
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return false, errs.New(errs.CodeInternal, "runtime path: component is not a real directory")
		}
		if err := ValidateOwnership(info, expectedUID, "directory component"); err != nil {
			return false, err
		}
		if create && index >= managedFrom {
			if err := root.Chmod(current, 0o700); err != nil {
				return false, errs.Wrap(errs.CodeInternal, fmt.Errorf("runtime path: set managed directory mode: %w", err))
			}
		}
	}
	return true, nil
}

func ValidateOwnership(info fs.FileInfo, expectedUID uint32, label string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errs.Newf(errs.CodeInternal, "runtime path: cannot determine %s ownership", label)
	}
	if stat.Uid != expectedUID {
		return errs.Newf(errs.CodeInternal, "runtime path: %s is owned by uid %d, want %d", label, stat.Uid, expectedUID)
	}
	return nil
}

func SyncDirectory(ctx context.Context, root *os.Root) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	directory, err := root.Open(".")
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("runtime path: open directory for sync: %w", err))
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("runtime path: sync directory: %w", err))
	}
	return nil
}

func RootedName(absolute string) string {
	return strings.TrimPrefix(absolute, "/")
}
