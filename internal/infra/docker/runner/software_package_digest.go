package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// Digest the predecessor before activation, so replayed recovery can distinguish
// an already-restored entry from a lost backup. Paths are relative to the entry.
func packageDigest(root string) (string, error) {
	hash := sha256.New()
	var total int64
	count := 0
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		count++
		if count > 30000 {
			return errs.New(errs.KindStateConflict, "Runner predecessor has too many package files")
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		owner, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return errs.New(errs.KindStateConflict, "Runner predecessor metadata is unavailable")
		}
		fmt.Fprintf(hash, "%d:%s:%d:%d:%d:%d:", len(relative), relative, info.Mode(), owner.Uid, owner.Gid, info.Size())
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			fmt.Fprintf(hash, "%d:%s", len(target), target)
		case info.IsDir():
		case info.Mode().IsRegular():
			total += info.Size()
			if total > 1<<30 {
				return errs.New(errs.KindStateConflict, "Runner predecessor exceeds its one-GiB capture bound")
			}
			file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			n, copyErr := io.Copy(hash, io.LimitReader(file, info.Size()+1))
			closeErr := file.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if n != info.Size() {
				return errs.New(errs.KindStateConflict, "Runner predecessor changed while being captured")
			}
		default:
			return errs.New(errs.KindStateConflict, "Runner package contains an unsupported special file")
		}
		return nil
	})
	if err != nil {
		return "", errs.Wrap(errs.KindStateConflict, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func predecessorRestored(home string, state softwareJournal) bool {
	for _, entry := range state.Entries {
		path := filepath.Join(home, entry.Name)
		if !entry.Prior {
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				return false
			}
			continue
		}
		digest, err := packageDigest(path)
		if err != nil || digest != entry.PriorDigest {
			return false
		}
	}
	return len(state.Entries) > 0
}
