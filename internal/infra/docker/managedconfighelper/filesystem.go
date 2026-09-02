package managedconfighelper

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ensureTrustedDirectory(root *os.Root, value string) error {
	if value == "." {
		return nil
	}
	if err := root.MkdirAll(value, 0o755); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return validateDirectoryChain(root, value)
}

func writeAtomic(
	ctx context.Context,
	root *os.Root,
	target string,
	content []byte,
	mode os.FileMode,
) (resultErr error) {
	parent := path.Dir(target)
	if err := ensureTrustedDirectory(root, parent); err != nil {
		return err
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	temporary := path.Join(parent, "."+path.Base(target)+"."+hex.EncodeToString(nonce[:])+".tmp")
	file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	keep := true
	defer func() {
		if keep {
			_ = root.Remove(temporary)
		}
	}()
	if err := writeAll(ctx, file, content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(mode); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := file.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := root.Rename(temporary, target); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	keep = false
	return syncDirectory(root, parent)
}

func readRegular(root *os.Root, name string) ([]byte, bool, error) {
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errs.Wrap(errs.KindInternal, err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, false, errs.New(errs.KindStateConflict, "managed-config path is not a regular file")
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, false, errs.Wrap(errs.KindInternal, err)
	}
	value, readErr := io.ReadAll(io.LimitReader(file, managedconfig.MaximumArtifactBytes+4097))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		clear(value)
		return nil, false, errs.Wrap(errs.KindInternal, errors.Join(readErr, closeErr))
	}
	return value, true, nil
}

func exists(root *os.Root, name string) bool {
	info, err := root.Lstat(name)
	return err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0
}

func syncDirectory(root *os.Root, directory string) error {
	file, err := root.Open(directory)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil || closeErr != nil {
		return errs.Wrap(errs.KindInternal, errors.Join(syncErr, closeErr))
	}
	return nil
}

func resetTransactionPreparation(root *os.Root, staging string) error {
	info, err := root.Lstat(staging)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errs.New(errs.KindStateConflict, "managed-config preparation is not trusted")
	}
	directory, err := root.Open(staging)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil || closeErr != nil {
		return errs.Wrap(errs.KindInternal, errors.Join(readErr, closeErr))
	}
	for _, entry := range entries {
		entryInfo, infoErr := entry.Info()
		if infoErr != nil || !entryInfo.Mode().IsRegular() || entryInfo.Mode()&os.ModeSymlink != 0 ||
			!validPreparationEntry(entry.Name()) {
			return errs.New(errs.KindStateConflict, "managed-config preparation contains an untrusted entry")
		}
	}
	for _, entry := range entries {
		if err := root.Remove(path.Join(staging, entry.Name())); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
	}
	if err := root.Remove(staging); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return syncDirectory(root, path.Dir(staging))
}

func validPreparationEntry(name string) bool {
	for _, target := range []string{
		"request.pb", "candidate", "previous", "previous.absent", "previous.owner", "previous.owner.absent",
		"orphan.owner",
	} {
		if name == target {
			return true
		}
		prefix := "." + target + "."
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".tmp") {
			continue
		}
		nonce := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".tmp")
		decoded, err := hex.DecodeString(nonce)
		if err == nil && len(decoded) == 8 {
			return true
		}
	}
	return false
}

func validateDirectoryChain(root *os.Root, value string) error {
	current := ""
	for _, element := range strings.Split(value, "/") {
		if current == "" {
			current = element
		} else {
			current += "/" + element
		}
		info, err := root.Lstat(current)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errs.New(errs.KindStateConflict, "managed-config directory is not trusted")
		}
	}
	return nil
}
