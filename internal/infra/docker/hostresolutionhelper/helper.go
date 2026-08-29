// Package hostresolutionhelper owns the fixed host resolver materialization
// procedure executed inside a short-lived helper container.
package hostresolutionhelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	ResolverSourcePath = "/etc/resolv.conf"
	ResolverMountPath  = "/run/groundplane/host-resolver/resolv.conf"
	StateDirectory     = agentprotocol.StatePath + "/host-resolution"
	BaselinePath       = StateDirectory + "/resolv.conf.previous"
	maximumBytes       = 64 * 1024
)

var managedResolver = []byte("nameserver 127.0.0.1\n")

func Apply(ctx context.Context) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := ensureStateDirectory(); err != nil {
		return err
	}
	baseline, found, err := readBounded(BaselinePath)
	if err != nil {
		return err
	}
	if found {
		defer clear(baseline)
		if _, err := componentdns.ParseResolverBaseline(baseline); err != nil {
			return errs.New(errs.KindStateConflict, "host resolver baseline is invalid")
		}
	} else {
		baseline, _, err = readBounded(ResolverMountPath)
		if err != nil {
			return err
		}
		defer clear(baseline)
		if _, err := componentdns.ParseResolverBaseline(baseline); err != nil {
			return errs.New(errs.KindStateConflict, "host resolver cannot be claimed from its current content")
		}
		if err := writeAtomic(BaselinePath, baseline, 0o600); err != nil {
			return err
		}
	}
	return writeMountedResolver(ctx, managedResolver)
}

func Restore(ctx context.Context) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	baseline, found, err := readBounded(BaselinePath)
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "host resolver baseline is missing")
	}
	defer clear(baseline)
	if _, err := componentdns.ParseResolverBaseline(baseline); err != nil {
		return errs.New(errs.KindStateConflict, "host resolver baseline is invalid")
	}
	if err := writeMountedResolver(ctx, baseline); err != nil {
		return err
	}
	if err := os.Remove(BaselinePath); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("host resolution helper: remove restored baseline: %w", err))
	}
	return syncDirectory(StateDirectory)
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "host resolution helper context is required")
	}
	return ctx.Err()
}

func ensureStateDirectory() error {
	if err := os.MkdirAll(StateDirectory, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("host resolution helper: create state directory: %w", err))
	}
	info, err := os.Lstat(StateDirectory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errs.New(errs.KindStateConflict, "host resolution helper state directory is not trusted")
	}
	if err := os.Chmod(StateDirectory, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func readBounded(path string) ([]byte, bool, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errs.Wrap(errs.KindInternal, fmt.Errorf("host resolution helper: open file: %w", err))
	}
	content, readErr := io.ReadAll(io.LimitReader(file, maximumBytes+1))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		clear(content)
		return nil, false, errs.Wrap(errs.KindInternal, err)
	}
	if len(content) == 0 || len(content) > maximumBytes {
		clear(content)
		return nil, false, errs.New(errs.KindStateConflict, "host resolver file size is invalid")
	}
	return content, true, nil
}

func writeMountedResolver(ctx context.Context, content []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := os.OpenFile(ResolverMountPath, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("host resolution helper: open resolver: %w", err))
	}
	writeErr := writeAll(file, content)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("host resolution helper: write resolver: %w", err))
	}
	return nil
}

func writeAtomic(target string, content []byte, mode os.FileMode) error {
	directory := filepath.Dir(target)
	temporary, err := os.CreateTemp(directory, ".resolver-*.tmp")
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	name := temporary.Name()
	keep := true
	defer func() {
		if keep {
			_ = os.Remove(name)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		_ = temporary.Close()
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := writeAll(temporary, content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := temporary.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Rename(name, target); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	keep = false
	return syncDirectory(directory)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	err = errors.Join(directory.Sync(), directory.Close())
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func writeAll(destination io.Writer, content []byte) error {
	for len(content) != 0 {
		written, err := destination.Write(content)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if written <= 0 {
			return errs.New(errs.KindInternal, "host resolution helper writer made no progress")
		}
		content = content[written:]
	}
	return nil
}
