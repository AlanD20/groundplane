// materializer.go: Agent-side writes to the host filesystem — env files
// at 0600, and the /etc/resolv.conf rewrite. "Everything host-level is
// actioned by the Agent (locked)" — no host-level hands anywhere but the
// Agent's. See mvp.md. Every function here does file I/O, so ctx is
// first per docs/standards.md, section 4.
package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// MaterializeEnvFile writes an env file at 0600 — the final step of
// applying a service's env_files. Never logs the values (see the
// unified-logging lock: never log secret values).
func MaterializeEnvFile(ctx context.Context, path string, entries map[string]string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("materializer: mkdir: %w", err))
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("materializer: open: %w", err))
	}
	defer f.Close()
	for k, v := range entries {
		if _, err := fmt.Fprintf(f, "%s=%s\n", k, v); err != nil {
			return errs.Wrap(errs.CodeInternal, fmt.Errorf("materializer: write: %w", err))
		}
	}
	return nil
}

// MaterializeFileSecret writes a file secret at 0600, at a path always
// relative to the environment's volume folder (never escapes it — see
// mvp.md, "File secret paths are volume-relative (locked)").
func MaterializeFileSecret(ctx context.Context, volumeDir, relPath string, content []byte) error {
	full := filepath.Join(volumeDir, relPath)
	if !isWithin(volumeDir, full) {
		return errs.Newf(errs.CodeValidationFailed, "materializer: path %q escapes volume_dir %q", relPath, volumeDir)
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("materializer: mkdir: %w", err))
	}
	if err := os.WriteFile(full, content, 0o600); err != nil {
		return errs.Wrap(errs.CodeInternal, fmt.Errorf("materializer: write: %w", err))
	}
	return nil
}

func isWithin(base, target string) bool {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false
	}
	return rel != ".." && !hasDotDotPrefix(rel)
}

func hasDotDotPrefix(rel string) bool {
	return len(rel) >= 2 && rel[0] == '.' && rel[1] == '.'
}

// RewriteResolvConf points /etc/resolv.conf at 127.0.0.1 as the final
// step of applying the DNS (CoreDNS) service, and restores the previous
// content if CoreDNS is removed.
//
// TODO: back up the previous /etc/resolv.conf content the first time
// this runs, so removal can restore it exactly (see mvp.md, "DNS
// resolver (locked)").
func RewriteResolvConf(ctx context.Context, pointAtLocalhost bool) error {
	return errs.New(errs.CodeNotImplemented, "materializer: RewriteResolvConf not implemented")
}
