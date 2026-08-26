package swarmgit

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/swarmcheck"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (inspector *Inspector) requireWaveResourcesAbsent(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) error {
	refs, err := inspector.git(
		ctx,
		"for-each-ref",
		"--format=%(refname)",
		"--",
		string(swarmcheck.SnapshotNamespace(manifest.Wave)),
	)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(refs.Stdout)) != "" {
		return invalid("complete phase retains a ref beneath the wave namespace")
	}
	worktrees, err := inspector.worktrees(ctx)
	if err != nil {
		return err
	}
	root := swarmcheck.WorktreeRoot(manifest.Wave)
	for value := range worktrees {
		if value == root || strings.HasPrefix(string(value), string(root)+"/") {
			return invalid(fmt.Sprintf("complete phase retains wave worktree %s", value))
		}
	}
	for _, writer := range manifest.Writers {
		if _, exists := worktrees[writer.Worktree]; exists {
			return invalid(
				fmt.Sprintf("complete phase retains declared writer worktree %s", writer.Worktree),
			)
		}
	}
	for _, reserve := range manifest.Reserves {
		if _, exists := worktrees[reserve.Worktree]; exists {
			return invalid(
				fmt.Sprintf("complete phase retains remediation worktree %s", reserve.Worktree),
			)
		}
	}
	for _, writer := range manifest.Writers {
		if err := inspector.requireFilesystemPathAbsent(ctx, writer.Worktree); err != nil {
			return err
		}
	}
	for _, reserve := range manifest.Reserves {
		if err := inspector.requireFilesystemPathAbsent(ctx, reserve.Worktree); err != nil {
			return err
		}
	}
	return inspector.requireFilesystemPathAbsent(ctx, root)
}

func (inspector *Inspector) requireFilesystemPathAbsent(
	ctx context.Context,
	value swarmcheck.RepoPath,
) error {
	if err := ctx.Err(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	absolute := filepath.Join(inspector.Root, filepath.FromSlash(string(value)))
	_, err := os.Lstat(absolute)
	if err == nil {
		return invalid(fmt.Sprintf("complete phase retains temporary filesystem path %s", value))
	}
	if !os.IsNotExist(err) {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
