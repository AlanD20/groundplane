package softwarepreparation

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Reconciliation struct {
	Cleaned  bool
	Verified bool
}

// Reconcile retires only this captured operation's builder, client and scratch.
// It may verify already-published outputs, but never downloads, builds or pushes
// again. The caller must exclude the previous executing process first.
func (preparer *Preparer) Reconcile(ctx context.Context, input Input, checkpoint Result) (Reconciliation, error) {
	if err := ValidateInput(input); err != nil {
		return Reconciliation{}, err
	}
	if err := ValidateResult(checkpoint, input.Source, true); err != nil {
		return Reconciliation{}, err
	}
	workspace := filepath.Join(preparer.workspaceRoot, ".software-preparation-"+input.OperationID)
	info, err := os.Lstat(workspace)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Reconciliation{}, errs.Wrap(errs.KindInternal, err)
	}
	if err == nil {
		status, ok := info.Sys().(*syscall.Stat_t)
		if !info.IsDir() || info.Mode().Perm() != 0o700 || !ok || status.Uid != uint32(os.Geteuid()) {
			return Reconciliation{}, errs.New(errs.KindStateConflict, "software preparation cleanup workspace ownership is unsafe")
		}
		if err := staleDockerOperation(preparer, workspace, input.OperationID, input.Source).cleanup(ctx); err != nil {
			return Reconciliation{}, err
		}
		if err := os.RemoveAll(workspace); err != nil {
			return Reconciliation{}, errs.Wrap(errs.KindInternal, err)
		}
	}
	result := Reconciliation{Cleaned: true}
	if ValidateResult(checkpoint, input.Source, false) != nil {
		return result, nil
	}
	if err := preparer.verifyCheckpoint(ctx, checkpoint); err != nil {
		return result, err
	}
	result.Verified = true
	return result, nil
}
