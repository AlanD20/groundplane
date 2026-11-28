package runner

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const (
	runnerCredentialDirectory = "/opt/groundplane-runner-credentials"
	runnerCredentialName      = "token"
	runnerCredentialFile      = runnerCredentialDirectory + "/" + runnerCredentialName
	runnerCredentialPrefix    = "gpr1_"
	runnerCredentialBytes     = 32
	runnerCredentialLength    = len(runnerCredentialPrefix) + 43
)

// InstallScopedCredential publishes an already authorized Runner bearer on its
// claimed host slot. The caller must serialize issuance and fence it against
// the durable Runner grant. This method consumes token whether it succeeds or fails.
func (control *HostControl) InstallScopedCredential(ctx context.Context, plan corerunner.Plan, token []byte) error {
	defer clear(token)
	if err := validateCall(ctx, plan); err != nil {
		return err
	}
	if control.local == nil {
		return errs.New(errs.KindStateConflict, "Runner credential installation requires the local host boundary")
	}
	if !plan.Egress.ScopedEndpoint.IsValid() || !validScopedToken(token) {
		return errs.New(errs.KindValidationFailed, "Scoped Runner credential is invalid")
	}
	if err := observeSlotClaim(plan); err != nil {
		return err
	}
	directory := scopedCredentialSource(plan)
	if err := ensureOwnedDirectory(directory, 0, plan.Identity.GID, 0o710); err != nil {
		return err
	}
	if err := publishScopedFile(directory, plan, runnerCredentialName, token); err != nil {
		return err
	}
	return requireScopedCredential(plan)
}

func scopedCredentialSource(plan corerunner.Plan) string {
	return filepath.Join(plan.Paths.SlotRoot, "scoped-credential")
}

func publishScopedFile(directory string, plan corerunner.Plan, name string, content []byte) error {
	target := filepath.Join(directory, name)
	if info, err := os.Lstat(target); err == nil {
		if !ownedScopedFile(info, plan.Identity.GID) {
			return errs.New(errs.KindStateConflict, "Scoped Runner file ownership changed")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, err)
	}
	stage, err := os.CreateTemp(directory, ".credential-")
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer os.Remove(stage.Name())
	_, writeErr := stage.Write(content)
	ownerErr := stage.Chown(0, int(plan.Identity.GID))
	modeErr := stage.Chmod(0o440)
	syncErr := stage.Sync()
	closeErr := stage.Close()
	if err := errors.Join(writeErr, ownerErr, modeErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Rename(stage.Name(), target); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	for _, path := range []string{directory, plan.Paths.SlotRoot} {
		root, err := os.Open(path)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if err := errors.Join(root.Sync(), root.Close()); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
	}
	return nil
}

func requireScopedCredential(plan corerunner.Plan) error {
	if !plan.Egress.ScopedEndpoint.IsValid() {
		return nil
	}
	if err := observeSlotClaim(plan); err != nil {
		return err
	}
	invalid := errs.New(errs.KindStateConflict, "Scoped Runner credential must be installed before activation")
	directory := scopedCredentialSource(plan)
	if err := observeOwnedDirectory(directory, 0, plan.Identity.GID, 0o710); err != nil {
		return invalid
	}
	file, err := os.OpenFile(
		filepath.Join(directory, runnerCredentialName),
		os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return invalid
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !ownedScopedFile(info, plan.Identity.GID) {
		return invalid
	}
	var token [runnerCredentialLength + 1]byte
	defer clear(token[:])
	count, err := io.ReadFull(file, token[:])
	if !errors.Is(err, io.ErrUnexpectedEOF) || !validScopedToken(token[:count]) {
		return invalid
	}
	return nil
}

func ownedScopedFile(info os.FileInfo, gid uint32) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && info.Mode().Perm() == 0o440 && stat.Uid == 0 && stat.Gid == gid &&
		stat.Nlink == 1
}

func validScopedToken(value []byte) bool {
	if len(value) != runnerCredentialLength || !bytes.HasPrefix(value, []byte(runnerCredentialPrefix)) {
		return false
	}
	var decoded [runnerCredentialBytes]byte
	defer clear(decoded[:])
	size, err := base64.RawURLEncoding.Strict().Decode(decoded[:], value[len(runnerCredentialPrefix):])
	return err == nil && size == len(decoded)
}
