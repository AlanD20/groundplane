package runner

import (
	"context"
	"errors"
	"fmt"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

func (operations *localOperations) EnsureIdentity(ctx context.Context, plan corerunner.Plan) error {
	if err := operations.ensureGroup(ctx, plan); err != nil {
		return err
	}
	if err := operations.ensureUser(ctx, plan); err != nil {
		return err
	}
	if err := ensureSubordinateRange("/etc/subuid", plan.Identity.User, plan.Identity.SubUIDStart, plan.Identity.SubUIDCount); err != nil {
		return err
	}
	if err := ensureSubordinateRange("/etc/subgid", plan.Identity.User, plan.Identity.SubGIDStart, plan.Identity.SubGIDCount); err != nil {
		return err
	}
	if _, err := operations.run(ctx, "loginctl", "enable-linger", plan.Identity.User); err != nil {
		return err
	}
	if err := ensureOwnedDirectory(plan.Paths.SlotRoot, 0, 0, 0o700); err != nil {
		return err
	}
	for _, path := range []string{
		plan.Paths.RunnerHome,
		plan.Paths.WorkRoot,
		plan.Paths.DataRoot,
		filepath.Join(plan.Paths.SlotRoot, "control"),
		filepath.Dir(plan.Paths.ProxySocket),
	} {
		if err := ensureOwnedDirectory(path, plan.Identity.UID, plan.Identity.GID, 0o700); err != nil {
			return err
		}
	}
	return nil
}

func (operations *localOperations) ObserveIdentity(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	if err := operations.identityMatches(ctx, plan); err != nil {
		return corerunner.StepEvidence{}, err
	}
	return applied(corerunner.StepEnsureIdentity), nil
}

func (operations *localOperations) RemoveIdentity(ctx context.Context, plan corerunner.Plan) (string, error) {
	_, _ = operations.run(ctx, "loginctl", "disable-linger", plan.Identity.User)
	if _, err := operations.run(ctx, "userdel", plan.Identity.User); err != nil {
		result, probeErr := operations.run(ctx, "getent", "passwd", plan.Identity.User)
		if probeErr == nil || result.ExitCode == 0 {
			return "", err
		}
	}
	if _, err := operations.run(ctx, "groupdel", plan.Identity.Group); err != nil {
		result, probeErr := operations.run(ctx, "getent", "group", plan.Identity.Group)
		if probeErr == nil || result.ExitCode == 0 {
			return "", err
		}
	}
	if err := os.RemoveAll(plan.Paths.SlotRoot); err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return receipt(plan, corerunner.StepRemoveIdentity), nil
}

func (operations *localOperations) ObserveIdentityAbsent(
	ctx context.Context,
	plan corerunner.Plan,
) (corerunner.StepEvidence, error) {
	if result, err := operations.run(ctx, "getent", "passwd", plan.Identity.User); err == nil || result.ExitCode == 0 {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner host user still exists")
	}
	if _, err := os.Lstat(plan.Paths.SlotRoot); err == nil || !errors.Is(err, os.ErrNotExist) {
		return corerunner.StepEvidence{}, errs.New(errs.KindStateConflict, "Runner slot root still exists")
	}
	return absent(plan, corerunner.StepRemoveIdentity), nil
}

func (operations *localOperations) ensureGroup(ctx context.Context, plan corerunner.Plan) error {
	result, err := operations.run(ctx, "getent", "group", plan.Identity.Group)
	if err != nil {
		if result.ExitCode != 2 {
			return err
		}
		_, err = operations.run(
			ctx,
			"groupadd",
			"--gid",
			strconv.FormatUint(uint64(plan.Identity.GID), 10),
			plan.Identity.Group,
		)
		return err
	}
	fields := strings.Split(strings.TrimSpace(string(result.Stdout)), ":")
	if len(fields) < 3 || fields[0] != plan.Identity.Group ||
		fields[2] != strconv.FormatUint(uint64(plan.Identity.GID), 10) {
		return errs.New(errs.KindStateConflict, "Runner host group identity changed")
	}
	return nil
}

func (operations *localOperations) ensureUser(ctx context.Context, plan corerunner.Plan) error {
	result, err := operations.run(ctx, "getent", "passwd", plan.Identity.User)
	if err != nil {
		if result.ExitCode != 2 {
			return err
		}
		_, err = operations.run(
			ctx, "useradd", "--uid", strconv.FormatUint(uint64(plan.Identity.UID), 10),
			"--gid", plan.Identity.Group, "--home-dir", plan.Paths.RunnerHome,
			"--no-create-home", "--shell", "/usr/sbin/nologin", plan.Identity.User,
		)
		return err
	}
	return validatePasswd(result.Stdout, plan)
}

func (operations *localOperations) identityMatches(ctx context.Context, plan corerunner.Plan) error {
	group, err := operations.run(ctx, "getent", "group", plan.Identity.Group)
	if err != nil {
		return err
	}
	groupFields := strings.Split(strings.TrimSpace(string(group.Stdout)), ":")
	if len(groupFields) < 3 || groupFields[2] != strconv.FormatUint(uint64(plan.Identity.GID), 10) {
		return errs.New(errs.KindStateConflict, "Runner host group identity changed")
	}
	user, err := operations.run(ctx, "getent", "passwd", plan.Identity.User)
	if err != nil {
		return err
	}
	if err := validatePasswd(user.Stdout, plan); err != nil {
		return err
	}
	if !hasSubordinateRange("/etc/subuid", plan.Identity.User, plan.Identity.SubUIDStart, plan.Identity.SubUIDCount) ||
		!hasSubordinateRange("/etc/subgid", plan.Identity.User, plan.Identity.SubGIDStart, plan.Identity.SubGIDCount) {
		return errs.New(errs.KindStateConflict, "Runner subordinate identity changed")
	}
	return observeOwnedDirectory(plan.Paths.RunnerHome, plan.Identity.UID, plan.Identity.GID, 0o700)
}

func ensureOwnedDirectory(path string, uid uint32, gid uint32, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Chown(path, int(uid), int(gid)); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return observeOwnedDirectory(path, uid, gid, mode)
}

func observeOwnedDirectory(path string, uid uint32, gid uint32, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm() != mode || stat.Uid != uid || stat.Gid != gid {
		return errs.New(errs.KindStateConflict, "Runner owned directory identity changed")
	}
	return nil
}

func ensureSubordinateRange(path string, user string, start uint32, count uint32) error {
	if hasSubordinateRange(path, user, start, count) {
		return nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	line := fmt.Sprintf("%s:%d:%d\n", user, start, count)
	_, writeErr := file.WriteString(line)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return errs.Wrap(errs.KindInternal, errors.Join(writeErr, syncErr, closeErr))
	}
	return nil
}

func hasSubordinateRange(path string, user string, start uint32, count uint32) bool {
	content, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	want := fmt.Sprintf("%s:%d:%d", user, start, count)
	for _, line := range strings.Split(strings.TrimSpace(string(content)), "\n") {
		if line == want {
			return true
		}
	}
	return false
}

func validatePasswd(value []byte, plan corerunner.Plan) error {
	fields := strings.Split(strings.TrimSpace(string(value)), ":")
	if len(fields) < 7 || fields[0] != plan.Identity.User ||
		fields[2] != strconv.FormatUint(uint64(plan.Identity.UID), 10) ||
		fields[3] != strconv.FormatUint(uint64(plan.Identity.GID), 10) ||
		fields[5] != plan.Paths.RunnerHome || fields[6] != "/usr/sbin/nologin" {
		return errs.New(errs.KindStateConflict, "Runner host user identity changed")
	}
	return nil
}
