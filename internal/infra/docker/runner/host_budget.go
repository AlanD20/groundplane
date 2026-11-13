package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

const budgetUnitDirectory = "/run/systemd/system"

func budgetContent(plan corerunner.Plan) []byte {
	return fmt.Appendf(
		nil,
		"[Unit]\nDescription=Groundplane Runner budget %s %s\n[Slice]\nCPUAccounting=yes\nMemoryAccounting=yes\nCPUQuota=%s%%\nMemoryMax=%d\nMemorySwapMax=0\n",
		plan.RunnerID,
		plan.IdentityDigest(),
		strconv.FormatFloat(float64(plan.CPUMillis)/10, 'f', -1, 64),
		plan.MemoryBytes,
	)
}

func budgetPath(plan corerunner.Plan) string {
	return filepath.Join(budgetUnitDirectory, runnerallocation.ResourceSlice(plan.RunnerID))
}

func ownedBudget(plan corerunner.Plan) (bool, error) {
	info, err := os.Lstat(budgetPath(plan))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, errs.Wrap(errs.KindInternal, err)
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o644 || info.Size() > 4096 {
		return false, errs.New(errs.KindStateConflict, "Runner resource slice ownership changed")
	}
	value, err := os.ReadFile(budgetPath(plan))
	if err != nil {
		return false, errs.Wrap(errs.KindInternal, err)
	}
	if !bytes.Equal(value, budgetContent(plan)) {
		return false, errs.New(errs.KindStateConflict, "Runner resource slice differs from its captured budget")
	}
	return true, nil
}

func (operations *localOperations) ensureBudget(ctx context.Context, plan corerunner.Plan) error {
	if err := observeSlotClaim(plan); err != nil {
		return err
	}
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return err
	}
	defer engine.Close()
	info, err := engine.Info(ctx, client.InfoOptions{})
	if err != nil {
		return dockerError(ctx, "read Docker cgroup support", err)
	}
	if info.Info.CgroupDriver != "systemd" || info.Info.CgroupVersion != "2" {
		return errs.New(
			errs.KindValidationFailed,
			"Runner budgets require cgroup v2 and Docker’s systemd cgroup driver",
		)
	}
	if err := ensureOwnedDirectory(budgetUnitDirectory, 0, 0, 0o755); err != nil {
		return err
	}
	found, err := ownedBudget(plan)
	if err != nil {
		return err
	}
	if !found {
		file, err := os.CreateTemp(budgetUnitDirectory, ".groundplane-runner-budget-")
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		defer os.Remove(file.Name())
		_, writeErr := file.Write(budgetContent(plan))
		modeErr := file.Chmod(0o644)
		syncErr := file.Sync()
		closeErr := file.Close()
		if err := errors.Join(writeErr, modeErr, syncErr, closeErr); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if err := os.Link(file.Name(), budgetPath(plan)); err != nil && !errors.Is(err, os.ErrExist) {
			return errs.Wrap(errs.KindInternal, err)
		}
		if _, err := ownedBudget(plan); err != nil {
			return err
		}
	}
	if _, err := operations.run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	if _, err := operations.run(ctx, "systemctl", "start", runnerallocation.ResourceSlice(plan.RunnerID)); err != nil {
		return err
	}
	return operations.observeBudget(ctx, plan)
}

func (operations *localOperations) removeBudget(ctx context.Context, plan corerunner.Plan) error {
	found, err := ownedBudget(plan)
	if err != nil || !found {
		return err
	}
	// A failed start may have published the file before systemd loaded it.
	if _, err := operations.run(ctx, "systemctl", "daemon-reload"); err != nil {
		return err
	}
	values, err := operations.budgetProperties(ctx, plan)
	if err != nil {
		return err
	}
	if group := values["ControlGroup"]; group != "" {
		root, err := budgetCgroup(plan, group)
		if err != nil {
			return err
		}
		events, err := os.ReadFile(filepath.Join(root, "cgroup.events"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return errs.Wrap(errs.KindInternal, err)
		}
		if err == nil && !strings.Contains(string(events), "populated 0\n") {
			return errs.New(errs.KindStateConflict, "Runner processes remain in its resource slice")
		}
	}
	if _, err := operations.run(ctx, "systemctl", "stop", runnerallocation.ResourceSlice(plan.RunnerID)); err != nil {
		return err
	}
	if err := os.Remove(budgetPath(plan)); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	_, err = operations.run(ctx, "systemctl", "daemon-reload")
	return err
}
