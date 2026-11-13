package runner

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func parseSystemdQuota(value string) (int64, error) {
	duration, err := time.ParseDuration(strings.ReplaceAll(value, " ", ""))
	if err != nil || duration <= 0 || duration%time.Millisecond != 0 {
		return 0, errs.New(errs.KindStateConflict, "Runner CPU quota observation is invalid")
	}
	return duration.Milliseconds(), nil
}

func (operations *localOperations) budgetProperties(
	ctx context.Context,
	plan corerunner.Plan,
) (map[string]string, error) {
	result, err := operations.run(
		ctx,
		"systemctl",
		"show",
		"--property=ActiveState",
		"--property=MemoryMax",
		"--property=CPUQuotaPerSecUSec",
		"--property=MemorySwapMax",
		"--property=ControlGroup",
		runnerallocation.ResourceSlice(plan.RunnerID),
	)
	if err != nil {
		return nil, err
	}
	values := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(string(result.Stdout)), "\n") {
		key, value, found := strings.Cut(line, "=")
		if !found {
			return nil, errs.New(errs.KindStateConflict, "Runner budget observation is invalid")
		}
		values[key] = value
	}
	return values, nil
}

func (operations *localOperations) observeBudget(ctx context.Context, plan corerunner.Plan) error {
	found, err := ownedBudget(plan)
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "Runner resource slice is missing")
	}
	values, err := operations.budgetProperties(ctx, plan)
	if err != nil {
		return err
	}
	// systemctl formats the microsecond property as a duration.
	quota, err := parseSystemdQuota(values["CPUQuotaPerSecUSec"])
	if err != nil || quota != plan.CPUMillis || values["MemoryMax"] != strconv.FormatInt(plan.MemoryBytes, 10) ||
		values["ActiveState"] != "active" || values["MemorySwapMax"] != "0" {
		return errs.New(errs.KindStateConflict, "Runner CPU or memory budget is not active")
	}
	return observeKernelBudget(plan, values["ControlGroup"])
}

func budgetCgroup(plan corerunner.Plan, group string) (string, error) {
	clean := filepath.Clean(group)
	if !strings.HasPrefix(clean, "/groundplane.slice/groundplane-runner.slice/") ||
		filepath.Base(clean) != runnerallocation.ResourceSlice(plan.RunnerID) {
		return "", errs.New(errs.KindStateConflict, "Runner resource cgroup ownership changed")
	}
	return filepath.Join("/sys/fs/cgroup", clean), nil
}

func observeKernelBudget(plan corerunner.Plan, group string) error {
	root, err := budgetCgroup(plan, group)
	if err != nil {
		return err
	}
	cpu, err := os.ReadFile(filepath.Join(root, "cpu.max"))
	if err != nil {
		return errs.Wrap(errs.KindStateConflict, err)
	}
	parts := strings.Fields(string(cpu))
	if len(parts) != 2 {
		return errs.New(errs.KindStateConflict, "Runner CPU controller is unavailable")
	}
	quota, quotaErr := strconv.ParseInt(parts[0], 10, 64)
	period, periodErr := strconv.ParseInt(parts[1], 10, 64)
	if quotaErr != nil || periodErr != nil || quota <= 0 || quota > 1<<40 || period <= 0 || period > 1000000 ||
		quota*1000 != plan.CPUMillis*period {
		return errs.New(errs.KindStateConflict, "Runner kernel CPU quota differs from its budget")
	}
	for name, expected := range map[string]string{
		"memory.max": strconv.FormatInt(plan.MemoryBytes, 10), "memory.swap.max": "0",
	} {
		value, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return errs.Wrap(errs.KindStateConflict, err)
		}
		if strings.TrimSpace(string(value)) != expected {
			return errs.New(errs.KindStateConflict, "Runner kernel memory limit differs from its budget")
		}
	}
	return nil
}
