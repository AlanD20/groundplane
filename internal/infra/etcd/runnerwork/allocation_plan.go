package runnerwork

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The reserved plan survives Task retention even when creation never succeeds.
// It is removed only when the Runner's host allocation is safely released.
func AllocationPlanKey(runnerID string) string {
	return "/v1/runtime/runners/allocation-plan/" + runnerID
}

func EncodeAllocationPlan(plan runnerallocation.RuntimePlan) ([]byte, error) {
	if ids.Validate(ids.KindRunner, plan.RunnerID) != nil || plan.RuntimeEpoch == 0 ||
		plan.AllocationConfig.ValidateAllocation(plan.Allocation) != nil || plan.Digest() == "" {
		return nil, errs.New(errs.KindValidationFailed, "Runner allocation plan is invalid")
	}
	value, err := recordcodec.Encode("runner_allocation_plan", plan)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInputBytes {
		return nil, errs.New(errs.KindValidationFailed, "Runner allocation plan exceeds its bound")
	}
	return value, nil
}

func DecodeAllocationPlan(value []byte) (runnerallocation.RuntimePlan, error) {
	if len(value) > maximumInputBytes {
		return runnerallocation.RuntimePlan{}, errs.New(errs.KindInternal, "Runner allocation plan exceeds its bound")
	}
	plan, err := recordcodec.Decode[runnerallocation.RuntimePlan](value, "runner_allocation_plan")
	if err != nil {
		return runnerallocation.RuntimePlan{}, err
	}
	if _, err := EncodeAllocationPlan(plan); err != nil {
		return runnerallocation.RuntimePlan{}, errs.New(errs.KindInternal, "Runner allocation plan is corrupt")
	}
	return plan.Clone(), nil
}
