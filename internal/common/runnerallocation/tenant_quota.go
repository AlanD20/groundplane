package runnerallocation

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RunnerTenantQuota struct {
	RunnerIDs []string `json:"runner_ids"`
}

func (quota RunnerTenantQuota) Claim(runnerID string) (RunnerTenantQuota, error) {
	if err := quota.Validate(); err != nil {
		return RunnerTenantQuota{}, err
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return RunnerTenantQuota{}, errs.New(errs.KindValidationFailed, "runner id is invalid")
	}
	index := sort.SearchStrings(quota.RunnerIDs, runnerID)
	if index < len(quota.RunnerIDs) && quota.RunnerIDs[index] == runnerID {
		return RunnerTenantQuota{RunnerIDs: append([]string(nil), quota.RunnerIDs...)}, nil
	}
	if len(quota.RunnerIDs) >= MaximumTenantRunners {
		return RunnerTenantQuota{}, errs.New(errs.KindResourceInUse, "tenant already owns the maximum of five runners")
	}
	next := RunnerTenantQuota{RunnerIDs: append([]string(nil), quota.RunnerIDs...)}
	next.RunnerIDs = append(next.RunnerIDs, runnerID)
	sort.Strings(next.RunnerIDs)
	return next, nil
}

func (quota RunnerTenantQuota) Release(runnerID string) (RunnerTenantQuota, error) {
	if err := quota.Validate(); err != nil {
		return RunnerTenantQuota{}, err
	}
	index := sort.SearchStrings(quota.RunnerIDs, runnerID)
	if index >= len(quota.RunnerIDs) || quota.RunnerIDs[index] != runnerID {
		return RunnerTenantQuota{}, errs.New(
			errs.KindStateConflict,
			"runner tenant quota slot does not match its owner",
		)
	}
	next := RunnerTenantQuota{RunnerIDs: make([]string, 0, len(quota.RunnerIDs)-1)}
	next.RunnerIDs = append(next.RunnerIDs, quota.RunnerIDs[:index]...)
	next.RunnerIDs = append(next.RunnerIDs, quota.RunnerIDs[index+1:]...)
	return next, nil
}

func (quota RunnerTenantQuota) Validate() error {
	if len(quota.RunnerIDs) > MaximumTenantRunners {
		return corruptRunnerTenantQuota()
	}
	for index, runnerID := range quota.RunnerIDs {
		if ids.Validate(ids.KindRunner, runnerID) != nil ||
			(index > 0 && quota.RunnerIDs[index-1] >= runnerID) {
			return corruptRunnerTenantQuota()
		}
	}
	return nil
}

func corruptRunnerTenantQuota() error {
	return errs.New(errs.KindInternal, "runner tenant quota is corrupt")
}
