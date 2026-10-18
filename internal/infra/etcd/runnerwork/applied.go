package runnerwork

import (
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AppliedRuntime belongs to the Runner, not its retained execution history.
// Maintenance and connection checks can keep using it after Task collection.
type AppliedRuntime struct {
	Plan     runnerallocation.RuntimePlan           `json:"plan"`
	Evidence runnerallocation.RunnerRuntimeEvidence `json:"evidence"`
}

func AppliedKey(runnerID string) string { return "/v1/runtime/runners/applied/" + runnerID }

func EncodeApplied(record AppliedRuntime) ([]byte, error) {
	if record.Plan.RuntimeEpoch == 0 || !record.Evidence.Valid() ||
		record.Plan.Container.ImageRef != record.Evidence.Software.Image || record.Plan.Digest() == "" {
		return nil, errs.New(errs.KindValidationFailed, "Runner applied runtime is invalid")
	}
	value, err := recordcodec.Encode("runner_applied_runtime", record)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInputBytes {
		return nil, errs.New(errs.KindValidationFailed, "Runner applied runtime exceeds its storage bound")
	}
	return value, nil
}

func DecodeApplied(value []byte) (AppliedRuntime, error) {
	if len(value) > maximumInputBytes {
		return AppliedRuntime{}, errs.New(errs.KindInternal, "Runner applied runtime exceeds its bound")
	}
	record, err := recordcodec.Decode[AppliedRuntime](value, "runner_applied_runtime")
	if err != nil {
		return AppliedRuntime{}, err
	}
	if _, err := EncodeApplied(record); err != nil {
		return AppliedRuntime{}, errs.New(errs.KindInternal, "Runner applied runtime is corrupt")
	}
	return record, nil
}
