// Package runnerwork owns captured Runner lifecycle execution input.
package runnerwork

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumInputBytes = 64 << 10

// Input is immutable across retries. Registration tokens never enter this record.
// The complete runtime plan is selected before publication, not by the executor.
type Input struct {
	TaskID     string                            `json:"task_id"`
	WorkItemID string                            `json:"work_item_id"`
	PlanID     string                            `json:"plan_id"`
	Operation  runnerallocation.RuntimeOperation `json:"operation"`
	Desired    runners.RunnerDesiredRecord       `json:"desired"`
	Plan       runnerallocation.RuntimePlan      `json:"plan"`
	CreatedAt  time.Time                         `json:"created_at"`
	Removal    *RemovalSource                    `json:"removal,omitempty"`
}

type RemovalSource struct {
	DesiredRevision   int64                                 `json:"desired_revision"`
	LifecycleRevision int64                                 `json:"lifecycle_revision"`
	Lifecycle         runners.RunnerLifecycleRecord         `json:"lifecycle"`
	Ownership         *runners.RunnerRuntimeOwnershipRecord `json:"ownership,omitempty"`
}

func InputKey(workID string) string     { return "/v1/execution/runner/inputs/" + workID }
func TaskInputKey(taskID string) string { return "/v1/execution/runner/tasks/" + taskID }

func (input Input) Validate() error {
	plan, desired := input.Plan, input.Desired
	if ids.Validate(ids.KindTask, input.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil ||
		runners.ValidateRunnerDesired(desired) != nil ||
		!recordcodec.IsCanonicalUTC(input.CreatedAt) ||
		(input.Operation != runnerallocation.RuntimeOperationCreate && input.Operation != runnerallocation.RuntimeOperationRemove) ||
		plan.RunnerID != desired.ID || plan.TenantID != desired.TenantID ||
		plan.OwnerKind != string(desired.OwnerKind) || plan.OwnerID != desired.OwnerID ||
		plan.Container.ImageRef != desired.ImageRef || plan.Container.GitHubURL != desired.GitHubURL ||
		input.Operation == runnerallocation.RuntimeOperationCreate && !slices.Equal(plan.Container.Labels, desired.Labels) || plan.RuntimeEpoch == 0 ||
		plan.AllocationConfig.ValidateAllocation(plan.Allocation) != nil || plan.Digest() == "" {
		return errs.New(errs.KindValidationFailed, "Runner execution input is invalid")
	}
	if input.Operation == runnerallocation.RuntimeOperationCreate {
		if input.Removal != nil {
			return errs.New(errs.KindValidationFailed, "Runner creation contains removal authority")
		}
	} else {
		source := input.Removal
		if source == nil || source.DesiredRevision <= 0 || source.LifecycleRevision <= 0 ||
			runners.ValidateRunnerLifecycle(source.Lifecycle) != nil || source.Lifecycle.RunnerID != desired.ID ||
			source.Lifecycle.ProvisioningState == runners.RunnerProvisioningProvisioning ||
			source.Lifecycle.Allocation != plan.Allocation || source.Lifecycle.RuntimeEpoch > plan.RuntimeEpoch {
			return errs.New(errs.KindValidationFailed, "Runner removal source is invalid")
		}
		if source.Ownership == nil {
			if source.Lifecycle.ContainerID != "" || source.Lifecycle.ProvisioningState == runners.RunnerProvisioningReady {
				return errs.New(errs.KindValidationFailed, "Runner removal lost runtime ownership")
			}
		} else if runners.ValidateRunnerRuntimeOwnership(*source.Ownership) != nil || source.Ownership.RunnerID != desired.ID ||
			source.Ownership.RuntimeEpoch != plan.RuntimeEpoch || source.Lifecycle.RuntimeEpoch != plan.RuntimeEpoch || source.Lifecycle.ContainerID == "" {
			return errs.New(errs.KindValidationFailed, "Runner removal runtime ownership differs from its captured plan")
		}
	}
	return nil
}

func (input Input) Reference() (workexecution.Input, error) {
	value, err := Encode(input)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(value)
	return workexecution.Input{ID: input.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func Encode(input Input) ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("runner_execution_input", input)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInputBytes {
		return nil, errs.New(errs.KindResourceInUse, "Runner execution input exceeds its storage bound")
	}
	return value, nil
}

func Decode(value []byte) (Input, error) {
	if len(value) > maximumInputBytes {
		return Input{}, errs.New(errs.KindInternal, "Runner execution input exceeds its storage bound")
	}
	input, err := recordcodec.Decode[Input](value, "runner_execution_input")
	if err != nil || input.Validate() != nil {
		return Input{}, errs.New(errs.KindInternal, "Runner execution input is corrupt")
	}
	return input, nil
}

func PreparePublication(input Input) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	value, err := Encode(input)
	if err != nil {
		return nil, nil, err
	}
	key := InputKey(input.WorkItemID)
	return []keyvalue.Condition{{Key: key}, {Key: TaskInputKey(input.TaskID)}}, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: key, Value: value},
		{Type: keyvalue.MutationPut, Key: TaskInputKey(input.TaskID), Value: []byte(input.WorkItemID)},
	}, nil
}

// Clone isolates the captured input from caller-owned slices.
func (input Input) Clone() Input {
	input.Desired.Labels = slices.Clone(input.Desired.Labels)
	input.Plan = input.Plan.Clone()
	if input.Removal != nil {
		copy := *input.Removal
		if copy.Ownership != nil {
			ownership := *copy.Ownership
			copy.Ownership = &ownership
		}
		input.Removal = &copy
	}
	return input
}
