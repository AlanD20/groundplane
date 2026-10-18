// Package servicelifecyclework owns captured Service start/stop/runtime-removal
// input. It does not resolve current desired state while executing an attempt.
package servicelifecyclework

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumInputBytes = 768 << 10

// Runtime is absent only when admission proved that no applied runtime exists.
// Hook values remain in a private encrypted envelope, not this capture.
type Input struct {
	TaskID          string                                     `json:"task_id"`
	WorkItemID      string                                     `json:"work_item_id"`
	PlanID          string                                     `json:"plan_id"`
	Action          workexecution.Action                       `json:"action"`
	Scope           workexecution.Scope                        `json:"scope"`
	ServiceID       string                                     `json:"service_id"`
	ServiceRevision int64                                      `json:"service_revision"`
	HostID          string                                     `json:"host_id,omitempty"`
	Runtime         *releaserender.ServiceLifecycleRenderInput `json:"runtime,omitempty"`
	HookInputs      *HookInputs                                `json:"hook_inputs,omitempty"`
	CreatedAt       time.Time                                  `json:"created_at"`
}

type HookInputs struct {
	SecretPins       *tasksecretpins.Binding      `json:"secret_pins,omitempty"`
	OperationID      string                       `json:"operation_id"`
	CiphertextSHA256 string                       `json:"ciphertext_sha256"`
	SecretSources    []tasksecretpinrecord.Record `json:"secret_sources,omitempty"`
}

func InputKey(workID string) string { return "/v1/execution/service-lifecycle/inputs/" + workID }

func (input Input) Validate() error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindService, input.ServiceID) != nil ||
		ids.Validate(ids.KindEnvironment, input.Scope.EnvironmentID) != nil || ids.Validate(ids.KindProject, input.Scope.ProjectID) != nil ||
		input.Scope.TenantID != "" && ids.Validate(ids.KindTenant, input.Scope.TenantID) != nil ||
		input.ServiceRevision <= 0 || !recordcodec.IsCanonicalUTC(input.CreatedAt) ||
		(input.Action != workexecution.ActionStart && input.Action != workexecution.ActionStop && input.Action != workexecution.ActionDestroy) {
		return errs.New(errs.KindValidationFailed, "Service lifecycle capture identity is invalid")
	}
	if input.Runtime == nil {
		if input.HostID != "" || input.HookInputs != nil {
			return errs.New(errs.KindValidationFailed, "undeployed Service lifecycle cannot authorize host effects")
		}
		return nil
	}
	runtime := input.Runtime
	if ids.Validate(ids.KindHost, input.HostID) != nil || releaserender.ValidateServiceLifecycleRenderInput(*runtime) != nil ||
		runtime.PlanID != input.PlanID || runtime.ServiceID != input.ServiceID || runtime.EnvironmentID != input.Scope.EnvironmentID ||
		runtime.ProjectID != input.Scope.ProjectID || runtime.TenantID != input.Scope.TenantID ||
		runtime.AcknowledgedRuntime != nil && runtime.AcknowledgedRuntime.Source.Executor.HostID != input.HostID {
		return errs.New(errs.KindValidationFailed, "Service lifecycle runtime differs from its captured target")
	}
	configuration := runtime.HookConfiguration
	if configuration == nil {
		if input.HookInputs != nil {
			return errs.New(errs.KindValidationFailed, "Service lifecycle has hook values without a hook")
		}
		return nil
	}
	definition := configuration.BeforeStop
	if input.Action == workexecution.ActionStart {
		definition = configuration.AfterStart
	}
	if definition == nil || (len(configuration.Inputs) != 0) != (input.HookInputs != nil) {
		return errs.New(errs.KindValidationFailed, "Service lifecycle hook or captured inputs are incomplete")
	}
	if input.HookInputs == nil {
		return nil
	}
	hook := input.HookInputs
	if (len(hook.SecretSources) != 0) != (hook.SecretPins != nil) {
		return errs.New(errs.KindValidationFailed, "Service lifecycle Secret sources require their retained membership")
	}
	if pins := hook.SecretPins; pins != nil && (!pins.MatchesSources(hook.SecretSources) ||
		pins.OperationID != hook.OperationID || pins.TaskID != input.TaskID || pins.WorkItemID != input.WorkItemID) {
		return errs.New(errs.KindValidationFailed, "Service lifecycle Secret retention belongs to another execution")
	}
	if ids.Validate(ids.KindOperation, hook.OperationID) != nil || !recordcodec.ValidSHA256(hook.CiphertextSHA256) {
		return errs.New(errs.KindValidationFailed, "Service lifecycle encrypted hook binding is invalid")
	}
	seen := make(map[string]bool, len(hook.SecretSources))
	for _, pin := range hook.SecretSources {
		if tasksecretpinrecord.Validate(pin) != nil || pin.OperationID != hook.OperationID || seen[pin.SecretID] {
			return errs.New(errs.KindValidationFailed, "Service lifecycle hook Secret ownership is invalid")
		}
		seen[pin.SecretID] = true
	}
	return nil
}

func Encode(input Input) ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("service_lifecycle_work_input", input)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInputBytes {
		clear(value)
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle capture exceeds its storage bound")
	}
	return value, nil
}

func Decode(value []byte) (Input, error) {
	if len(value) > maximumInputBytes {
		return Input{}, errs.New(errs.KindInternal, "Service lifecycle capture exceeds its storage bound")
	}
	input, err := recordcodec.Decode[Input](value, "service_lifecycle_work_input")
	if err != nil || input.Validate() != nil {
		return Input{}, errs.New(errs.KindInternal, "Service lifecycle capture is corrupt")
	}
	return input, nil
}

func (input Input) Reference() (workexecution.Input, error) {
	value, err := Encode(input)
	if err != nil {
		return workexecution.Input{}, err
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	return workexecution.Input{ID: input.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}
