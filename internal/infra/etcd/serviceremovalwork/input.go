// Package serviceremovalwork owns immutable inputs for Service removal. Runtime
// evidence and the desired revision survive independently of diagnostic history.
package serviceremovalwork

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumInputBytes = 768 << 10

// Input identifies the immutable staged desired input, not the latest head.
// DesiredSHA256 is the seal's ProjectionSHA256 (canonical desired bytes).
// Blueprint work belongs directly to its visible Apply Task; no child Task is
// created. Direct removal owns a separate visible Remove Task.
type Input struct {
	TaskID                string                          `json:"task_id"`
	WorkItemID            string                          `json:"work_item_id"`
	PlanID                string                          `json:"plan_id"`
	Scope                 workexecution.Scope             `json:"scope"`
	ServiceID             string                          `json:"service_id"`
	ServiceName           string                          `json:"service_name"`
	ServiceRevision       int64                           `json:"service_revision"`
	RuntimeRevision       int64                           `json:"runtime_revision"`
	DesiredRevisionID     string                          `json:"desired_revision_id"`
	DesiredSHA256         string                          `json:"desired_sha256"`
	BaseRevisionID        string                          `json:"base_revision_id"`
	HeadRevision          int64                           `json:"head_revision"`
	Generation            uint64                          `json:"generation"`
	HostID                string                          `json:"host_id,omitempty"`
	Runtime               *serviceruntimerecord.Record    `json:"runtime,omitempty"`
	RuntimeSourceRevision int64                           `json:"runtime_source_revision"`
	Configuration         *runtimeconfiguration.Reference `json:"configuration,omitempty"`
	ConfigurationRevision int64                           `json:"configuration_revision"`
	ConfigurationPins     *tasksecretpins.Binding         `json:"configuration_pins,omitempty"`
	CreatedAt             time.Time                       `json:"created_at"`
}

func InputKey(workID string) string { return "/v1/execution/service-removal/inputs/" + workID }

func (input Input) Validate() error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindService, input.ServiceID) != nil ||
		input.Scope.Validate() != nil || input.Scope.EnvironmentID == "" || input.ServiceName == "" ||
		input.ServiceRevision <= 0 || input.RuntimeRevision < 0 || input.RuntimeSourceRevision < 0 ||
		ids.Validate(ids.KindTask, input.DesiredRevisionID) != nil || !recordcodec.ValidSHA256(input.DesiredSHA256) ||
		ids.Validate(ids.KindTask, input.BaseRevisionID) != nil ||
		input.HeadRevision < 0 || input.Generation == 0 || !recordcodec.IsCanonicalUTC(input.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Service removal capture identity is invalid")
	}
	if input.ConfigurationRevision < 0 || (input.Configuration == nil) != (input.ConfigurationRevision == 0) {
		return errs.New(errs.KindValidationFailed, "Service removal configuration capture is incomplete")
	}
	if pins := input.ConfigurationPins; pins != nil && (pins.Validate() != nil ||
		input.Configuration == nil || pins.TaskID != input.TaskID || pins.WorkItemID != input.WorkItemID ||
		pins.OperationID != input.ConfigurationOperationID()) {
		return errs.New(
			errs.KindValidationFailed,
			"Service removal Secret retention differs from its captured configuration owner",
		)
	}
	if input.Configuration != nil && (runtimeconfiguration.ValidateReference(*input.Configuration) != nil ||
		input.Configuration.EnvironmentID != input.Scope.EnvironmentID || input.Configuration.Generation > input.Generation) {
		return errs.New(errs.KindValidationFailed, "Service removal configuration differs from its Environment")
	}
	if input.Runtime == nil {
		if input.RuntimeSourceRevision != 0 || input.Configuration != nil ||
			input.HostID != "" && ids.Validate(ids.KindHost, input.HostID) != nil {
			return errs.New(
				errs.KindValidationFailed,
				"Service removal without runtime cannot authorize host execution",
			)
		}
		return nil
	}
	if input.Runtime.Configuration != nil && input.Configuration == nil {
		return errs.New(
			errs.KindValidationFailed,
			"Service removal must capture current configuration before removing a configured runtime",
		)
	}
	if input.RuntimeSourceRevision == 0 || serviceruntimerecord.Validate(*input.Runtime) != nil ||
		input.Runtime.EnvironmentID != input.Scope.EnvironmentID || input.Runtime.Runtime.ServiceID != input.ServiceID ||
		input.Runtime.Source.Executor.HostID != input.HostID || ids.Validate(ids.KindHost, input.HostID) != nil {
		return errs.New(errs.KindValidationFailed, "Service removal runtime differs from its captured target")
	}
	return nil
}

func (input Input) ConfigurationOperationID() string {
	return ids.DeriveAt(ids.KindOperation, input.CreatedAt, input.TaskID, "removal-configuration/"+input.WorkItemID)
}

func Encode(input Input) ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("service_removal_work_input", input)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInputBytes {
		clear(value)
		return nil, errs.New(errs.KindValidationFailed, "Service removal capture exceeds its storage bound")
	}
	return value, nil
}

func Decode(value []byte) (Input, error) {
	if len(value) > maximumInputBytes {
		return Input{}, errs.New(errs.KindInternal, "Service removal capture exceeds its storage bound")
	}
	input, err := recordcodec.Decode[Input](value, "service_removal_work_input")
	if err != nil || input.Validate() != nil {
		return Input{}, errs.New(errs.KindInternal, "Service removal capture is corrupt")
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
