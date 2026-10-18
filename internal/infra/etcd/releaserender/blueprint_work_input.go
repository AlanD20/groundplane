package releaserender

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintReleaseWorkInput binds the pre-switch rollout to the file state
// captured at final publication. It references the large immutable rollout rather
// than copying it into the work item, attempts or progress events. The operation
// handler consumes this input directly; no generic ExecutionPlan is embedded.
type BlueprintReleaseWorkInput struct {
	OperationID   string                           `json:"operation_id"`
	Rollout       workexecution.Input              `json:"rollout"`
	Configuration runtimeconfiguration.WorkCapture `json:"configuration"`
	Secrets       *tasksecretpins.Binding          `json:"secrets,omitempty"`
}

func (input BlueprintReleaseWorkInput) Validate() error {
	if ids.Validate(ids.KindOperation, input.OperationID) != nil || input.Rollout.Validate() != nil ||
		input.Configuration.Validate() != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint Release work input is invalid")
	}
	if input.Secrets != nil && (input.Secrets.Validate() != nil ||
		input.Secrets.OperationID != input.OperationID ||
		input.Secrets.TaskID != input.Configuration.TaskID ||
		input.Secrets.WorkItemID != input.Configuration.WorkItemID) {
		return errs.New(errs.KindValidationFailed, "Blueprint Release Secret retention belongs to another work item")
	}
	return nil
}

func (input BlueprintReleaseWorkInput) ValidateRollout(capture BlueprintServiceRolloutCapture) error {
	if err := input.Validate(); err != nil {
		return err
	}
	expected, err := BlueprintRolloutInput(capture)
	if err != nil {
		return err
	}
	configuration := input.Configuration
	if input.Rollout != expected || input.OperationID != capture.OperationID ||
		configuration.TaskID != capture.TaskID || configuration.WorkItemID != capture.WorkItemID ||
		configuration.Placement != capture.Placement ||
		configuration.Current.EnvironmentID != capture.Scope.EnvironmentID ||
		configuration.Generation != capture.Render.Projection.RenderGeneration {
		return errs.New(errs.KindStateConflict, "Blueprint Release input differs from its captured rollout")
	}
	return nil
}

func EncodeBlueprintReleaseWorkInput(input BlueprintReleaseWorkInput) ([]byte, workexecution.Input, error) {
	if err := input.Validate(); err != nil {
		return nil, workexecution.Input{}, err
	}
	value, err := recordcodec.Encode("blueprint-release-work", input)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	if len(value) > keyvalue.MaximumBytes {
		clear(value)
		return nil, workexecution.Input{}, errs.New(errs.KindReleasePlanTooLarge, "Blueprint Release work input exceeds the storage budget")
	}
	digest := sha256.Sum256(value)
	return value, workexecution.Input{ID: input.Rollout.ID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeBlueprintReleaseWorkInput(value []byte, expected workexecution.Input) (BlueprintReleaseWorkInput, error) {
	if len(value) == 0 || len(value) > keyvalue.MaximumBytes || expected.Validate() != nil {
		return BlueprintReleaseWorkInput{}, errs.New(errs.KindValidationFailed, "Blueprint Release work input reference is invalid")
	}
	digest := sha256.Sum256(value)
	if hex.EncodeToString(digest[:]) != expected.SHA256 {
		return BlueprintReleaseWorkInput{}, errs.New(errs.KindStateConflict, "Blueprint Release work input digest changed")
	}
	input, err := recordcodec.Decode[BlueprintReleaseWorkInput](value, "blueprint-release-work")
	if err != nil {
		return BlueprintReleaseWorkInput{}, err
	}
	if err := input.Validate(); err != nil {
		return BlueprintReleaseWorkInput{}, err
	}
	if input.Rollout.ID != expected.ID {
		return BlueprintReleaseWorkInput{}, errs.New(errs.KindStateConflict, "Blueprint Release work input identity changed")
	}
	return input, nil
}
