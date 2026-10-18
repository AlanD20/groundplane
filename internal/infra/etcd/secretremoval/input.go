// Package secretremoval owns immutable Secret deletion identity, never secret bytes.
package secretremoval

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumInputBytes = 4096

type Input struct {
	TaskID         string    `json:"task_id"`
	WorkItemID     string    `json:"work_item_id"`
	PlanID         string    `json:"plan_id"`
	TenantID       string    `json:"tenant_id,omitempty"`
	ProjectID      string    `json:"project_id,omitempty"`
	SecretID       string    `json:"secret_id"`
	SecretRevision int64     `json:"secret_revision"`
	ValueRevision  int64     `json:"value_revision"`
	CreatedAt      time.Time `json:"created_at"`
}

func InputKey(workID string) string { return "/v1/execution/secret-removal/inputs/" + workID }

func (input Input) Validate() error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindSecret, input.SecretID) != nil ||
		input.ProjectID != "" && ids.Validate(ids.KindProject, input.ProjectID) != nil ||
		input.ProjectID == "" && input.TenantID != "" ||
		input.TenantID != "" && ids.Validate(ids.KindTenant, input.TenantID) != nil ||
		input.SecretRevision <= 0 || input.ValueRevision <= 0 || !recordcodec.IsCanonicalUTC(input.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Secret removal capture is invalid")
	}
	return nil
}

func Encode(input Input) ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("secret_removal_input", input)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInputBytes {
		return nil, errs.New(errs.KindValidationFailed, "Secret removal input exceeds its storage bound")
	}
	return value, nil
}

func Decode(value []byte) (Input, error) {
	if len(value) > maximumInputBytes {
		return Input{}, errs.New(errs.KindInternal, "Secret removal input exceeds its storage bound")
	}
	input, err := recordcodec.Decode[Input](value, "secret_removal_input")
	if err != nil || input.Validate() != nil {
		return Input{}, errs.New(errs.KindInternal, "Secret removal input is corrupt")
	}
	return input, nil
}

func (input Input) Reference() (workexecution.Input, error) {
	value, err := Encode(input)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(value)
	return workexecution.Input{ID: input.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func PreparePublication(input Input) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	value, err := Encode(input)
	if err != nil {
		return nil, nil, err
	}
	key := InputKey(input.WorkItemID)
	return []keyvalue.Condition{{Key: key}}, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}, nil
}
