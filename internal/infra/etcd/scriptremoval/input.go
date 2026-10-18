// Package scriptremoval owns captured input for Controller-only deletion.
// It contains identity and revisions only, never executable body or secret bytes.
package scriptremoval

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumInputBytes = 4096

type Input struct {
	TaskID              string    `json:"task_id"`
	WorkItemID          string    `json:"work_item_id"`
	PlanID              string    `json:"plan_id"`
	TenantID            string    `json:"tenant_id,omitempty"`
	ProjectID           string    `json:"project_id"`
	EnvironmentID       string    `json:"environment_id"`
	ScriptID            string    `json:"script_id"`
	ScriptRevision      int64     `json:"script_revision"`
	ServiceID           string    `json:"service_id"`
	ScriptSetGeneration string    `json:"script_set_generation"`
	CreatedAt           time.Time `json:"created_at"`
}

func InputKey(workID string) string { return "/v1/execution/script-removal/inputs/" + workID }

func (input Input) Validate() error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindScript, input.ScriptID) != nil ||
		ids.Validate(ids.KindProject, input.ProjectID) != nil || ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		input.TenantID != "" && ids.Validate(ids.KindTenant, input.TenantID) != nil ||
		input.ScriptRevision <= 0 || ids.Validate(ids.KindService, input.ServiceID) != nil || input.ScriptSetGeneration == "" || len(input.ScriptSetGeneration) > 128 || strings.Contains(input.ScriptSetGeneration, "/") || !recordcodec.IsCanonicalUTC(input.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Script removal capture is invalid")
	}
	return nil
}

func Encode(input Input) ([]byte, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("script_removal_input", input)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumInputBytes {
		return nil, errs.New(errs.KindValidationFailed, "Script removal input exceeds its storage bound")
	}
	return value, nil
}

func Decode(value []byte) (Input, error) {
	if len(value) > maximumInputBytes {
		return Input{}, errs.New(errs.KindInternal, "Script removal input exceeds its storage bound")
	}
	input, err := recordcodec.Decode[Input](value, "script_removal_input")
	if err != nil || input.Validate() != nil {
		return Input{}, errs.New(errs.KindInternal, "Script removal input is corrupt")
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
