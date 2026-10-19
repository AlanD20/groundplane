// Package blueprintunits owns durable per-resource Blueprint execution facts.
// Desired input remains in the Blueprint head; these records never contain
// secret values or infer application from publication.
package blueprintunits

import (
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const recordPrefix = "/v1/runtime/blueprint-units/"
const maximumDesiredPlanBytes = 1 << 20

type ResourceKey struct {
	Kind ids.Kind `json:"kind"`
	ID   string   `json:"id"`
}

type Unit struct {
	Target      ResourceKey   `json:"target"`
	Removal     bool          `json:"removal"`
	Fingerprint string        `json:"fingerprint,omitempty"`
	Reads       []ResourceKey `json:"reads,omitempty"`
	Writes      []ResourceKey `json:"writes"`
	After       []ResourceKey `json:"after,omitempty"`
}

type AppliedState string

const (
	Applied   AppliedState = "applied"
	Absent    AppliedState = "absent"
	Diverged  AppliedState = "diverged"
	Uncertain AppliedState = "uncertain"
)

// AppliedRecord retains the acknowledged resource effect independently of Task
// diagnostics. Initial proven absence has no execution source.
type AppliedRecord struct {
	EnvironmentID                       string                         `json:"environment_id"`
	Target                              ResourceKey                    `json:"target"`
	State                               AppliedState                   `json:"state"`
	Fingerprint                         string                         `json:"fingerprint,omitempty"`
	AffectedWrites                      []ResourceKey                  `json:"affected_writes,omitempty"`
	ParentTaskID                        string                         `json:"parent_task_id,omitempty"`
	Execution                           *AppliedExecution              `json:"execution,omitempty"`
	NativeOperationID                   string                         `json:"native_operation_id,omitempty"`
	ControllerOperationID               string                         `json:"controller_operation_id,omitempty"`
	ControllerAttachInputRevision       int64                          `json:"controller_attach_input_revision,omitempty"`
	ControllerAttachBindingSHA256       string                         `json:"controller_attach_binding_sha256,omitempty"`
	ControllerAttachReferences          []attachments.BindingReference `json:"controller_attach_references,omitempty"`
	ControllerValueGenerationID         string                         `json:"controller_value_generation_id,omitempty"`
	ControllerRuntimeRevision           int64                          `json:"controller_runtime_revision,omitempty"`
	ControllerAuthoredRevision          int64                          `json:"controller_authored_revision,omitempty"`
	ControllerComponentRecordRevision   int64                          `json:"controller_component_record_revision,omitempty"`
	ControllerComponentRuntimeRevision  int64                          `json:"controller_component_runtime_revision,omitempty"`
	ControllerProxySelection            bool                           `json:"controller_proxy_selection,omitempty"`
	ControllerScriptSetRevision         int64                          `json:"controller_script_set_revision,omitempty"`
	ControllerDNS                       *DNSReceipt                    `json:"controller_dns,omitempty"`
	MetadataDNS                         *MetadataDNSReceipt            `json:"metadata_dns,omitempty"`
	ControllerServiceDeclarationAbsence *ServiceDeclarationAbsence     `json:"controller_service_declaration_absence,omitempty"`
}

// AppliedExecution identifies the exact accepted attempt and its immutable
// result. This receipt survives collection of execution history.
type AppliedExecution struct {
	TaskID     string                 `json:"task_id"`
	WorkItemID string                 `json:"work_item_id"`
	AttemptID  string                 `json:"attempt_id"`
	Executor   workexecution.Identity `json:"executor"`
	Input      workexecution.Input    `json:"input"`
	Result     workexecution.Input    `json:"result"`
}

func (receipt AppliedExecution) Validate() error {
	if ids.Validate(ids.KindTask, receipt.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, receipt.WorkItemID) != nil ||
		ids.Validate(ids.KindAttempt, receipt.AttemptID) != nil ||
		receipt.Executor.Validate() != nil || receipt.Input.Validate() != nil || receipt.Result.Validate() != nil {
		return invalidRecord()
	}
	return nil
}

type ExecutionState string

const (
	Pending  ExecutionState = "pending"
	Running  ExecutionState = "running"
	Draining ExecutionState = "draining"
)

// Execution keeps claims until an exact terminal/effect receipt settles them.
// Its parent can become obsolete without changing this immutable Unit.
type ExecutionRecord struct {
	EnvironmentID string                  `json:"environment_id"`
	TaskID        string                  `json:"task_id"`
	WorkItemID    string                  `json:"work_item_id"`
	Input         *workexecution.Input    `json:"input,omitempty"`
	AttemptID     string                  `json:"attempt_id,omitempty"`
	AttemptNumber uint64                  `json:"attempt_number"`
	Executor      *workexecution.Identity `json:"executor,omitempty"`
	Unit          Unit                    `json:"unit"`
	State         ExecutionState          `json:"state"`
}

// Epoch fences an entire fixed-revision selection before a child is published
// or a pending child is withdrawn. Sequence is monotonic within Environment.
type EpochRecord struct {
	EnvironmentID string `json:"environment_id"`
	Sequence      uint64 `json:"sequence"`
}

// DesiredPlan is the current parent's durable effective-input selection.
// Complete distinguishes an intentional empty plan from one still awaiting
// facts produced by an earlier unit in the same Apply.
type DesiredPlan struct {
	EnvironmentID string `json:"environment_id"`
	ParentTaskID  string `json:"parent_task_id"`
	Complete      bool   `json:"complete"`
	Units         []Unit `json:"units,omitempty"`
}

func EpochKey(environmentID string) string {
	return recordPrefix + environmentID + "/epoch"
}

func DesiredPlanKey(environmentID string) string {
	return recordPrefix + environmentID + "/desired"
}

func AppliedPrefix(environmentID string) string {
	return recordPrefix + environmentID + "/applied/"
}

func AppliedKey(environmentID string, target ResourceKey) string {
	return AppliedPrefix(environmentID) + string(target.Kind) + "/" + target.ID
}

func ExecutionPrefix(environmentID string) string {
	return recordPrefix + environmentID + "/executions/"
}

func ExecutionKey(environmentID, workItemID string) string {
	return ExecutionPrefix(environmentID) + workItemID
}

func EncodeEpoch(record EpochRecord) ([]byte, error) {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil || record.Sequence == 0 {
		return nil, invalidRecord()
	}
	return recordcodec.Encode("blueprint-unit-epoch", record)
}

func DecodeEpoch(value []byte) (EpochRecord, error) {
	record, err := recordcodec.Decode[EpochRecord](value, "blueprint-unit-epoch")
	if err != nil || ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil || record.Sequence == 0 {
		return EpochRecord{}, corruptRecord()
	}
	return record, nil
}

func EncodeDesiredPlan(record DesiredPlan) ([]byte, error) {
	if !validDesiredPlan(record) {
		return nil, invalidRecord()
	}
	value, err := recordcodec.Encode("blueprint-unit-desired-plan", record)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumDesiredPlanBytes {
		clear(value)
		return nil, invalidRecord()
	}
	return value, nil
}

func DecodeDesiredPlan(value []byte) (DesiredPlan, error) {
	if len(value) > maximumDesiredPlanBytes {
		return DesiredPlan{}, corruptRecord()
	}
	record, err := recordcodec.Decode[DesiredPlan](value, "blueprint-unit-desired-plan")
	if err != nil || !validDesiredPlan(record) {
		return DesiredPlan{}, corruptRecord()
	}
	return record, nil
}

func validDesiredPlan(record DesiredPlan) bool {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, record.ParentTaskID) != nil {
		return false
	}
	for index, unit := range record.Units {
		if !validUnit(unit) || index > 0 && compareKey(record.Units[index-1].Target, unit.Target) >= 0 {
			return false
		}
	}
	return true
}

func EncodeApplied(record AppliedRecord) ([]byte, error) {
	if err := validateApplied(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("blueprint-unit-applied", record)
}

func DecodeApplied(value []byte) (AppliedRecord, error) {
	record, err := recordcodec.Decode[AppliedRecord](value, "blueprint-unit-applied")
	if err != nil || validateApplied(record) != nil {
		return AppliedRecord{}, corruptRecord()
	}
	return record, nil
}

func EncodeExecution(record ExecutionRecord) ([]byte, error) {
	if err := validateExecution(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("blueprint-unit-execution", record)
}

func DecodeExecution(value []byte) (ExecutionRecord, error) {
	record, err := recordcodec.Decode[ExecutionRecord](value, "blueprint-unit-execution")
	if err != nil || validateExecution(record) != nil {
		return ExecutionRecord{}, corruptRecord()
	}
	return record, nil
}

func validateApplied(record AppliedRecord) error {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil || !validKey(record.Target) ||
		!validKeys(record.AffectedWrites) {
		return invalidRecord()
	}
	if record.MetadataDNS != nil {
		if !validMetadataDNSReceipt(record) {
			return invalidRecord()
		}
		return nil
	}
	if record.ControllerProxySelection &&
		(record.Target.Kind != ids.KindComponent || record.ControllerOperationID == "" ||
			record.NativeOperationID != "" || record.Execution != nil || record.State != Applied) {
		return invalidRecord()
	}
	if record.ControllerAttachInputRevision != 0 && (record.ControllerAttachInputRevision < 0 ||
		!recordcodec.ValidSHA256(record.ControllerAttachBindingSHA256) ||
		record.Target.Kind != ids.KindAttach || record.ControllerOperationID == "" || record.State != Applied ||
		record.NativeOperationID != "" || record.Execution != nil) {
		return invalidRecord()
	}
	if record.ControllerAttachInputRevision == 0 && record.ControllerAttachBindingSHA256 != "" {
		return invalidRecord()
	}
	if record.ControllerAttachInputRevision > 0 {
		if len(record.ControllerAttachReferences) == 0 ||
			len(record.ControllerAttachReferences) > attachments.MaximumAttachGrants+1 {
			return invalidRecord()
		}
		seen := make(map[string]bool, len(record.ControllerAttachReferences))
		for _, reference := range record.ControllerAttachReferences {
			if reference.Validate() != nil || seen[reference.AttachID] {
				return invalidRecord()
			}
			seen[reference.AttachID] = true
		}
	} else if len(record.ControllerAttachReferences) != 0 {
		return invalidRecord()
	}
	if record.ControllerDNS != nil && (record.Target.Kind != ids.KindDNSRecord ||
		record.ControllerOperationID == "" || !validDNSReceipt(record)) {
		return invalidRecord()
	}
	if record.ControllerServiceDeclarationAbsence != nil && !validServiceDeclarationAbsence(record) {
		return invalidRecord()
	}
	if record.NativeOperationID != "" {
		if record.State != Absent || !nativeRemovalTarget(record.Target) || record.ParentTaskID != "" ||
			ids.Validate(ids.KindOperation, record.NativeOperationID) != nil ||
			record.Execution == nil || record.Execution.Validate() != nil ||
			record.ControllerOperationID != "" || record.ControllerValueGenerationID != "" ||
			record.ControllerRuntimeRevision != 0 || record.ControllerAuthoredRevision != 0 ||
			record.ControllerComponentRecordRevision != 0 || record.ControllerComponentRuntimeRevision != 0 ||
			record.ControllerScriptSetRevision != 0 {
			return invalidRecord()
		}
	} else if record.ControllerOperationID != "" {
		validAttachBinding := record.Target.Kind == ids.KindAttach && record.ControllerAttachInputRevision > 0 &&
			record.ControllerValueGenerationID == "" && record.ControllerRuntimeRevision == 0 &&
			record.ControllerAuthoredRevision == 0 && record.ControllerComponentRecordRevision == 0 &&
			record.ControllerComponentRuntimeRevision == 0 && record.ControllerScriptSetRevision == 0 &&
			record.ControllerDNS == nil && record.ControllerServiceDeclarationAbsence == nil
		validEntry := record.Target.Kind == ids.KindEnvEntry &&
			ids.Validate(ids.KindConfig, record.ControllerValueGenerationID) == nil &&
			record.ControllerRuntimeRevision == 0 && record.ControllerAuthoredRevision == 0 &&
			record.ControllerComponentRecordRevision == 0 && record.ControllerComponentRuntimeRevision == 0 &&
			record.ControllerScriptSetRevision == 0
		validServiceConfiguration := record.Target.Kind == ids.KindService &&
			record.ControllerValueGenerationID == "" && record.ControllerRuntimeRevision >= 0 &&
			record.ControllerAuthoredRevision > 0 && record.ControllerComponentRecordRevision == 0 &&
			record.ControllerComponentRuntimeRevision == 0 && record.ControllerScriptSetRevision == 0
		validServiceDeclaration := validServiceDeclarationAbsence(record) &&
			record.ControllerValueGenerationID == "" && record.ControllerRuntimeRevision == 0 &&
			record.ControllerAuthoredRevision == 0 && record.ControllerComponentRecordRevision == 0 &&
			record.ControllerComponentRuntimeRevision == 0 && record.ControllerScriptSetRevision == 0
		validRoute := record.Target.Kind == ids.KindRoute &&
			record.ControllerValueGenerationID == "" && record.ControllerRuntimeRevision == 0 &&
			record.ControllerAuthoredRevision > 0 && record.ControllerComponentRecordRevision == 0 &&
			record.ControllerComponentRuntimeRevision == 0 && record.ControllerScriptSetRevision == 0
		validDisabledComponent := record.Target.Kind == ids.KindComponent &&
			record.ControllerValueGenerationID == "" && record.ControllerRuntimeRevision == 0 &&
			record.ControllerAuthoredRevision == 0 && record.ControllerComponentRecordRevision > 0 &&
			record.ControllerComponentRuntimeRevision >= 0 && record.ControllerScriptSetRevision == 0
		validScript := record.Target.Kind == ids.KindScript && record.ControllerScriptSetRevision > 0 &&
			record.ControllerValueGenerationID == "" && record.ControllerRuntimeRevision == 0 &&
			record.ControllerAuthoredRevision == 0 && record.ControllerComponentRecordRevision == 0 &&
			record.ControllerComponentRuntimeRevision == 0
		validDNS := record.Target.Kind == ids.KindDNSRecord && record.ControllerDNS != nil &&
			record.ControllerAuthoredRevision > 0 && record.ControllerValueGenerationID == "" &&
			record.ControllerRuntimeRevision == 0 && record.ControllerComponentRecordRevision == 0 &&
			record.ControllerComponentRuntimeRevision == 0 && record.ControllerScriptSetRevision == 0
		if (!validAttachBinding && !validEntry && !validServiceConfiguration && !validServiceDeclaration && !validRoute && !validDisabledComponent && !validScript && !validDNS) ||
			(record.State != Applied && !((validScript || validEntry || validServiceDeclaration || validRoute || validDisabledComponent || validDNS) && record.State == Absent)) ||
			ids.Validate(ids.KindTask, record.ParentTaskID) != nil ||
			ids.Validate(ids.KindOperation, record.ControllerOperationID) != nil ||
			record.Execution != nil {
			return invalidRecord()
		}
	} else if record.Execution == nil {
		if record.ParentTaskID != "" || record.ControllerValueGenerationID != "" ||
			record.ControllerRuntimeRevision != 0 || record.ControllerAuthoredRevision != 0 ||
			record.ControllerComponentRecordRevision != 0 || record.ControllerComponentRuntimeRevision != 0 ||
			record.ControllerScriptSetRevision != 0 ||
			record.State != Absent {
			return invalidRecord()
		}
	} else if ids.Validate(ids.KindTask, record.ParentTaskID) != nil ||
		record.Execution.Validate() != nil || record.ParentTaskID != record.Execution.TaskID ||
		record.ControllerValueGenerationID != "" || record.ControllerRuntimeRevision != 0 ||
		record.ControllerAuthoredRevision != 0 || record.ControllerComponentRecordRevision != 0 ||
		record.ControllerComponentRuntimeRevision != 0 || record.ControllerScriptSetRevision != 0 {
		return invalidRecord()
	}
	switch record.State {
	case Applied:
		if !recordcodec.ValidSHA256(record.Fingerprint) || len(record.AffectedWrites) != 0 {
			return invalidRecord()
		}
	case Absent:
		if record.Fingerprint != "" || len(record.AffectedWrites) != 0 {
			return invalidRecord()
		}
	case Diverged, Uncertain:
		if len(record.AffectedWrites) == 0 || !slices.Contains(record.AffectedWrites, record.Target) ||
			record.Fingerprint != "" && !recordcodec.ValidSHA256(record.Fingerprint) {
			return invalidRecord()
		}
		if record.State == Diverged && len(record.AffectedWrites) != 1 {
			return invalidRecord()
		}
	default:
		return invalidRecord()
	}
	return nil
}

func (record AppliedRecord) HasEffectSource() bool {
	return record.Execution != nil || record.ControllerOperationID != "" || record.MetadataDNS != nil
}

func validateExecution(record ExecutionRecord) error {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, record.WorkItemID) != nil || !validUnit(record.Unit) ||
		record.Input != nil && record.Input.Validate() != nil {
		return invalidRecord()
	}
	switch record.State {
	case Pending:
		if record.AttemptNumber != 0 || record.AttemptID != "" || record.Executor != nil {
			return invalidRecord()
		}
	case Running, Draining:
		if record.AttemptNumber == 0 || ids.Validate(ids.KindAttempt, record.AttemptID) != nil ||
			record.Executor == nil || record.Executor.Validate() != nil || record.Input == nil {
			return invalidRecord()
		}
	default:
		return invalidRecord()
	}
	return nil
}

func validUnit(unit Unit) bool {
	return validKey(unit.Target) && validKeys(unit.Reads) && validKeys(unit.Writes) &&
		validKeys(unit.After) && slices.Contains(unit.Writes, unit.Target) &&
		!intersects(unit.Reads, unit.Writes) && !slices.Contains(unit.After, unit.Target) &&
		(unit.Removal && unit.Fingerprint == "" || !unit.Removal && recordcodec.ValidSHA256(unit.Fingerprint))
}

// ValidateUnit exposes the resource dependency/claim contract to native work
// publication without encoding a record merely to validate its shape.
func ValidateUnit(unit Unit) error {
	if !validUnit(unit) {
		return invalidRecord()
	}
	return nil
}

func intersects(left, right []ResourceKey) bool {
	for i, j := 0, 0; i < len(left) && j < len(right); {
		switch order := compareKey(left[i], right[j]); {
		case order == 0:
			return true
		case order < 0:
			i++
		default:
			j++
		}
	}
	return false
}

func validKeys(keys []ResourceKey) bool {
	for index, key := range keys {
		if !validKey(key) || index > 0 && compareKey(keys[index-1], key) >= 0 {
			return false
		}
	}
	return true
}

func validKey(key ResourceKey) bool {
	switch key.Kind {
	case ids.KindEnvironment, ids.KindService, ids.KindEnvEntry, ids.KindVolume, ids.KindAttach,
		ids.KindRoute, ids.KindDNSRecord, ids.KindNetwork, ids.KindComponent, ids.KindSecret, ids.KindScript:
		return ids.Validate(key.Kind, key.ID) == nil
	default:
		return false
	}
}

func compareKey(left, right ResourceKey) int {
	if order := strings.Compare(string(left.Kind), string(right.Kind)); order != 0 {
		return order
	}
	return strings.Compare(left.ID, right.ID)
}

func invalidRecord() error {
	return errs.New(errs.KindValidationFailed, "Blueprint unit record is invalid")
}
func corruptRecord() error { return errs.New(errs.KindInternal, "Blueprint unit record is corrupt") }
