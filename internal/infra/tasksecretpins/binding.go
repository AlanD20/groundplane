package tasksecretpins

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
)

// Binding is included in the immutable operation input. It identifies retained
// metadata and ciphertext digests, never Secret plaintext. All attempts of the
// same work item keep this binding until the operation releases recovery rights.
type Binding struct {
	OperationID string `json:"operation_id"`
	TaskID      string `json:"task_id"`
	WorkItemID  string `json:"work_item_id"`
	Count       uint64 `json:"count"`
	SHA256      string `json:"sha256"`
}

func (binding Binding) Validate() error {
	if ids.Validate(ids.KindOperation, binding.OperationID) != nil ||
		ids.Validate(ids.KindTask, binding.TaskID) != nil || ids.Validate(ids.KindWorkItem, binding.WorkItemID) != nil ||
		binding.Count == 0 || binding.Count > MaximumPins || !validSHA256(binding.SHA256) {
		return validation("work Secret retention binding is invalid")
	}
	return nil
}

func (prepared Prepared) Binding() (Binding, error) {
	if err := validatePrepared(prepared); err != nil {
		return Binding{}, err
	}
	return Binding{OperationID: prepared.record.OperationID, TaskID: prepared.record.TaskID,
		WorkItemID: prepared.record.WorkItemID, Count: prepared.record.MembershipCount,
		SHA256: prepared.record.MembershipSHA256}, nil
}

// MatchesSources checks the retained membership digest, not merely its size.
func (binding Binding) MatchesSources(sources []tasksecretpinrecord.Record) bool {
	if binding.Validate() != nil {
		return false
	}
	_, digest, err := canonicalPins(binding.OperationID, binding.TaskID, binding.WorkItemID, sources)
	return err == nil && binding.Count == uint64(len(sources)) && binding.SHA256 == digest
}

// AdmissionCondition must join the native assignment transaction. The caller
// also compares the work item and captured input, preventing concurrent release
// of recovery rights from admitting a new attempt.
func (root ActiveRoot) AdmissionCondition(binding Binding) (Condition, error) {
	condition, _, err := root.RetentionCondition(binding)
	if err != nil || root.record.Phase != phaseActive {
		return Condition{}, conflict("work item no longer owns its captured Secret values")
	}
	return condition, nil
}

// RetentionCondition matches the same immutable binding while an already
// authorized release is draining. It cannot grant execution permission.
func (root ActiveRoot) RetentionCondition(binding Binding) (Condition, bool, error) {
	if validateActive(root) != nil || binding.Validate() != nil ||
		root.record.OperationID != binding.OperationID || root.record.TaskID != binding.TaskID ||
		root.record.WorkItemID != binding.WorkItemID || root.record.MembershipCount != binding.Count ||
		root.record.MembershipSHA256 != binding.SHA256 {
		return Condition{}, false, conflict("work item no longer owns its captured Secret values")
	}
	return Condition{Key: RootKey(binding.OperationID), ModRevision: root.revision}, root.record.Phase == phaseReleasing, nil
}
