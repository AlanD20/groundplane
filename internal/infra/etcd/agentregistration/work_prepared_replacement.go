package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedReplacementExecution struct {
	TaskID               string                 `json:"task_id"`
	AgentID              string                 `json:"agent_id"`
	CredentialGeneration uint64                 `json:"credential_generation"`
	Identity             workexecution.Identity `json:"identity"`
	ProcessGeneration    [16]byte               `json:"process_generation"`
}

func PreparedReplacementExecutionKey(agentID, taskID string) string {
	return "/v1/execution/agent-prepared-replacements/" + agentID + "/" + taskID
}

func (proof preparedReplacementExecution) validate(taskID string, binding MaintenanceBinding) error {
	if ids.Validate(ids.KindTask, proof.TaskID) != nil || proof.TaskID != taskID ||
		proof.AgentID != binding.AgentID || proof.CredentialGeneration != binding.Generation ||
		proof.Identity.Validate() != nil || proof.Identity.Executor != workexecution.Agent ||
		proof.Identity.HostID != binding.HostID || proof.Identity.AgentID != binding.AgentID ||
		proof.ProcessGeneration == [16]byte{} {
		return errs.New(errs.KindStateConflict, "prepared Agent replacement execution proof is invalid")
	}
	return nil
}

func (repository *Repository) readPreparedReplacementExecution(
	ctx context.Context,
	taskID string,
	binding MaintenanceBinding,
) (keyvalue.Condition, preparedReplacementExecution, bool, error) {
	if ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Condition{}, preparedReplacementExecution{}, false,
			errs.New(errs.KindValidationFailed, "prepared Agent replacement Task identity is invalid")
	}
	key := PreparedReplacementExecutionKey(binding.AgentID, taskID)
	guard := keyvalue.Condition{Key: key}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{key}, Revision: binding.ReadRevision,
	})
	if err != nil {
		return guard, preparedReplacementExecution{}, false, err
	}
	if read == nil || read.ReadRevision != binding.ReadRevision || len(read.Values) != 1 {
		return guard, preparedReplacementExecution{}, false,
			errs.New(errs.KindStateConflict, "prepared Agent replacement proof read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return guard, preparedReplacementExecution{}, false, nil
	}
	if read.Values[0].Key != key || read.Values[0].ModRevision <= 0 ||
		read.Values[0].ModRevision > read.ReadRevision {
		return guard, preparedReplacementExecution{}, false,
			errs.New(errs.KindStateConflict, "prepared Agent replacement proof revision is invalid")
	}
	proof, err := recordcodec.Decode[preparedReplacementExecution](
		read.Values[0].Value,
		"agent_prepared_replacement_execution",
	)
	if err != nil {
		return guard, preparedReplacementExecution{}, false, err
	}
	if err := proof.validate(taskID, binding); err != nil {
		return guard, preparedReplacementExecution{}, false, err
	}
	guard.ModRevision = read.Values[0].ModRevision
	return guard, proof, true, nil
}

// BindPreparedReplacementExecution seals one freshly qualified work process
// to the native Task before credential replacement.
func (repository *Repository) BindPreparedReplacementExecution(
	ctx context.Context,
	taskID string,
	execution MaintenanceExecution,
) error {
	if ids.Validate(ids.KindTask, taskID) != nil || execution.ExecutorRevision <= 0 ||
		execution.Binding.ReadRevision <= 0 || execution.Identity.Executor != workexecution.Agent ||
		execution.Identity.HostID != execution.Binding.HostID ||
		execution.Identity.AgentID != execution.Binding.AgentID || execution.ProcessGeneration == [16]byte{} {
		return errs.New(errs.KindValidationFailed, "prepared Agent replacement binding is invalid")
	}
	proofGuard, _, _, err := repository.readPreparedReplacementExecution(ctx, taskID, execution.Binding)
	if err != nil {
		return err
	}
	proof := preparedReplacementExecution{TaskID: taskID, AgentID: execution.Binding.AgentID,
		CredentialGeneration: execution.Binding.Generation, Identity: execution.Identity,
		ProcessGeneration: execution.ProcessGeneration}
	raw, err := recordcodec.Encode("agent_prepared_replacement_execution", proof)
	if err != nil {
		return err
	}
	defer clear(raw)
	conditions := append([]keyvalue.Condition(nil), execution.Binding.Conditions...)
	conditions = append(conditions, keyvalue.Condition{
		Key: workqueue.ExecutorKey(execution.Identity), ModRevision: execution.ExecutorRevision,
	}, proofGuard)
	result, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: proofGuard.Key, Value: raw,
	}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Agent executor changed before prepared replacement was sealed")
	}
	return nil
}

// PreparedReplacementExecution returns the original qualified process while
// the credential primary points at its offline candidate.
func (repository *Repository) PreparedReplacementExecution(
	ctx context.Context,
	taskID, agentID string,
	original, current uint64,
) (MaintenanceExecution, error) {
	binding, err := repository.replacementBinding(ctx, agentID, original, current, 0)
	if err != nil {
		return MaintenanceExecution{}, err
	}
	_, proof, found, err := repository.readPreparedReplacementExecution(ctx, taskID, binding)
	if err != nil {
		return MaintenanceExecution{}, err
	}
	if !found {
		return MaintenanceExecution{}, errs.New(
			errs.KindStateConflict,
			"prepared Agent replacement has no qualified executor proof",
		)
	}
	return MaintenanceExecution{Binding: binding, Identity: proof.Identity,
		ProcessGeneration: proof.ProcessGeneration}, nil
}

func (repository *Repository) ReleasePreparedReplacementExecution(
	ctx context.Context,
	taskID, agentID string,
	original, current uint64,
) error {
	binding, err := repository.replacementBinding(ctx, agentID, original, current, 0)
	if err != nil {
		return err
	}
	guard, _, found, err := repository.readPreparedReplacementExecution(ctx, taskID, binding)
	if err != nil || !found {
		return err
	}
	conditions := append([]keyvalue.Condition(nil), binding.Conditions...)
	conditions = append(conditions, guard)
	result, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{{
		Type: keyvalue.MutationDelete, Key: guard.Key,
	}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "prepared Agent replacement changed before proof release")
	}
	return nil
}
