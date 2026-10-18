package etcd

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareReleaseHookRecovery ties reported absence to durable receipts and
// records cleanup separately from forward execution. The caller commits this
// change with recovery completion; a changed receipt cannot release claims.
func (repository *ScriptRepository) PrepareReleaseHookRecovery(ctx context.Context, view workqueue.View,
	input releaseexecution.RecoveryInput, proof *releaseexecution.QuiescenceProof, evidence workexecution.Input, at time.Time,
) (workqueue.Change, error) {
	if input.Validate() != nil || proof == nil || evidence.Validate() != nil || len(proof.Hooks) != len(input.Input.Release.Hooks) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release recovery is missing captured Script cleanup proof")
	}
	var change workqueue.Change
	ok := false
	defer func() {
		if !ok {
			keyvalue.ZeroMutationBytes(change.Mutations)
		}
	}()
	for index, hook := range input.Input.Release.Hooks {
		reference := workexecution.Input{ID: hook.Input.InputId, SHA256: hex.EncodeToString(hook.Input.Sha256)}
		observed := proof.Hooks[index]
		if observed.ExecutionID != hook.ExecutionId || observed.Input != reference || !observed.Absent {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script cleanup proof differs from the captured hook")
		}
		loaded, err := repository.loadCapturedReleaseScriptWork(ctx, view, hook.ExecutionId, reference)
		if err != nil {
			return workqueue.Change{}, err
		}
		record := loaded.Execution
		cleanup := observed.Cleanup
		matches := cleanup.ContainerAbsent && cleanup.BodyAbsent && cleanup.ExecutionDirectoryAbsent &&
			!observed.ObservedAt.Before(record.UpdatedAt)
		if record.ContainerCreated == nil {
			matches = matches && cleanup.ContainerID == "" && observed.ContainerID == ""
		} else {
			matches = matches && cleanup.ContainerID == record.ContainerCreated.ContainerID &&
				(observed.ContainerID == "" || observed.ContainerID == record.ContainerCreated.ContainerID)
		}
		if record.BodyPrepared == nil {
			matches = matches && cleanup.BodyDevice == 0 && cleanup.BodyInode == 0 && cleanup.BodyLeaf == ""
		} else {
			matches = matches && cleanup.BodyDevice == record.BodyPrepared.Device &&
				cleanup.BodyInode == record.BodyPrepared.Inode && cleanup.BodyLeaf == record.BodyPrepared.Leaf
		}
		if !matches {
			loaded.Execution.Clear()
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script cleanup proof does not match its saved container and file receipts")
		}
		next, err := scriptexecutions.SettleRecovery(record, scriptexecutions.ScriptRecoverySettlement{
			AttemptID: input.Input.Attempt.ID, Evidence: evidence, Cleanup: cleanup,
			ObservedAt: observed.ObservedAt, SettledAt: at.UTC(),
		})
		if err != nil {
			loaded.Execution.Clear()
			return workqueue.Change{}, err
		}
		raw, err := recordcodec.Encode("script-execution", next)
		loaded.Execution.Clear()
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, loaded.Conditions...)
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: scriptexecutions.ScriptExecutionKey(hook.ExecutionId), Value: raw})
	}
	ok = true
	return change, nil
}
