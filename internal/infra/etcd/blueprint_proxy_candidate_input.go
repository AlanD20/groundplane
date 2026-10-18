package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// StageBlueprintProxyCandidateInput writes inert, immutable input chunks. The
// Component's atomic publication must subsequently compare the returned root;
// neither these writes nor the root grant an execution or applied-state receipt.
func (repository *EnvironmentBlueprintRepository) StageBlueprintProxyCandidateInput(ctx context.Context,
	work workqueue.View, projection projectionrecord.EnvironmentComposeProjection,
	desiredRootRevision int64,
) (keyvalue.Versioned[componentplanning.ProxyCandidateInputReference], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	task, item := work.Task.Task, work.Item.Item
	if repository == nil || desiredRootRevision <= 0 || work.ReadRevision < desiredRootRevision ||
		work.Task.Revision <= 0 || work.Item.Revision <= 0 || task.Validate() != nil || item.Validate() != nil ||
		task.Action != workexecution.ActionApply || item.TaskID != task.ID ||
		item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Agent ||
		(item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.ForwardRevoked ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		task.Scope.EnvironmentID != projection.EnvironmentID || task.ID != projection.RevisionID {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, errs.New(
			errs.KindValidationFailed,
			"prepared router input staging is invalid",
		)
	}
	if err := repository.validateProxyCandidateDesired(ctx, task, projection, desiredRootRevision, work.ReadRevision); err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	matched := 0
	for _, component := range projection.Components {
		if component.Desired.ID == item.TargetID && component.Desired.Kind.IsEnvironmentProxy() &&
			component.Desired.OwnerID == projection.EnvironmentID {
			matched++
		}
	}
	if matched != 1 {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, errs.New(
			errs.KindStateConflict, "prepared router input has no unique target Component")
	}
	return repository.stageBlueprintProxyCandidateInput(ctx, componentplanning.ProxyCandidateInputReference{
		EnvironmentID: projection.EnvironmentID, ParentTaskID: task.ID,
		SourceID: item.ID, SourceOwner: componentplanning.ProxyInputAgentWork, ComponentID: item.TargetID,
	}, projection, desiredRootRevision)
}

func (repository *EnvironmentBlueprintRepository) stageBlueprintProxyCandidateInput(ctx context.Context,
	reference componentplanning.ProxyCandidateInputReference, projection projectionrecord.EnvironmentComposeProjection,
	desiredRootRevision int64,
) (keyvalue.Versioned[componentplanning.ProxyCandidateInputReference], error) {
	value, err := projectionrecord.EncodePreparedEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	reference.SHA256, reference.ByteSize = hex.EncodeToString(digest[:]), len(value)
	if err := reference.Validate(); err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	fence := keyvalue.Condition{
		Key:         blueprints.EnvironmentBlueprintRootKey(reference.EnvironmentID, reference.ParentTaskID),
		ModRevision: desiredRootRevision,
	}
	for offset, ordinal := 0, 0; offset < len(value); offset, ordinal = offset+componentplanning.ProxyCandidateInputChunkBytes, ordinal+1 {
		chunk := value[offset:min(offset+componentplanning.ProxyCandidateInputChunkBytes, len(value))]
		if _, _, err := repository.stageBlueprintProxyInputValue(ctx, fence, componentplanning.ProxyCandidateInputChunkKey(reference, ordinal), chunk); err != nil {
			return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
		}
	}
	root, err := recordcodec.Encode("proxy-candidate-input", reference)
	if err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	defer clear(root)
	modRevision, readRevision, err := repository.stageBlueprintProxyInputValue(
		ctx,
		fence,
		componentplanning.ProxyCandidateInputKey(reference),
		root,
	)
	if err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{
		Record:       reference,
		Revision:     modRevision,
		ReadRevision: readRevision,
	}, nil
}

func (repository *EnvironmentBlueprintRepository) stageBlueprintProxyInputValue(ctx context.Context,
	fence keyvalue.Condition, key string, value []byte,
) (int64, int64, error) {
	conditions := []keyvalue.Condition{fence, {Key: key}}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return 0, 0, err
	}
	defer keyvalue.ClearValues(result.FailureReads)
	if result.Succeeded {
		return result.Revision, result.Revision, nil
	}
	if len(result.FailureReads) != len(conditions) || result.FailureReads[0] == nil ||
		result.FailureReads[0].ModRevision != fence.ModRevision {
		return 0, 0, errs.New(errs.KindStateConflict, "prepared router input lost its desired revision")
	}
	stored := result.FailureReads[1]
	if stored == nil || stored.Key != key || !bytes.Equal(stored.Value, value) {
		return 0, 0, errs.New(errs.KindStateConflict, "prepared router input was already staged with different content")
	}
	return stored.ModRevision, result.Revision, nil
}

func (repository *EnvironmentBlueprintRepository) validateProxyCandidateDesired(ctx context.Context,
	task workexecution.Task, projection projectionrecord.EnvironmentComposeProjection,
	desiredRootRevision, readRevision int64,
) error {
	desired, found, err := blueprints.ReadDesiredInputRevision(ctx, repository.store,
		projection.EnvironmentID, task.ID, readRevision)
	if err != nil {
		return err
	}
	if !found || desired.Revision != desiredRootRevision ||
		desired.Record.EnvironmentID != projection.EnvironmentID || desired.Record.RevisionID != task.ID ||
		desired.Record.RenderGeneration != projection.RenderGeneration {
		return errs.New(
			errs.KindValidationFailed,
			"prepared router input generation differs from its recipe",
		)
	}
	encoded, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(desired.Record)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	if hex.EncodeToString(digest[:]) != task.Intent.SHA256 {
		return errs.New(
			errs.KindStateConflict, "prepared router input does not match the accepted Blueprint")
	}

	return nil
}
