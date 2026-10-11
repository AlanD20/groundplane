package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// StageBlueprintProxyCandidateInput writes inert, immutable input chunks. The
// Component's atomic publication must subsequently compare the returned root;
// neither these writes nor the root grant an execution or applied-state receipt.
func (repository *EnvironmentBlueprintRepository) StageBlueprintProxyCandidateInput(ctx context.Context,
	execution componentplanning.ChildExecution, projection projectionrecord.EnvironmentComposeProjection,
	desiredRootRevision int64,
) (keyvalue.Versioned[componentplanning.ProxyCandidateInputReference], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	if repository == nil || desiredRootRevision <= 0 || !execution.Component.Desired.Kind.IsEnvironmentProxy() ||
		execution.EnvironmentID != projection.EnvironmentID || execution.ParentTaskID != projection.RevisionID {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, errs.New(
			errs.KindValidationFailed,
			"prepared router input staging is invalid",
		)
	}
	plan, err := execution.ExecutionPlan()
	if err != nil {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, err
	}
	if plan.GetRenderGeneration() != projection.RenderGeneration {
		return keyvalue.Versioned[componentplanning.ProxyCandidateInputReference]{}, errs.New(
			errs.KindValidationFailed,
			"prepared router input generation differs from its recipe",
		)
	}
	return repository.stageBlueprintProxyCandidateInput(ctx, componentplanning.ProxyCandidateInputReference{
		EnvironmentID: execution.EnvironmentID, ParentTaskID: execution.ParentTaskID,
		SourceID: execution.TaskID, SourceOwner: componentplanning.ProxyInputAgentTask, ComponentID: execution.Component.Desired.ID,
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
