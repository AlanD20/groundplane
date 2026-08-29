package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"sort"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type scriptArtifactRepository interface {
	ResolveScriptAssignmentArtifacts(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
	) (*agentpb.ScriptAssignmentArtifacts, error)
	GetScriptExecution(context.Context, string) (etcd.Versioned[etcd.ScriptExecutionRecord], error)
}

type scriptEntryValueResolver interface {
	ResolveTaskMaterializationSource(
		context.Context,
		string,
		etcd.TaskMaterializationSource,
	) ([]byte, error)
}

type ScriptArtifactService struct {
	repository scriptArtifactRepository
	values     scriptEntryValueResolver
}

func NewScriptArtifactService(
	repository scriptArtifactRepository,
	values scriptEntryValueResolver,
) (*ScriptArtifactService, error) {
	if repository == nil || values == nil {
		return nil, errs.New(errs.KindInternal, "Script artifact dependencies are required")
	}
	return &ScriptArtifactService{repository: repository, values: values}, nil
}

func (service *ScriptArtifactService) BuildScriptEntryBindings(
	ctx context.Context,
	sources etcd.ScriptExecutionSources,
) ([]*agentpb.ScriptRunnerEntryBinding, error) {
	if ctx == nil || service == nil || service.values == nil {
		return nil, errs.New(errs.KindInternal, "Script Entry binding service is not configured")
	}
	bindings := make([]*agentpb.ScriptRunnerEntryBinding, 0, len(sources.AppliedProjection.Record.Entries))
	for _, record := range sources.AppliedProjection.Record.Entries {
		if !scriptEntryExposesService(record.Entry, sources.Service.Record.Desired.Name) {
			continue
		}
		value, err := service.resolveScriptEntryValue(ctx, sources.Environment.Record.ID, record)
		if err != nil {
			clear(value)
			return nil, err
		}
		if record.Entry.Kind == core.EntryKindEnv && (!utf8.Valid(value) || bytes.IndexByte(value, 0) >= 0) {
			clear(value)
			return nil, errs.New(errs.KindValidationFailed, "Script Environment Entry is not valid text")
		}
		digest := sha256.Sum256(value)
		clear(value)
		binding := &agentpb.ScriptRunnerEntryBinding{
			EntryId: record.Entry.ID, ValueGenerationId: record.CurrentValueGenerationID,
			Sha256: append([]byte(nil), digest[:]...), Secret: record.Entry.Secret,
		}
		switch record.Entry.Kind {
		case core.EntryKindEnv:
			binding.Kind = agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV
			binding.EnvironmentKey = record.Entry.Key
		case core.EntryKindFile:
			binding.Kind = agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE
			binding.FileTarget = path.Join("/", record.Entry.Path)
			binding.Uid = *record.Entry.UID
			binding.Gid = *record.Entry.GID
			binding.Mode = 0o444
			if record.Entry.Secret {
				binding.Mode = 0o600
			}
		default:
			return nil, errs.New(errs.KindInternal, "Script Entry kind is invalid")
		}
		bindings = append(bindings, binding)
	}
	sort.Slice(bindings, func(left, right int) bool { return bindings[left].EntryId < bindings[right].EntryId })
	return bindings, nil
}

func (service *ScriptArtifactService) ResolveScriptAssignmentArtifacts(
	ctx context.Context,
	task etcd.TaskRecord,
	plan *agentpb.ExecutionPlan,
) (*agentpb.ScriptAssignmentArtifacts, error) {
	if ctx == nil || service == nil || service.repository == nil || service.values == nil {
		return nil, errs.New(errs.KindInternal, "Script artifact service is not configured")
	}
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return nil, err
	}
	if len(validated.ScriptRunnerSnapshots) != 1 {
		return nil, errs.New(errs.KindInternal, "Script runner snapshot is missing")
	}
	artifacts, err := service.repository.ResolveScriptAssignmentArtifacts(ctx, task, validated)
	if err != nil {
		return nil, err
	}
	if artifacts == nil || len(artifacts.Entries) != 0 {
		clearScriptArtifactValues(artifacts)
		return nil, errs.New(errs.KindInternal, "Script body artifact response is invalid")
	}
	snapshot := validated.ScriptRunnerSnapshots[0]
	for _, binding := range snapshot.EntryBindings {
		reference := etcd.TaskEntryValueReference{
			EntryID: binding.EntryId, ValueGenerationID: binding.ValueGenerationId,
			Storage: etcd.TaskEntryValueStoragePlain,
		}
		if binding.Secret {
			reference.Storage = etcd.TaskEntryValueStorageSecret
		}
		value, resolveErr := service.values.ResolveTaskMaterializationSource(
			ctx, snapshot.EnvironmentId, etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceEntryValue, EntryValue: &reference,
			},
		)
		if resolveErr != nil {
			clear(value)
			clearScriptArtifactValues(artifacts)
			return nil, resolveErr
		}
		digest := sha256.Sum256(value)
		if !bytes.Equal(digest[:], binding.Sha256) ||
			(binding.Kind == agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV &&
				(!utf8.Valid(value) || bytes.IndexByte(value, 0) >= 0)) {
			clear(value)
			clearScriptArtifactValues(artifacts)
			return nil, errs.New(errs.KindInternal, "Script Entry artifact does not match its sealed binding")
		}
		artifacts.Entries = append(artifacts.Entries, &agentpb.ScriptEntryArtifact{
			Binding: proto.Clone(binding).(*agentpb.ScriptRunnerEntryBinding), Value: value,
		})
	}
	return artifacts, nil
}

func (service *ScriptArtifactService) ResolveScriptExecutionCheckpoint(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ScriptExecutionCheckpoint, error) {
	if ctx == nil || service == nil || service.repository == nil || task.Type != etcd.TaskScript {
		return nil, errs.New(errs.KindInternal, "Script checkpoint resolver is not configured")
	}
	executionID := task.Params[etcd.ScriptExecutionIDParam]
	versioned, err := service.repository.GetScriptExecution(ctx, executionID)
	if err != nil {
		return nil, err
	}
	record := versioned.Record
	if record.CurrentTaskID != task.ID || record.OperationID != task.OperationID || record.PlanHash != task.PlanHash ||
		record.StepID == "" {
		return nil, errs.New(errs.KindInternal, "Script execution checkpoint does not match its Task")
	}
	checkpoint, err := scriptExecutionCheckpointMessage(record)
	if err != nil {
		return nil, err
	}
	return executionplan.ValidateScriptExecutionCheckpoint(checkpoint)
}

func scriptExecutionCheckpointMessage(record etcd.ScriptExecutionRecord) (*agentpb.ScriptExecutionCheckpoint, error) {
	state, ok := scriptExecutionCheckpointState(record.State)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Script execution state is invalid")
	}
	checkpoint := &agentpb.ScriptExecutionCheckpoint{
		State: state, StartAuthorized: record.StartAuthorized,
		ReconciliationRequired: record.ReconciliationRequired,
	}
	if record.BodyPrepared != nil {
		digest, err := hex.DecodeString(record.BodyPrepared.BodySHA256)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		checkpoint.BodyPrepared = &agentpb.ScriptBodyPreparedCheckpoint{
			BodySha256: digest, Uid: record.BodyPrepared.UID, Gid: record.BodyPrepared.GID,
			Device: record.BodyPrepared.Device, Inode: record.BodyPrepared.Inode, Leaf: record.BodyPrepared.Leaf,
		}
	}
	if record.ContainerCreated != nil {
		digest, err := hex.DecodeString(record.ContainerCreated.OwnershipLabelsSHA256)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		checkpoint.ContainerCreated = &agentpb.ScriptContainerCreatedCheckpoint{
			ContainerId: record.ContainerCreated.ContainerID, OwnershipLabelsSha256: digest,
		}
	}
	if record.Outcome != nil {
		reason, ok := scriptOutcomeCheckpointReason(record.Outcome.Reason)
		if !ok {
			return nil, errs.New(errs.KindInternal, "Script outcome reason is invalid")
		}
		checkpoint.Outcome = &agentpb.ScriptOutcomeCheckpoint{
			Reason: reason, OutputTruncated: record.Outcome.OutputTruncated,
			ObservedAt: timestamppb.New(record.Outcome.ObservedAt.UTC()),
		}
		if record.Outcome.ExitCode != nil {
			checkpoint.Outcome.ExitCode = new(int32)
			*checkpoint.Outcome.ExitCode = *record.Outcome.ExitCode
		}
	}
	if record.Cleanup != nil {
		checkpoint.Cleanup = &agentpb.ScriptCleanupCheckpoint{
			ContainerId: record.Cleanup.ContainerID, BodyDevice: record.Cleanup.BodyDevice,
			BodyInode: record.Cleanup.BodyInode, BodyLeaf: record.Cleanup.BodyLeaf,
			ContainerAbsent: record.Cleanup.ContainerAbsent, BodyAbsent: record.Cleanup.BodyAbsent,
			ExecutionDirectoryAbsent: record.Cleanup.ExecutionDirectoryAbsent,
		}
	}
	return checkpoint, nil
}

func scriptExecutionCheckpointState(state etcd.ScriptExecutionState) (agentpb.ScriptExecutionState, bool) {
	switch state {
	case etcd.ScriptExecutionNotStarted:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED, true
	case etcd.ScriptExecutionStartAuthorized:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED, true
	case etcd.ScriptExecutionBodyPrepared:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED, true
	case etcd.ScriptExecutionContainerCreated:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED, true
	case etcd.ScriptExecutionOutcomeRecorded:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED, true
	case etcd.ScriptExecutionCleanupProven:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN, true
	default:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_UNSPECIFIED, false
	}
}

func scriptOutcomeCheckpointReason(reason etcd.ScriptOutcomeReason) (agentpb.ScriptOutcomeReason, bool) {
	switch reason {
	case etcd.ScriptOutcomeNormalExit:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT, true
	case etcd.ScriptOutcomeStartFailure:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_START_FAILURE, true
	case etcd.ScriptOutcomeRuntimeFailure:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE, true
	case etcd.ScriptOutcomeTimeout:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT, true
	case etcd.ScriptOutcomeAbort:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT, true
	case etcd.ScriptOutcomeAbortBeforeStart:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START, true
	case etcd.ScriptOutcomeExpiryBeforeStart:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_EXPIRY_BEFORE_START, true
	case etcd.ScriptOutcomeNoServingRelease:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE, true
	case etcd.ScriptOutcomeRecoveryInvariantFailure:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE, true
	default:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_UNSPECIFIED, false
	}
}

func (service *ScriptArtifactService) resolveScriptEntryValue(
	ctx context.Context,
	environmentID string,
	record etcd.EntryRecord,
) ([]byte, error) {
	storage := etcd.TaskEntryValueStoragePlain
	if record.Entry.Secret {
		storage = etcd.TaskEntryValueStorageSecret
	}
	reference := etcd.TaskEntryValueReference{
		EntryID: record.Entry.ID, ValueGenerationID: record.CurrentValueGenerationID, Storage: storage,
	}
	return service.values.ResolveTaskMaterializationSource(ctx, environmentID, etcd.TaskMaterializationSource{
		Kind: etcd.TaskMaterializationSourceEntryValue, EntryValue: &reference,
	})
}

func scriptEntryExposesService(entry core.EnvEntry, serviceName string) bool {
	if entry.ExposesAll() {
		return true
	}
	for _, exposed := range entry.Exposure {
		if exposed == serviceName {
			return true
		}
	}
	return false
}

func clearScriptArtifactValues(artifacts *agentpb.ScriptAssignmentArtifacts) {
	if artifacts == nil {
		return
	}
	for _, body := range artifacts.Bodies {
		if body != nil {
			clear(body.Body)
			body.Body = nil
		}
	}
	for _, entry := range artifacts.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
}
