package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
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
	ResolveReleaseScriptOwnership(context.Context, workqueue.View, string, workexecution.Input) (*etcd.ScriptWorkOwnership, error)
	ResolveScriptWorkArtifacts(context.Context, workqueue.View) (*etcd.ScriptWorkArtifacts, error)
	ResolveReleaseScriptWorkArtifacts(context.Context, workqueue.View, string, workexecution.Input) (*etcd.ScriptWorkArtifacts, error)
}

// ResolveReleaseScriptOwnership does not resolve or decrypt Script Entry values.
func (service *ScriptArtifactService) ResolveReleaseScriptOwnership(ctx context.Context, view workqueue.View, executionID string, reference workexecution.Input) (*etcd.ScriptWorkOwnership, error) {
	if ctx == nil || service == nil || service.repository == nil {
		return nil, errs.New(errs.KindInternal, "Script ownership delivery is not configured")
	}
	return service.repository.ResolveReleaseScriptOwnership(ctx, view, executionID, reference)
}

type scriptEntryValueResolver interface {
	ResolveTaskMaterializationSource(
		context.Context,
		string,
		materializationrecord.Source,
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
	sources scriptsourcequeries.ScriptExecutionSources,
) ([]*agentpb.ScriptRunnerEntryBinding, error) {
	if ctx == nil || service == nil || service.values == nil {
		return nil, errs.New(errs.KindInternal, "Script Entry binding service is not configured")
	}
	entries, err := selectedScriptEntries(sources)
	if err != nil {
		return nil, err
	}
	return service.buildScriptEntryBindings(ctx, sources, entries)
}

func (service *ScriptArtifactService) buildScriptEntryBindings(
	ctx context.Context, sources scriptsourcequeries.ScriptExecutionSources, entries []entryrecord.Record,
) ([]*agentpb.ScriptRunnerEntryBinding, error) {
	if service.values == nil {
		return nil, errs.New(errs.KindInternal, "Script Entry values are not configured")
	}
	bindings := make([]*agentpb.ScriptRunnerEntryBinding, 0, len(entries))
	for _, record := range entries {
		binding, err := scriptEntryBindingMetadata(record)
		if err != nil {
			return nil, err
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
		binding.Sha256 = append([]byte(nil), digest[:]...)
		bindings = append(bindings, binding)
	}
	sort.Slice(bindings, func(left, right int) bool { return bindings[left].EntryId < bindings[right].EntryId })
	return bindings, nil
}

func scriptEntryBindingMetadata(record entryrecord.Record) (*agentpb.ScriptRunnerEntryBinding, error) {
	binding := &agentpb.ScriptRunnerEntryBinding{
		EntryId: record.Entry.ID, ValueGenerationId: record.CurrentValueGenerationID, Secret: record.Entry.Secret,
	}
	switch record.Entry.Kind {
	case core.EntryKindEnv:
		binding.Kind = agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV
		binding.EnvironmentKey = record.Entry.Key
	case core.EntryKindFile:
		if record.Entry.UID == nil || record.Entry.GID == nil {
			return nil, errs.New(errs.KindValidationFailed, "Script Entry file ownership is required")
		}
		binding.Kind = agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE
		binding.FileTarget = path.Join("/", record.Entry.Path)
		binding.Uid, binding.Gid, binding.Mode = *record.Entry.UID, *record.Entry.GID, 0o444
		if record.Entry.Secret {
			binding.Mode = 0o600
		}
	default:
		return nil, errs.New(errs.KindInternal, "Script Entry kind is invalid")
	}
	return binding, nil
}

// ScriptWorkDelivery owns transient plaintext until the native channel consumes
// it. Its comparisons must join start authorization before any host effects.
type ScriptWorkDelivery struct {
	Captured   *etcd.ScriptWorkArtifacts
	Entries    []*agentpb.ScriptEntryArtifact
	Checkpoint *agentpb.ScriptExecutionCheckpoint
}

func (delivery *ScriptWorkDelivery) Clear() {
	if delivery == nil {
		return
	}
	delivery.Captured.Clear()
	for _, entry := range delivery.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
}

func (service *ScriptArtifactService) ResolveScriptWorkArtifacts(ctx context.Context, view workqueue.View) (*ScriptWorkDelivery, error) {
	if ctx == nil || service == nil || service.repository == nil || service.values == nil {
		return nil, errs.New(errs.KindInternal, "Script artifact service is not configured")
	}
	captured, err := service.repository.ResolveScriptWorkArtifacts(ctx, view)
	if err != nil {
		return nil, err
	}
	return service.resolveCapturedScriptValues(ctx, captured)
}

func (service *ScriptArtifactService) ResolveReleaseScriptWorkArtifacts(ctx context.Context, view workqueue.View,
	executionID string, input workexecution.Input,
) (*ScriptWorkDelivery, error) {
	if ctx == nil || service == nil || service.repository == nil || service.values == nil {
		return nil, errs.New(errs.KindInternal, "Script artifact service is not configured")
	}
	captured, err := service.repository.ResolveReleaseScriptWorkArtifacts(ctx, view, executionID, input)
	if err != nil {
		return nil, err
	}
	return service.resolveCapturedScriptValues(ctx, captured)
}

func (service *ScriptArtifactService) resolveCapturedScriptValues(ctx context.Context, captured *etcd.ScriptWorkArtifacts) (*ScriptWorkDelivery, error) {
	if captured == nil || captured.Snapshot == nil || captured.Body == nil {
		captured.Clear()
		return nil, errs.New(errs.KindInternal, "Script captured artifacts are missing")
	}
	delivery := &ScriptWorkDelivery{Captured: captured}
	fail := func(err error) (*ScriptWorkDelivery, error) { delivery.Clear(); return nil, err }
	seen := make(map[string]struct{}, len(captured.Snapshot.EntryBindings))
	for _, binding := range captured.Snapshot.EntryBindings {
		if binding == nil {
			return fail(errs.New(errs.KindInternal, "Script captured Entry binding is missing"))
		}
		identity := binding.EntryId + "\x00" + binding.ValueGenerationId
		if _, duplicate := seen[identity]; duplicate {
			return fail(errs.New(errs.KindInternal, "Script captured Entry binding is duplicated"))
		}
		seen[identity] = struct{}{}
		reference := materializationrecord.EntryValueReference{
			EntryID: binding.EntryId, ValueGenerationID: binding.ValueGenerationId,
			Storage: materializationrecord.EntryValueStoragePlain,
		}
		if binding.Secret {
			reference.Storage = materializationrecord.EntryValueStorageSecret
		}
		value, err := service.values.ResolveTaskMaterializationSource(ctx, captured.Snapshot.EnvironmentId,
			materializationrecord.Source{Kind: materializationrecord.SourceEntryValue, EntryValue: &reference})
		if err != nil {
			clear(value)
			return fail(err)
		}
		digest := sha256.Sum256(value)
		if !bytes.Equal(digest[:], binding.Sha256) ||
			binding.Kind == agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV && (!utf8.Valid(value) || bytes.IndexByte(value, 0) >= 0) {
			clear(value)
			return fail(errs.New(errs.KindInternal, "Script Entry value does not match its captured binding"))
		}
		delivery.Entries = append(delivery.Entries, &agentpb.ScriptEntryArtifact{
			Binding: proto.Clone(binding).(*agentpb.ScriptRunnerEntryBinding), Value: value,
		})
	}
	checkpoint, err := scriptExecutionCheckpointMessage(captured.Input.Execution)
	if err != nil {
		return fail(err)
	}
	delivery.Checkpoint, err = executionplan.ValidateScriptExecutionCheckpoint(checkpoint)
	if err != nil {
		return fail(err)
	}
	return delivery, nil
}

func scriptExecutionCheckpointMessage(
	record scriptexecutions.ScriptExecutionRecord,
) (*agentpb.ScriptExecutionCheckpoint, error) {
	state, ok := scriptExecutionCheckpointState(record.State)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Script execution state is invalid")
	}
	checkpoint := &agentpb.ScriptExecutionCheckpoint{
		ScriptExecutionId: record.ID, State: state, StartAuthorized: record.StartAuthorized,
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

func scriptExecutionCheckpointState(state scriptexecutions.ScriptExecutionState) (agentpb.ScriptExecutionState, bool) {
	switch state {
	case scriptexecutions.ScriptExecutionNotStarted:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_NOT_STARTED, true
	case scriptexecutions.ScriptExecutionStartAuthorized:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_START_AUTHORIZED, true
	case scriptexecutions.ScriptExecutionBodyPrepared:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_BODY_PREPARED, true
	case scriptexecutions.ScriptExecutionContainerCreated:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CONTAINER_CREATED, true
	case scriptexecutions.ScriptExecutionOutcomeRecorded:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED, true
	case scriptexecutions.ScriptExecutionCleanupProven:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN, true
	default:
		return agentpb.ScriptExecutionState_SCRIPT_EXECUTION_STATE_UNSPECIFIED, false
	}
}

func scriptOutcomeCheckpointReason(reason scriptexecutions.ScriptOutcomeReason) (agentpb.ScriptOutcomeReason, bool) {
	switch reason {
	case scriptexecutions.ScriptOutcomeNormalExit:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NORMAL_EXIT, true
	case scriptexecutions.ScriptOutcomeStartFailure:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_START_FAILURE, true
	case scriptexecutions.ScriptOutcomeRuntimeFailure:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RUNTIME_FAILURE, true
	case scriptexecutions.ScriptOutcomeTimeout:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_TIMEOUT, true
	case scriptexecutions.ScriptOutcomeAbort:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT, true
	case scriptexecutions.ScriptOutcomeAbortBeforeStart:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_ABORT_BEFORE_START, true
	case scriptexecutions.ScriptOutcomeExpiryBeforeStart:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_EXPIRY_BEFORE_START, true
	case scriptexecutions.ScriptOutcomeNoServingRelease:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE, true
	case scriptexecutions.ScriptOutcomeRecoveryInvariantFailure:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE, true
	default:
		return agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_UNSPECIFIED, false
	}
}

func (service *ScriptArtifactService) resolveScriptEntryValue(
	ctx context.Context,
	environmentID string,
	record entryrecord.Record,
) ([]byte, error) {
	storage := materializationrecord.EntryValueStoragePlain
	if record.Entry.Secret {
		storage = materializationrecord.EntryValueStorageSecret
	}
	reference := materializationrecord.EntryValueReference{
		EntryID: record.Entry.ID, ValueGenerationID: record.CurrentValueGenerationID, Storage: storage,
	}
	return service.values.ResolveTaskMaterializationSource(ctx, environmentID, materializationrecord.Source{
		Kind: materializationrecord.SourceEntryValue, EntryValue: &reference,
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
