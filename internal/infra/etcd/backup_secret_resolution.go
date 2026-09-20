package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupSecretValueEvidence is an encrypted Secret value selected by the
// project-first/platform-fallback lookup. Ciphertext is caller-owned and must
// be cleared after the controller has opened it.
type BackupSecretValueEvidence struct {
	Name      backupsecret.CredentialName
	Reference string
	Value     secretrecord.EncryptedValue
}

// BackupSecretResolutionEvidence contains only the durable proof and
// encrypted values required by the Controller resolver. No plaintext,
// Connector object locator, or age identity is returned.
type BackupSecretResolutionEvidence struct {
	ReadRevision        int64
	Task                TaskRecord
	Assignment          TaskAssignmentRecord
	Environment         hierarchyrecord.EnvironmentRecord
	EnvironmentRevision int64
	Project             hierarchyrecord.ProjectRecord
	ProjectRevision     int64
	Connector           connectorrecord.Record
	ConnectorRevision   int64
	Run                 *BackupRunRecord
	Dispatch            *BackupRecoveryPointPruneDispatchRecord
	DispatchRevision    int64
	Point               *BackupRecoveryPointRecord
	Prune               *BackupRecoveryPointPruneRecord
	Source              BackupSourceRecord
	SourceRevision      int64
	Credentials         connectorrecord.EncryptedCredentials
	HasCredentials      bool
	SecretValues        []BackupSecretValueEvidence
}

// Clear releases every encrypted buffer returned by the reader. It is safe
// to call more than once and is intentionally explicit for callers that take
// ownership before returning an error.
func (e *BackupSecretResolutionEvidence) Clear() {
	if e == nil {
		return
	}
	clear(e.Credentials.Ciphertext)
	e.Credentials.Ciphertext = nil
	for index := range e.SecretValues {
		clear(e.SecretValues[index].Value.Ciphertext)
		e.SecretValues[index].Value.Ciphertext = nil
	}
	e.SecretValues = nil
}

// BackupSecretResolutionReader reads the exact durable evidence needed by a
// backup capture or prune secret delivery. It is deliberately narrower than
// the runtime repositories so channel composition can depend on one read
// contract without acquiring lifecycle authority.
type BackupSecretResolutionReader struct {
	store backupSecretResolutionStore
	now   func() time.Time
}

type backupSecretResolutionStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func NewBackupSecretResolutionReader(store backupSecretResolutionStore) (*BackupSecretResolutionReader, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup secret resolution store is required")
	}
	return &BackupSecretResolutionReader{store: store, now: time.Now}, nil
}

func (reader *BackupSecretResolutionReader) ResolveBackupSecretEvidence(
	ctx context.Context,
	request backupsecret.Request,
) (BackupSecretResolutionEvidence, error) {
	if err := validateContext(ctx); err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	if err := validateBackupSecretResolutionRequest(request); err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	plan, err := executionplan.Validate(request.Plan)
	if err != nil {
		return BackupSecretResolutionEvidence{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	stepIndex, err := backupSecretStepIndex(plan, request.StepID)
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}

	anchor, err := reader.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskKey(request.TaskID), taskAssignmentIndexKey(request.TaskID),
	}})
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	if anchor == nil || anchor.ReadRevision <= 0 || len(anchor.Values) != 2 || anchor.Values[1] == nil {
		clearKeyValues(anchorValues(anchor))
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment evidence is unavailable",
		)
	}
	assignmentForKey, err := decodeTaskAssignment(anchor.Values[1].Value)
	clearKeyValues(anchor.Values)
	if err != nil {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindInternal,
			"backup task assignment evidence is corrupt",
		)
	}
	fixedRevision := anchor.ReadRevision

	baseKeys := []string{
		taskKey(request.TaskID),
		taskAssignmentIndexKey(request.TaskID),
		taskExecutionClaimKey(TaskExecutorAgent, request.AgentID, request.TaskID),
		taskTimeoutIndexKey(request.TaskID, assignmentForKey.Deadline),
		backupRunKey(request.TaskID),
		backupRecoveryPointPruneDispatchKey(request.TaskID),
	}
	base, err := reader.readFixed(ctx, baseKeys, fixedRevision)
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	defer clearKeyValues(base.Values)
	if base.Values[0] == nil {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindTaskNotFound,
			"backup task was not found",
		)
	}
	if base.Values[1] == nil || base.Values[2] == nil || base.Values[3] == nil ||
		base.Values[1].ModRevision != base.Values[2].ModRevision ||
		base.Values[1].ModRevision != base.Values[3].ModRevision ||
		!bytes.Equal(base.Values[1].Value, base.Values[2].Value) ||
		!bytes.Equal(base.Values[1].Value, base.Values[3].Value) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment evidence changed",
		)
	}
	task, err := decodeTaskRecord(base.Values[0].Value)
	if err != nil {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindInternal,
			"backup task evidence is corrupt",
		)
	}
	assignment, err := decodeTaskAssignment(base.Values[1].Value)
	if err != nil {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindInternal,
			"backup task assignment evidence is corrupt",
		)
	}
	if err := validateBackupSecretTaskAssignment(task, assignment, request); err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	now := reader.now
	if now == nil {
		now = time.Now
	}
	if !now().Before(assignment.Deadline) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment deadline has expired",
		)
	}
	if len(task.Params) != 0 || len(task.Materializations) != 0 {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task contains mutable execution inputs",
		)
	}
	if task.PlanID != plan.PlanId || task.PlanHash != hex.EncodeToString(plan.PlanHash) ||
		task.RenderGeneration != int32(plan.RenderGeneration) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task plan identity changed",
		)
	}
	if !proto.Equal(plan.Steps[stepIndex], request.Step) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task step evidence changed",
		)
	}
	for index, taskStep := range task.Steps {
		if index >= len(plan.Steps) || taskStep.ID != plan.Steps[index].StepId {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup task step evidence changed",
			)
		}
	}
	if len(task.Steps) != len(plan.Steps) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task step evidence changed",
		)
	}

	evidence := BackupSecretResolutionEvidence{
		ReadRevision: fixedRevision, Task: task, Assignment: assignment,
	}
	if task.Type == TaskBackup {
		if base.Values[4] == nil || base.Values[5] != nil {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup run evidence is unavailable",
			)
		}
		run, decodeErr := decodeBackupRunRecord(base.Values[4].Value)
		if decodeErr != nil {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindInternal,
				"backup run evidence is corrupt",
			)
		}
		if err := validateBackupRunTaskBinding(task, run); err != nil {
			return BackupSecretResolutionEvidence{}, err
		}
		if run.State != BackupRunQueued && run.State != BackupRunRunning {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup run is no longer active",
			)
		}
		if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup task plan operation changed",
			)
		}
		if err := validateBackupRunExecutionPlan(run, plan); err != nil {
			return BackupSecretResolutionEvidence{}, err
		}
		evidence.Run = &run
	} else {
		if base.Values[4] != nil || base.Values[5] == nil {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup prune dispatch evidence is unavailable",
			)
		}
		dispatch, decodeErr := decodeBackupRecoveryPointPruneDispatchRecord(base.Values[5].Value)
		if decodeErr != nil {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindInternal,
				"backup prune dispatch evidence is corrupt",
			)
		}
		if err := validateBackupPruneTaskBinding(task, dispatch); err != nil {
			return BackupSecretResolutionEvidence{}, err
		}
		if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup prune task plan operation changed",
			)
		}
		evidence.Dispatch = &dispatch
		evidence.DispatchRevision = base.Values[5].ModRevision
	}

	dynamic := newBackupSecretDynamicRead()
	environmentID, connectorIDs, sourceIDs, err := reader.planDynamicKeys(
		dynamic, evidence, plan, stepIndex,
	)
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	firstDynamic, err := reader.readFixed(ctx, dynamic.keys, fixedRevision)
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	defer clearKeyValues(firstDynamic.Values)
	selectedConnectorID := ""
	if evidence.Run != nil {
		selectedConnectorID = evidence.Run.ConnectorID
	} else if evidence.Dispatch != nil {
		selectedConnectorID = plan.Steps[stepIndex].GetBackupArtifactPrune().ConnectorId
	}
	if err := reader.decodeCommonDynamicEvidence(
		firstDynamic, dynamic, &evidence, environmentID, connectorIDs, selectedConnectorID,
	); err != nil {
		evidence.Clear()
		return BackupSecretResolutionEvidence{}, err
	}
	if err := reader.decodeSourceDynamicEvidence(
		firstDynamic, dynamic, &evidence, plan, stepIndex, sourceIDs,
	); err != nil {
		evidence.Clear()
		return BackupSecretResolutionEvidence{}, err
	}
	if evidence.Dispatch != nil {
		if err := reader.decodePruneDynamicEvidence(
			ctx, firstDynamic, dynamic, &evidence, plan, stepIndex,
		); err != nil {
			evidence.Clear()
			return BackupSecretResolutionEvidence{}, err
		}
	}

	secondDynamic, err := reader.readFixed(ctx, dynamic.keys, fixedRevision)
	if err != nil {
		evidence.Clear()
		return BackupSecretResolutionEvidence{}, err
	}
	defer clearKeyValues(secondDynamic.Values)
	if err := reader.resolveEncryptedCredentialValues(
		ctx, fixedRevision, dynamic, &evidence, secondDynamic,
	); err != nil {
		evidence.Clear()
		return BackupSecretResolutionEvidence{}, err
	}
	return evidence, nil
}

func validateBackupSecretResolutionRequest(request backupsecret.Request) error {
	if ids.Validate(ids.KindTask, request.TaskID) != nil ||
		ids.Validate(ids.KindAssignment, request.AssignmentID) != nil ||
		ids.Validate(ids.KindAgent, request.AgentID) != nil || request.AgentGeneration == 0 ||
		request.Deadline.IsZero() || !request.Deadline.Equal(request.Deadline.UTC()) ||
		ids.Validate(ids.KindStep, request.StepID) != nil || request.Plan == nil || request.Step == nil ||
		request.Step.StepId != request.StepID {
		return errs.New(errs.KindValidationFailed, "backup secret resolution identity is invalid")
	}
	return nil
}

func validateBackupSecretTaskAssignment(
	task TaskRecord,
	assignment TaskAssignmentRecord,
	request backupsecret.Request,
) error {
	if task.ID != request.TaskID || task.Type != TaskBackup && task.Type != TaskBackupPrune ||
		task.Status != TaskStatusRunning || task.Executor != TaskExecutorAgent ||
		assignment.AssignmentID != request.AssignmentID || assignment.TaskID != task.ID ||
		assignment.Executor != TaskExecutorAgent || assignment.AgentID != request.AgentID ||
		assignment.AgentGeneration != request.AgentGeneration ||
		!assignment.Deadline.Equal(request.Deadline) || task.StartedAt == nil ||
		!task.StartedAt.Equal(assignment.AssignedAt) ||
		!assignment.Deadline.Equal(assignment.AssignedAt.Add(
			time.Duration(task.TimeoutSeconds)*time.Second,
		)) {
		return errs.New(errs.KindStateConflict, "backup task assignment is not active")
	}
	return nil
}

func backupSecretStepIndex(plan *agentpb.ExecutionPlan, stepID string) (int, error) {
	index := -1
	for position, step := range plan.Steps {
		if step != nil && step.StepId == stepID {
			if index >= 0 {
				return 0, errs.New(errs.KindValidationFailed, "backup task step identity is duplicated")
			}
			index = position
		}
	}
	if index < 0 {
		return 0, errs.New(errs.KindStateConflict, "backup task step is not in its sealed plan")
	}
	return index, nil
}

type backupSecretDynamicRead struct {
	keys  []string
	index map[string]int

	environment      int
	project          int
	environmentFence int
	projectFence     int
	connectors       map[string]int
	connectorFences  map[string]int
	credentials      map[string]int
	sources          map[string]int
	points           map[string]int
	prunes           map[string]int
	secretIndexes    []backupSecretIndexRead
	secretRecords    map[string]int
	secretFences     map[string]int
	secretValues     map[string]int
	last             *etcdstore.GetManyResult
}

type backupSecretIndexRead struct {
	name          backupsecret.CredentialName
	reference     string
	projectIndex  int
	platformIndex int
}

func newBackupSecretDynamicRead() *backupSecretDynamicRead {
	return &backupSecretDynamicRead{
		index: make(map[string]int), connectors: make(map[string]int),
		connectorFences: make(map[string]int), credentials: make(map[string]int),
		sources: make(map[string]int), points: make(map[string]int),
		prunes: make(map[string]int), secretRecords: make(map[string]int),
		secretFences: make(map[string]int), secretValues: make(map[string]int),
	}
}

func (dynamic *backupSecretDynamicRead) add(key string) int {
	if position, ok := dynamic.index[key]; ok {
		return position
	}
	position := len(dynamic.keys)
	dynamic.keys = append(dynamic.keys, key)
	dynamic.index[key] = position
	return position
}

func (reader *BackupSecretResolutionReader) planDynamicKeys(
	dynamic *backupSecretDynamicRead,
	evidence BackupSecretResolutionEvidence,
	plan *agentpb.ExecutionPlan,
	stepIndex int,
) (string, map[string]int, map[string]int, error) {
	environmentID := ""
	connectorIDs := make(map[string]int)
	sourceIDs := make(map[string]int)
	if evidence.Run != nil {
		environmentID = evidence.Run.EnvironmentID
		for _, source := range evidence.Run.Sources {
			position := dynamic.add(backupSourceKey(source.SourceID))
			sourceIDs[source.SourceID] = position
			dynamic.sources[source.SourceID] = position
		}
		position := dynamic.add(connectorrecord.RecordKey(evidence.Run.ConnectorID))
		connectorIDs[evidence.Run.ConnectorID] = position
		dynamic.connectors[evidence.Run.ConnectorID] = position
	} else if evidence.Dispatch != nil {
		environmentID = evidence.Dispatch.EnvironmentID
		for index, pointID := range evidence.Dispatch.RecoveryPointIDs {
			dynamic.points[pointID] = dynamic.add(backupRecoveryPointKey(pointID))
			dynamic.prunes[pointID] = dynamic.add(backupRecoveryPointPruneKey(pointID))
			if index < len(plan.Steps) {
				prune := plan.Steps[index].GetBackupArtifactPrune()
				if prune != nil {
					sourcePosition := dynamic.add(backupSourceKey(prune.SourceId))
					sourceIDs[prune.SourceId] = sourcePosition
					dynamic.sources[prune.SourceId] = sourcePosition
					connectorPosition := dynamic.add(connectorrecord.RecordKey(prune.ConnectorId))
					connectorIDs[prune.ConnectorId] = connectorPosition
					dynamic.connectors[prune.ConnectorId] = connectorPosition
				}
			}
		}
	}
	if environmentID == "" || stepIndex < 0 || stepIndex >= len(plan.Steps) {
		return "", nil, nil, errs.New(errs.KindInternal, "backup secret resolution scope is incomplete")
	}
	dynamic.environment = dynamic.add(hierarchyrecord.EnvironmentKey(environmentID))
	dynamic.environmentFence = dynamic.add(
		deletionTombstoneKey(string(DeletionTargetEnvironment), environmentID),
	)
	for connectorID := range connectorIDs {
		dynamic.connectorFences[connectorID] = dynamic.add(
			deletionTombstoneKey(string(DeletionTargetConnector), connectorID),
		)
		dynamic.credentials[connectorID] = dynamic.add(connectorrecord.CredentialValueKey(connectorID))
	}
	if evidence.Run != nil {
		source := evidence.Run.Sources[stepIndex]
		switch source.Kind {
		case BackupRuntimeSourceAttach:
			snapshot := source.Snapshot.Postgres
			if snapshot == nil {
				return "", nil, nil, errs.New(errs.KindInternal, "postgres backup source snapshot is corrupt")
			}
			dynamic.add(attachrecord.AttachKey(source.TargetID))
			dynamic.add(attachrecord.AttachFactsKey(source.TargetID))
			dynamic.add(hierarchyrecord.ProjectKey(snapshot.BackingProjectID))
			dynamic.add(hierarchyrecord.EnvironmentKey(snapshot.BackingEnvironmentID))
			dynamic.add(environmentBlueprintHeadKey(snapshot.BackingEnvironmentID))
		case BackupRuntimeSourceVolume:
			snapshot := source.Snapshot.Volume
			if snapshot == nil {
				return "", nil, nil, errs.New(errs.KindInternal, "volume backup source snapshot is corrupt")
			}
			dynamic.add(environmentBlueprintHeadKey(snapshot.EnvironmentID))
			dynamic.add(environmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID))
			for _, service := range snapshot.Services {
				dynamic.add(serviceRuntimeKey(service.ServiceID))
			}
		case BackupRuntimeSourceConfig:
			if source.Snapshot.Config == nil {
				return "", nil, nil, errs.New(errs.KindInternal, "config backup source snapshot is corrupt")
			}
			// The target environment is already part of the common evidence.
			dynamic.add(hierarchyrecord.EnvironmentKey(environmentID))
		default:
			return "", nil, nil, errs.New(errs.KindInternal, "backup source kind is corrupt")
		}
	}
	return environmentID, connectorIDs, sourceIDs, nil
}

func (reader *BackupSecretResolutionReader) readFixed(
	ctx context.Context,
	keys []string,
	revision int64,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "backup secret fixed read is invalid")
	}
	combined := &etcdstore.GetManyResult{Values: make([]*etcdstore.KeyValue, 0, len(keys)), ReadRevision: revision}
	for start := 0; start < len(keys); start += etcdstore.MaximumOperations {
		end := min(start+etcdstore.MaximumOperations, len(keys))
		batch := keys[start:end]
		result, err := getBackupSecretManyOwned(ctx, reader.store, batch, revision)
		if err != nil {
			clearKeyValues(combined.Values)
			return nil, err
		}
		combined.Values = append(combined.Values, result.Values...)
		combined.ResponseRevision = max(combined.ResponseRevision, result.ResponseRevision)
		result.Values = nil
	}
	return combined, nil
}

func anchorValues(result *etcdstore.GetManyResult) []*etcdstore.KeyValue {
	if result == nil {
		return nil
	}
	return result.Values
}

func (reader *BackupSecretResolutionReader) decodeCommonDynamicEvidence(
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	evidence *BackupSecretResolutionEvidence,
	environmentID string,
	connectorIDs map[string]int,
	selectedConnectorID string,
) error {
	environmentValue := result.Values[dynamic.environment]
	projectKeyID := ""
	if environmentValue == nil {
		return errs.New(errs.KindStateConflict, "backup environment evidence is unavailable")
	}
	environment, err := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	if err != nil || environment.ID != environmentID {
		return errs.New(errs.KindInternal, "backup environment evidence is corrupt")
	}
	evidence.Environment = environment
	evidence.EnvironmentRevision = environmentValue.ModRevision
	projectKeyID = environment.ProjectID
	dynamic.project = dynamic.add(hierarchyrecord.ProjectKey(projectKeyID))
	dynamic.projectFence = dynamic.add(
		deletionTombstoneKey(string(DeletionTargetProject), projectKeyID),
	)
	if err := requireNoDeletionFence(
		result.Values[dynamic.environmentFence], DeletionTargetEnvironment, environmentID,
	); err != nil {
		return err
	}
	for connectorID, position := range connectorIDs {
		value := result.Values[position]
		if value == nil {
			return errs.New(errs.KindStateConflict, "backup connector evidence is unavailable")
		}
		connector, decodeErr := connectorrecord.DecodeRecord(value.Value)
		if decodeErr != nil || connector.Connector.ID != connectorID ||
			connector.Connector.EnvironmentID != environmentID {
			return errs.New(errs.KindInternal, "backup connector evidence is corrupt")
		}
		if err := requireNoDeletionFence(
			result.Values[dynamic.connectorFences[connectorID]], DeletionTargetConnector, connectorID,
		); err != nil {
			return err
		}
		if connectorID == selectedConnectorID {
			if evidence.Run != nil && value.ModRevision != evidence.Run.ConnectorRevision {
				return errs.New(errs.KindStateConflict, "backup connector snapshot changed")
			}
			evidence.Connector = connector
			evidence.ConnectorRevision = value.ModRevision
		}
	}
	if evidence.Connector.Connector.ID != selectedConnectorID {
		return errs.New(errs.KindStateConflict, "backup connector evidence is unavailable")
	}
	for _, name := range []backupsecret.CredentialName{
		backupsecret.CredentialAccessKey, backupsecret.CredentialSecretKey,
	} {
		credential := evidence.Connector.Connector.Credentials[name]
		if credential.Kind == backupsecret.CredentialSourceSecretRef {
			hierarchyrecord.ProjectKey := secretKeyIndexKey(backupsecret.SecretScopeProject, projectKeyID, credential.SecretRef)
			platformKey := secretKeyIndexKey(backupsecret.SecretScopePlatform, "", credential.SecretRef)
			dynamic.secretIndexes = append(dynamic.secretIndexes, backupSecretIndexRead{
				name: name, reference: credential.SecretRef,
				projectIndex: dynamic.add(hierarchyrecord.ProjectKey), platformIndex: dynamic.add(platformKey),
			})
		}
	}
	return nil
}

func requireNoDeletionFence(value *etcdstore.KeyValue, targetKind DeletionTargetKind, stableID string) error {
	if value == nil {
		return nil
	}
	tombstone, err := decodeDeletionTombstone(value.Value)
	if err != nil || tombstone.TargetKind != targetKind || tombstone.TargetID != stableID {
		return errs.New(errs.KindInternal, "deletion fence evidence is corrupt")
	}
	return errs.New(errs.KindStateConflict, "backup resource deletion is in progress")
}

func (reader *BackupSecretResolutionReader) decodeSourceDynamicEvidence(
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	evidence *BackupSecretResolutionEvidence,
	plan *agentpb.ExecutionPlan,
	stepIndex int,
	sourceIDs map[string]int,
) error {
	if evidence.Run == nil {
		return nil
	}
	if stepIndex >= len(evidence.Run.Sources) {
		return errs.New(errs.KindStateConflict, "backup source evidence is unavailable")
	}
	source := evidence.Run.Sources[stepIndex]
	value := result.Values[sourceIDs[source.SourceID]]
	if value == nil || value.ModRevision != source.SourceRevision {
		return errs.New(errs.KindStateConflict, "backup source evidence changed")
	}
	stored, err := decodeBackupSourceRecord(value.Value)
	if err != nil || stored.ID != source.SourceID || stored.EnvironmentID != evidence.Run.EnvironmentID ||
		string(stored.Kind) != string(source.Kind) || stored.TargetID != source.TargetID {
		return errs.New(errs.KindStateConflict, "backup source evidence changed")
	}
	evidence.Source = stored
	evidence.SourceRevision = value.ModRevision
	step := plan.Steps[stepIndex].GetBackupSourceCapture()
	if step == nil {
		return errs.New(errs.KindStateConflict, "backup source step evidence changed")
	}
	if err := reader.validateCaptureTargetEvidence(result, dynamic, evidence.Run, source, step); err != nil {
		return err
	}
	return nil
}

func (reader *BackupSecretResolutionReader) validateCaptureTargetEvidence(
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	run *BackupRunRecord,
	source BackupRunSourceAttemptRecord,
	step *agentpb.BackupSourceCapture,
) error {
	switch source.Kind {
	case BackupRuntimeSourceAttach:
		snapshot := source.Snapshot.Postgres
		if snapshot == nil {
			return errs.New(errs.KindInternal, "postgres backup source snapshot is corrupt")
		}
		keys := []*etcdstore.KeyValue{
			result.Values[dynamic.index[attachrecord.AttachKey(source.TargetID)]],
			result.Values[dynamic.index[attachrecord.AttachFactsKey(source.TargetID)]],
			result.Values[dynamic.index[hierarchyrecord.ProjectKey(snapshot.BackingProjectID)]],
			result.Values[dynamic.index[hierarchyrecord.EnvironmentKey(snapshot.BackingEnvironmentID)]],
			result.Values[dynamic.index[environmentBlueprintHeadKey(snapshot.BackingEnvironmentID)]],
		}
		return validateBackupPostgresPublicationEvidence(keys, source, *snapshot)
	case BackupRuntimeSourceVolume:
		snapshot := source.Snapshot.Volume
		if snapshot == nil {
			return errs.New(errs.KindInternal, "volume backup source snapshot is corrupt")
		}
		keys := make([]*etcdstore.KeyValue, 0, len(snapshot.Services)+3)
		keys = append(
			keys,
			result.Values[dynamic.index[hierarchyrecord.EnvironmentKey(snapshot.EnvironmentID)]],
			result.Values[dynamic.index[environmentBlueprintHeadKey(snapshot.EnvironmentID)]],
			result.Values[dynamic.index[environmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID)]],
		)
		for _, service := range snapshot.Services {
			keys = append(keys, result.Values[dynamic.index[serviceRuntimeKey(service.ServiceID)]])
		}
		return validateBackupVolumePublicationEvidence(keys, source, *snapshot)
	case BackupRuntimeSourceConfig:
		snapshot := source.Snapshot.Config
		config := step.GetConfig()
		if snapshot == nil || config == nil || snapshot.ConfigSnapshotID != run.TaskID ||
			config.SnapshotRevision != uint64(snapshot.ReadRevision) {
			return errs.New(errs.KindStateConflict, "backup config snapshot revision changed")
		}
		target := result.Values[dynamic.index[hierarchyrecord.EnvironmentKey(run.EnvironmentID)]]
		if target == nil || target.ModRevision != source.TargetRevision {
			return errs.New(errs.KindStateConflict, "backup config target evidence changed")
		}
		return nil
	default:
		return errs.New(errs.KindInternal, "backup source kind is corrupt")
	}
}

func (reader *BackupSecretResolutionReader) decodePruneDynamicEvidence(
	ctx context.Context,
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	evidence *BackupSecretResolutionEvidence,
	plan *agentpb.ExecutionPlan,
	stepIndex int,
) error {
	dispatch := evidence.Dispatch
	if dispatch == nil {
		return errs.New(errs.KindInternal, "backup prune dispatch evidence is missing")
	}
	items := make([]backupPruneExecutionEvidence, len(dispatch.RecoveryPointIDs))
	for index, pointID := range dispatch.RecoveryPointIDs {
		planned := plan.Steps[index].GetBackupArtifactPrune()
		if planned == nil || planned.PruneRevision == 0 {
			return errs.New(errs.KindStateConflict, "backup prune sealed authority is unavailable")
		}
		pointValue := result.Values[dynamic.points[pointID]]
		pruneValue := result.Values[dynamic.prunes[pointID]]
		if pointValue == nil || pruneValue == nil {
			return errs.New(errs.KindStateConflict, "backup prune point evidence is unavailable")
		}
		point, pointErr := decodeBackupRecoveryPointRecord(pointValue.Value)
		prune, pruneErr := decodeBackupRecoveryPointPruneRecord(pruneValue.Value)
		if pointErr != nil || pruneErr != nil || prune.Point != point.BackupRecoveryPointSnapshot ||
			prune.PointRevision != pointValue.ModRevision ||
			prune.Point.ID != pointID || prune.OperationID != dispatch.OperationID ||
			prune.TaskID != dispatch.TaskID || prune.State != BackupPruneAssigned ||
			prune.Point.EnvironmentID != dispatch.EnvironmentID ||
			pruneValue.ModRevision != evidence.DispatchRevision {
			return errs.New(errs.KindStateConflict, "backup prune point evidence changed")
		}
		sealed, err := reader.readFixed(
			ctx, []string{backupRecoveryPointPruneKey(pointID)}, int64(planned.PruneRevision),
		)
		if err != nil {
			return err
		}
		sealedValue := sealed.Values[0]
		if sealedValue == nil || sealedValue.ModRevision != int64(planned.PruneRevision) {
			clearKeyValues(sealed.Values)
			return errs.New(errs.KindStateConflict, "backup prune sealed authority changed")
		}
		sealedPrune, sealedErr := decodeBackupRecoveryPointPruneRecord(sealedValue.Value)
		clearKeyValues(sealed.Values)
		if sealedErr != nil || sealedPrune.Point != prune.Point ||
			sealedPrune.PointRevision != prune.PointRevision ||
			sealedPrune.OperationID != prune.OperationID || sealedPrune.TaskID != "" ||
			sealedPrune.State != BackupPrunePending {
			return errs.New(errs.KindStateConflict, "backup prune sealed authority changed")
		}
		sourceValue := result.Values[dynamic.sources[point.SourceID]]
		environmentValue := result.Values[dynamic.environment]
		connectorValue := result.Values[dynamic.connectors[point.ConnectorID]]
		if sourceValue == nil || environmentValue == nil || connectorValue == nil {
			return errs.New(errs.KindStateConflict, "backup prune authority evidence is unavailable")
		}
		source, sourceErr := decodeBackupSourceRecord(sourceValue.Value)
		environment, environmentErr := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
		connector, connectorErr := connectorrecord.DecodeRecord(connectorValue.Value)
		if sourceErr != nil {
			return errs.New(errs.KindStateConflict, "backup prune source authority is corrupt")
		}
		if environmentErr != nil {
			return errs.New(errs.KindStateConflict, "backup prune Environment authority is corrupt")
		}
		if connectorErr != nil {
			return errs.New(errs.KindStateConflict, "backup prune Connector authority is corrupt")
		}
		if source.ID != point.SourceID || source.EnvironmentID != point.EnvironmentID ||
			string(source.Kind) != string(point.SourceKind) || source.TargetID != point.TargetID {
			return errs.New(errs.KindStateConflict, "backup prune source authority changed")
		}
		if environment.ID != point.EnvironmentID {
			return errs.New(errs.KindStateConflict, "backup prune Environment authority changed")
		}
		if connector.Connector.ID != point.ConnectorID ||
			connector.Connector.EnvironmentID != point.EnvironmentID {
			return errs.New(errs.KindStateConflict, "backup prune Connector authority changed")
		}
		if err := requireNoDeletionFence(
			result.Values[dynamic.connectorFences[point.ConnectorID]], DeletionTargetConnector, point.ConnectorID,
		); err != nil {
			return err
		}
		items[index] = backupPruneExecutionEvidence{
			prune: prune, pruneRevision: int64(planned.PruneRevision),
			pointRevision: pointValue.ModRevision, sourceRevision: sourceValue.ModRevision,
			environmentRevision: environmentValue.ModRevision,
			connectorRevision:   connectorValue.ModRevision,
			connectorEndpoint:   connector.Connector.Endpoint, connectorBucket: connector.Connector.Bucket,
			connectorPrefix: connector.Connector.Prefix, connectorRegion: connector.Connector.Region,
			connectorPathStyle: connector.Connector.PathStyle,
		}
	}
	if err := validateBackupPruneExecutionPlan(*dispatch, items, plan); err != nil {
		return err
	}
	selected := plan.Steps[stepIndex]
	prune := selected.GetBackupArtifactPrune()
	if prune == nil {
		return errs.New(errs.KindStateConflict, "backup prune step evidence changed")
	}
	for index, pointID := range dispatch.RecoveryPointIDs {
		if pointID == prune.PointId {
			pointValue := result.Values[dynamic.points[pointID]]
			pruneValue := result.Values[dynamic.prunes[pointID]]
			point, _ := decodeBackupRecoveryPointRecord(pointValue.Value)
			pruneRecord, _ := decodeBackupRecoveryPointPruneRecord(pruneValue.Value)
			evidence.Point = &point
			evidence.Prune = &pruneRecord
			evidence.Source = mustDecodeBackupSource(result.Values[dynamic.sources[point.SourceID]])
			evidence.SourceRevision = items[index].sourceRevision
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "backup prune point is not in its dispatch")
}

func mustDecodeBackupSource(value *etcdstore.KeyValue) BackupSourceRecord {
	if value == nil {
		return BackupSourceRecord{}
	}
	record, _ := decodeBackupSourceRecord(value.Value)
	return record
}

func (reader *BackupSecretResolutionReader) resolveEncryptedCredentialValues(
	ctx context.Context,
	fixedRevision int64,
	dynamic *backupSecretDynamicRead,
	evidence *BackupSecretResolutionEvidence,
	second *etcdstore.GetManyResult,
) error {
	if dynamic.project < 0 || second.Values[dynamic.project] == nil {
		return errs.New(errs.KindStateConflict, "backup project evidence is unavailable")
	}
	projectValue := second.Values[dynamic.project]
	project, err := hierarchyrecord.DecodeProject(projectValue.Value)
	if err != nil || project.ID != evidence.Environment.ProjectID {
		return errs.New(errs.KindInternal, "backup project evidence is corrupt")
	}
	if err := requireNoDeletionFence(
		second.Values[dynamic.projectFence], DeletionTargetProject, project.ID,
	); err != nil {
		return err
	}
	evidence.Project = project
	evidence.ProjectRevision = projectValue.ModRevision
	dynamic.last = second
	connector := evidence.Connector.Connector
	if evidence.Run != nil {
		if connectorrecord.HasDirectCredentials(evidence.Connector) != evidence.Run.ConnectorHasDirectCredentials {
			return errs.New(errs.KindStateConflict, "backup connector credential mode changed")
		}
	}
	if connectorrecord.HasDirectCredentials(evidence.Connector) {
		position, ok := dynamic.credentials[connector.ID]
		if !ok {
			return errs.New(errs.KindInternal, "backup connector credential evidence is unavailable")
		}
		value := second.Values[position]
		expectedRevision := evidence.ConnectorRevision
		if evidence.Run != nil {
			if evidence.Run.ConnectorCredentialsRevision <= 0 {
				return errs.New(errs.KindStateConflict, "backup connector credential snapshot is missing")
			}
			expectedRevision = evidence.Run.ConnectorCredentialsRevision
		}
		if value == nil || value.ModRevision != expectedRevision {
			return errs.New(errs.KindStateConflict, "backup connector credential snapshot changed")
		}
		credentials, err := connectorrecord.DecodeEncryptedCredentials(value.Value)
		if err != nil || credentials.ConnectorID != connector.ID {
			return errs.New(errs.KindInternal, "backup connector credential evidence is corrupt")
		}
		evidence.Credentials = credentials
		evidence.HasCredentials = true
	}

	refs := dynamic.secretIndexes
	if len(refs) == 0 {
		return nil
	}
	secretPlans := make([]backupSecretIndexRead, len(refs))
	for index, reference := range refs {
		projectIndexValue := second.Values[reference.projectIndex]
		platformIndexValue := second.Values[reference.platformIndex]
		for _, candidate := range []*etcdstore.KeyValue{projectIndexValue, platformIndexValue} {
			if candidate != nil && ids.Validate(ids.KindSecret, string(candidate.Value)) != nil {
				return errs.New(errs.KindInternal, "backup Secret key index is corrupt")
			}
		}
		secretPlans[index] = reference
		for _, candidate := range []*etcdstore.KeyValue{projectIndexValue, platformIndexValue} {
			if candidate == nil {
				continue
			}
			id := string(candidate.Value)
			dynamic.secretRecords[id] = dynamic.add(secretrecord.RecordKey(id))
			dynamic.secretFences[id] = dynamic.add(
				deletionTombstoneKey(string(DeletionTargetSecret), id),
			)
			dynamic.secretValues[id] = dynamic.add(secretrecord.ValueKey(id))
		}
	}
	final, err := reader.readFixed(ctx, dynamic.keys, fixedRevision)
	if err != nil {
		return err
	}
	defer clearKeyValues(final.Values)
	dynamic.last = final
	for _, reference := range secretPlans {
		selected, err := selectBackupSecretCandidate(final.Values, dynamic, reference, evidence.Project.ID)
		if err != nil {
			return err
		}
		value := final.Values[dynamic.secretValues[selected.id]]
		if value == nil {
			return errs.New(errs.KindInternal, "backup Secret encrypted value is unavailable")
		}
		encrypted, decodeErr := secretrecord.DecodeEncryptedValue(value.Value)
		if decodeErr != nil || encrypted.SecretID != selected.id {
			return errs.New(errs.KindInternal, "backup Secret encrypted value is corrupt")
		}
		evidence.SecretValues = append(evidence.SecretValues, BackupSecretValueEvidence{
			Name: reference.name, Reference: reference.reference, Value: encrypted,
		})
	}
	return nil
}

type backupSecretCandidate struct {
	id      string
	project bool
}

func selectBackupSecretCandidate(
	values []*etcdstore.KeyValue,
	dynamic *backupSecretDynamicRead,
	reference backupSecretIndexRead,
	projectID string,
) (backupSecretCandidate, error) {
	candidates := []struct {
		value   *etcdstore.KeyValue
		project bool
	}{
		{value: values[reference.projectIndex], project: true},
		{value: values[reference.platformIndex], project: false},
	}
	for _, candidate := range candidates {
		if candidate.value == nil {
			continue
		}
		id := string(candidate.value.Value)
		fence := values[dynamic.secretFences[id]]
		if fence != nil {
			if err := validateSecretDeletionFence(fence, id); err != nil {
				return backupSecretCandidate{}, err
			}
			continue
		}
		recordValue := values[dynamic.secretRecords[id]]
		if recordValue == nil {
			return backupSecretCandidate{}, errs.New(errs.KindInternal, "backup Secret record is unavailable")
		}
		record, err := secretrecord.DecodeRecord(recordValue.Value)
		if err != nil || record.Secret.ID != id || record.Secret.Key != reference.reference ||
			record.Secret.Kind != backupsecret.SecretKindEnvVar ||
			(candidate.project && (record.Secret.Scope != backupsecret.SecretScopeProject || record.Secret.ProjectID != projectID)) ||
			(!candidate.project && (record.Secret.Scope != backupsecret.SecretScopePlatform || record.Secret.ProjectID != "")) {
			return backupSecretCandidate{}, errs.New(errs.KindInternal, "backup Secret record is corrupt")
		}
		return backupSecretCandidate{id: id, project: candidate.project}, nil
	}
	return backupSecretCandidate{}, errs.New(errs.KindSecretNotFound, "backup Secret was not found in scope")
}
