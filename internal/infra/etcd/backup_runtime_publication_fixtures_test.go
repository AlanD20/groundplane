package etcd

import (
	context "context"
	sha256 "crypto/sha256"
	hex "encoding/hex"
	testing "testing"
	time "time"

	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppruneevidence"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
)

func backupRuntimePrunePublicationTask(
	t *testing.T,
	store *memoryHierarchyStore,
	dispatch testbackupruntime.BackupRecoveryPointPruneDispatchRecord,
	pending []testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord],
	publicationConditions []testkeyvalue.Condition,
) (TaskRecord, *agentpb.ExecutionPlan, testidempotency.IdempotencyMarker, TaskInitiation) {
	t.Helper()
	environmentEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(dispatch.EnvironmentID))
	environment, err := testhierarchy.DecodeEnvironment(environmentEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	projectEntry := mustOptionalKey(t, store, testhierarchy.ProjectKey(environment.ProjectID))
	project, err := testhierarchy.DecodeProject(projectEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := testtaskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatal(err)
	}
	task := validTaskRecord(dispatch.CreatedAt)
	task.ID = dispatch.TaskID
	task.OperationID = dispatch.OperationID
	task.Owner = owner
	task.Actor = testtaskjournal.TaskActorSystem
	task.Executor = testtaskjournal.TaskExecutorAgent
	task.Type = testtaskjournal.TaskBackupPrune
	task.Target = dispatch.EnvironmentID
	task.IdempotencyKey = "backup-prune-runtime-0001"
	sealed := backupRuntimeSealedPrunePlan(t, store, dispatch, pending, task.PlanID)
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	task.RenderGeneration = 0
	task.Params = nil
	task.Materializations = nil
	task.TimeoutSeconds = backupTaskTimeout(task.Type)
	task.Steps = make([]testtaskjournal.TaskStepRecord, len(sealed.Steps))
	for index, step := range sealed.Steps {
		task.Steps[index] = testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: step.StepId}
	}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = dispatch.EnvironmentID
	marker.Locator.Route = "/internal/backup-prunes"
	marker.Locator.Key = task.IdempotencyKey
	var initiationFence testkeyvalue.Condition
	for _, condition := range publicationConditions {
		if condition.ModRevision > 0 {
			initiationFence = condition
			break
		}
	}
	if initiationFence.Key == "" {
		t.Fatal("prune Task publication has no durable initiation fence")
	}
	initiation, err := newTaskInitiation(owner, testtaskjournal.TaskActorSystem, initiationFence)
	if err != nil {
		t.Fatal(err)
	}
	return task, sealed, marker, initiation
}

func backupRuntimeSealedPrunePlan(
	t *testing.T,
	store *memoryHierarchyStore,
	dispatch testbackupruntime.BackupRecoveryPointPruneDispatchRecord,
	pending []testkeyvalue.Versioned[testbackupruntime.BackupRecoveryPointPruneRecord],
	planID string,
) *agentpb.ExecutionPlan {
	t.Helper()
	plan := &agentpb.ExecutionPlan{
		Schema:    executionplan.SchemaVersion,
		PlanId:    planID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE,
		TargetId:  dispatch.EnvironmentID,
		Steps:     make([]*agentpb.ExecutionStep, len(pending)),
	}
	environmentEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(dispatch.EnvironmentID))
	environment, err := testhierarchy.DecodeEnvironment(environmentEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	projectEntry := mustOptionalKey(t, store, testhierarchy.ProjectKey(environment.ProjectID))
	plan.BackupScope = &agentpb.BackupPlanScope{
		ProjectId: environment.ProjectID, Project: backupRuntimeRevisionDigest(projectEntry),
		EnvironmentId: environment.ID, Environment: backupRuntimeRevisionDigest(environmentEntry), TaskAttempt: 1,
	}
	repository, err := newBackupRuntimeRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	read, err := repository.ReadCurrentKeys(
		context.Background(),
		[]string{testhierarchy.EnvironmentKey(environment.ID)},
	)
	if err != nil {
		t.Fatal(err)
	}
	testkeyvalue.ClearValues(read.Values)
	for index, version := range pending {
		point := version.Record.Point
		pointEntry := mustOptionalKey(t, store, testbackupruntime.BackupRecoveryPointKey(point.ID))
		evidence, err := backuppruneevidence.LoadPointExecutionEvidence(
			context.Background(), repository, version, pointEntry, read.ReadRevision,
		)
		if err != nil {
			t.Fatal(err)
		}
		object := backupRuntimePruneObject(t, evidence, uint32(index+1))
		authority, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
			StepId:               ids.NewAt(ids.KindStep, dispatch.CreatedAt, int64(2500+index)),
			ExecutionId:          ids.NewULID(),
			StepDeadlineUnixNano: uint64(dispatch.CreatedAt.Add(30 * time.Minute).UnixNano()),
			Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
				RetentionPolicy: proto.CloneOf(evidence.RetentionPolicy), Objects: []*agentpb.BackupPruneObject{object},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		plan.Steps[index] = &agentpb.ExecutionStep{
			StepId:         authority.StepId,
			TimeoutSeconds: executionplan.MaximumBackupPruneStepTimeoutSeconds,
			Payload:        &agentpb.ExecutionStep_BackupStep{BackupStep: authority},
		}
	}
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

func backupRuntimeRevisionDigest(entry *testkeyvalue.KeyValue) *agentpb.RevisionDigest {
	digest := sha256.Sum256(entry.Value)
	return &agentpb.RevisionDigest{ModRevision: entry.ModRevision, Sha256: digest[:]}
}

func backupRuntimePublicationTask(
	t *testing.T,
	store *memoryHierarchyStore,
	run testbackupruntime.BackupRunRecord,
) (TaskRecord, *agentpb.ExecutionPlan, testidempotency.IdempotencyMarker, TaskInitiation) {
	t.Helper()
	environmentEntry := mustOptionalKey(t, store, testhierarchy.EnvironmentKey(run.EnvironmentID))
	environment, err := testhierarchy.DecodeEnvironment(environmentEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	projectEntry := mustOptionalKey(t, store, testhierarchy.ProjectKey(environment.ProjectID))
	project, err := testhierarchy.DecodeProject(projectEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := testtaskjournal.EnvironmentTaskOwner(project, environment)
	if err != nil {
		t.Fatal(err)
	}
	task := validTaskRecord(run.CreatedAt)
	task.ID = run.TaskID
	task.OperationID = run.OperationID
	task.Owner = owner
	task.Actor = testtaskjournal.TaskActorOperator
	task.Executor = testtaskjournal.TaskExecutorAgent
	task.Type = testtaskjournal.TaskBackup
	task.Target = run.EnvironmentID
	task.IdempotencyKey = "backup-runtime-0001"
	sealed := backupRuntimeRunPlanDraft(t, run, task.PlanID, task.Owner.ProjectID)
	sealed.PlanHash = nil
	sealed.BackupScope.Project = backupRuntimeRevisionDigest(projectEntry)
	sealed.BackupScope.Environment = backupRuntimeRevisionDigest(environmentEntry)
	connectorEntry := mustOptionalKey(t, store, testconnectors.RecordKey(run.ConnectorID))
	connector, err := testconnectors.DecodeRecord(connectorEntry.Value)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := newBackupRuntimeRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	connectorAuthority, err := backuppruneevidence.LoadConnectorAuthority(context.Background(), repository,
		connector, connectorEntry, mustOptionalKey(t, store, testconnectors.CredentialValueKey(run.ConnectorID)),
		project.ID, backupRuntimeCurrentRevision(t, store, run.EnvironmentID))
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range sealed.Steps {
		authority := step.GetBackupStep()
		authority.StepDigest = nil
		authority.GetCapture().Target.Connector = proto.Clone(connectorAuthority).(*agentpb.BackupConnectorAuthority)
		authority.GetCapture().Encryption.SecretSlot = backupRuntimeRevisionDigest(
			mustOptionalKey(t, store, testbackuppolicy.BackupKeyValueKey(run.EnvironmentID)))
		if authority.GetCapture().GetPostgres() != nil {
			authority.GetCapture().Resource.Resource = backupRuntimeRevisionDigest(
				mustOptionalKey(t, store, testattachments.AttachKey(authority.GetCapture().Resource.ResourceId)))
		}
		sealedAuthority, sealErr := executionplan.SealBackupStepAuthority(authority)
		if sealErr != nil {
			t.Fatal(sealErr)
		}
		step.Payload = &agentpb.ExecutionStep_BackupStep{BackupStep: sealedAuthority}
	}
	sealed, err = executionplan.Seal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	task.RenderGeneration = 0
	task.Params = nil
	task.Materializations = nil
	task.TimeoutSeconds = backupTaskTimeoutSeconds
	task.Steps = make([]testtaskjournal.TaskStepRecord, len(sealed.Steps))
	for index, step := range sealed.Steps {
		task.Steps[index] = testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: step.StepId}
	}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = run.EnvironmentID
	marker.Locator.Route = "/environments/{environment}/backups"
	marker.Locator.Key = task.IdempotencyKey
	initiation, err := newTaskInitiation(owner, testtaskjournal.TaskActorOperator)
	if err != nil {
		t.Fatal(err)
	}
	return task, sealed, marker, initiation
}
