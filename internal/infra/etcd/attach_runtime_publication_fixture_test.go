package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testattachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

type attachRuntimeTestPreparation func(testattachrender.AttachTaskRenderInput, TaskRecord, testidempotency.IdempotencyMarker) (testattachrender.AttachTaskRenderInput, TaskRecord)

func createTestAttach(
	t *testing.T,
	ctx context.Context,
	repository *AttachRepository,
	scope AttachCreateScope,
	record testattachments.Record,
	facts *testattachments.EncryptedFacts,
	prepareRuntime ...attachRuntimeTestPreparation,
) testkeyvalue.Versioned[testattachments.Record] {
	t.Helper()
	hierarchy, err := NewHierarchyRepository(repository.store)
	if err != nil {
		t.Fatalf("NewHierarchyRepository() error = %v", err)
	}
	scope.Environment, err = hierarchy.GetEnvironment(ctx, scope.Environment.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	recordDigest := sha256.Sum256([]byte(record.ID))
	seed := int64(binary.BigEndian.Uint64(recordDigest[:8]))
	planDigest := sha256.Sum256([]byte("attach-plan-" + record.ID))
	stepCount := len(record.GrantAttachIDs) + 2
	if scope.BackingService.Record.Desired.Adapter == "custom" ||
		scope.BackingService.Record.Desired.Authentication == core.BackingAuthenticationNone {
		stepCount = 1
	}
	steps := make([]testtaskjournal.TaskStepRecord, stepCount)
	for index := range steps {
		steps[index] = testtaskjournal.TaskStepRecord{
			Kind: testtaskjournal.TaskStepOperation,
			ID:   ids.NewAt(ids.KindStep, record.CreatedAt, seed+2+int64(index)),
		}
	}
	owner, err := testtaskjournal.EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task := TaskRecord{
		ID:             record.TaskID,
		OperationID:    ids.NewAt(ids.KindOperation, record.CreatedAt, seed),
		IdempotencyKey: "attach-create-key-" + record.ID,
		Owner:          owner,
		Actor:          testtaskjournal.TaskActorOperator,
		Executor:       testtaskjournal.TaskExecutorAgent,
		PlanID:         ids.NewAt(ids.KindPlan, record.CreatedAt, seed+1),
		PlanHash: hex.EncodeToString(
			planDigest[:],
		),
		RenderGeneration:  int32(scope.ComposeProjection.Record.RenderGeneration),
		Type:              testtaskjournal.TaskAttach,
		Target:            record.ID,
		Params:            map[string]string{testtaskjournal.TaskMutationEnvironmentParam: record.EnvironmentID},
		Steps:             steps,
		TimeoutSeconds:    120,
		Status:            testtaskjournal.TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         record.CreatedAt,
		UpdatedAt:         record.CreatedAt,
	}
	responseBody, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: task.ID})
	if err != nil {
		t.Fatalf("json.Marshal(TaskAccepted) error = %v", err)
	}
	intentCiphertext := []byte("protected-attach-intent-" + record.ID)
	intentDigest := sha256.Sum256(intentCiphertext)
	marker := testidempotency.IdempotencyMarker{
		Kind: testidempotency.IdempotencyMarkerTask, State: testidempotency.IdempotencyMarkerPending,
		Locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: record.EnvironmentID,
			Method: http.MethodPost, Route: "/attaches", Key: task.IdempotencyKey,
		},
		Intent: testidempotency.ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(intentDigest[:]), Ciphertext: intentCiphertext,
		},
		Response: testidempotency.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: responseBody,
		},
		TaskID: task.ID, CreatedAt: record.CreatedAt, UpdatedAt: record.CreatedAt,
	}
	renderInput := testattachrender.AttachTaskRenderInput{
		PlanID: task.PlanID, AttachID: record.ID, AttachName: record.Name,
		TenantID: scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: record.EnvironmentID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir:      scope.Environment.Record.VolumeDir,
		BackingServiceID:         scope.BackingService.Record.Desired.ID,
		BackingProjectID:         record.BackingProjectID,
		AdapterKey:               scope.BackingService.Record.Desired.Adapter,
		Authentication:           scope.BackingService.Record.Desired.Authentication,
		DesiredRevisionID:        scope.DesiredHead.Record.RevisionID,
		ArtifactID:               ids.NewAt(ids.KindConfig, record.CreatedAt, seed+2000),
		RenderGeneration:         scope.ComposeProjection.Record.RenderGeneration,
		EnvironmentEpochRevision: attachTestEpochRevision(t, ctx, repository.store, record.EnvironmentID),
		RuntimeProjection:        scope.ComposeProjection.Record,
		RuntimePreparation:       configuredAttachRuntimePreparation(task, record.EnvironmentID),
		Services: testattachrender.AttachTaskServiceSnapshots(
			scope.ComposeProjection.Record.DesiredServices,
		),
		Networks: testattachrender.AttachTaskOwnedNetworkSnapshots(
			scope.ComposeProjection.Record.DesiredZones,
		),
		Volumes: append(
			[]testenvironmentprojection.EnvironmentVolumeIdentity(nil),
			scope.ComposeProjection.Record.Volumes...),
		VolumeMounts: append(
			[]testenvironmentprojection.EnvironmentServiceVolumeMount(nil),
			scope.ComposeProjection.Record.VolumeMounts...),
		NetworkJoins: []testattachrender.AttachTaskNetworkJoin{{
			NetworkID: record.BackingNetworkID, ServiceIDs: []string{record.ServiceID},
		}},
		ConsumerServiceIDs: []string{record.ServiceID},
		GrantAttachIDs:     append([]string(nil), record.GrantAttachIDs...),
	}
	for _, prepare := range prepareRuntime {
		renderInput, task = prepare(renderInput, task, marker)
	}
	result, err := repository.CreateAttachWithTask(ctx, scope, record, facts, renderInput, task, marker)
	if err != nil {
		t.Fatalf("CreateAttachWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf(
			"CreateAttachWithTask().Classify() = %v, %v, %v",
			outcome,
			conflict,
			classifyErr,
		)
	}
	created, err := repository.GetAttach(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetAttach(created) error = %v", err)
	}
	queued, err := repository.store.Get(ctx, testtaskjournal.TaskQueueKey(testtaskjournal.TaskExecutorAgent, task.ID))
	if err != nil || queued == nil || queued.Entry == nil || queued.Entry.ModRevision != created.Revision {
		t.Fatalf("Attach Task queue = %#v, error = %v", queued, err)
	}
	storedRenderInput, err := repository.GetAttachTaskRenderInput(ctx, task.PlanID)
	if err != nil || storedRenderInput.Record.PlanID != task.PlanID || storedRenderInput.Revision != created.Revision ||
		storedRenderInput.Record.Authentication != scope.BackingService.Record.Desired.Authentication {
		t.Fatalf("Attach Task render input = %#v, error = %v", storedRenderInput, err)
	}
	return created
}

func publishTestDetach(
	t *testing.T,
	ctx context.Context,
	repository *AttachRepository,
	scope AttachCreateScope,
	current testkeyvalue.Versioned[testattachments.Record],
	createdAt time.Time,
	prepareRuntime ...attachRuntimeTestPreparation,
) TaskRecord {
	t.Helper()
	hierarchy, err := NewHierarchyRepository(repository.store)
	if err != nil {
		t.Fatalf("NewHierarchyRepository() error = %v", err)
	}
	scope.Environment, err = hierarchy.GetEnvironment(ctx, scope.Environment.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	task := validTaskRecord(createdAt)
	owner, err := testtaskjournal.EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task.Owner = owner
	task.Actor = testtaskjournal.TaskActorOperator
	task.ID = ids.NewAt(ids.KindTask, createdAt, 901)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 902)
	task.IdempotencyKey = "attach-detach-key-0001"
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 903)
	task.RenderGeneration = int32(scope.ComposeProjection.Record.RenderGeneration)
	task.Type = testtaskjournal.TaskDetach
	task.Target = current.Record.ID
	task.Params = map[string]string{testtaskjournal.TaskMutationEnvironmentParam: current.Record.EnvironmentID}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = current.Record.EnvironmentID
	marker.Locator.Method = http.MethodDelete
	marker.Locator.Route = "/attaches/{id}"
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetAttach,
		ID:   current.Record.ID,
	}
	renderInput := testattachrender.AttachTaskRenderInput{
		PlanID: task.PlanID, AttachID: current.Record.ID, AttachName: current.Record.Name,
		TenantID: scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: current.Record.EnvironmentID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir:      scope.Environment.Record.VolumeDir,
		BackingServiceID:         scope.BackingService.Record.Desired.ID,
		BackingProjectID:         current.Record.BackingProjectID,
		AdapterKey:               scope.BackingService.Record.Desired.Adapter,
		Authentication:           scope.BackingService.Record.Desired.Authentication,
		DesiredRevisionID:        scope.DesiredHead.Record.RevisionID,
		ArtifactID:               ids.NewAt(ids.KindConfig, createdAt, 904),
		RenderGeneration:         scope.ComposeProjection.Record.RenderGeneration,
		EnvironmentEpochRevision: attachTestEpochRevision(t, ctx, repository.store, current.Record.EnvironmentID),
		RuntimeProjection:        scope.ComposeProjection.Record,
		RuntimePreparation:       configuredAttachRuntimePreparation(task, current.Record.EnvironmentID),
		Services: testattachrender.AttachTaskServiceSnapshots(
			scope.ComposeProjection.Record.DesiredServices,
		),
		Networks: testattachrender.AttachTaskOwnedNetworkSnapshots(
			scope.ComposeProjection.Record.DesiredZones,
		),
		Volumes: append(
			[]testenvironmentprojection.EnvironmentVolumeIdentity(nil),
			scope.ComposeProjection.Record.Volumes...),
		VolumeMounts: append(
			[]testenvironmentprojection.EnvironmentServiceVolumeMount(nil),
			scope.ComposeProjection.Record.VolumeMounts...),
		NetworkJoins:       nil,
		ConsumerServiceIDs: []string{current.Record.ServiceID},
		GrantAttachIDs:     append([]string(nil), current.Record.GrantAttachIDs...),
	}
	for _, prepare := range prepareRuntime {
		renderInput, task = prepare(renderInput, task, marker)
	}
	result, err := repository.BeginAttachDetachWithTask(ctx, scope, current, renderInput, task, marker)
	if err != nil {
		t.Fatalf("BeginAttachDetachWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("BeginAttachDetachWithTask().Classify() = %v, %v, %v", outcome, conflict, classifyErr)
	}
	return task
}
