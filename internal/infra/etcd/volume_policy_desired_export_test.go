package etcd

import (
	"context"
	"crypto/sha256"
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testenvironmentcoordination "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleasegroups "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VolumePolicyDesiredFixture uses real policy preparation and publication;
// only the pre-existing desired baseline and storage are hermetic fixtures.
type VolumePolicyDesiredFixture struct {
	Store                            testkeyvalue.Store
	Task                             TaskRecord
	Marker                           testidempotency.IdempotencyMarker
	Request                          testblueprints.EnvironmentBlueprintStageRequest
	HeadRevision                     int64
	Initial                          *removalrecord.InitialPublication
	OwnerBeforePublication           *removalrecord.Owner
	EnvironmentLockBeforePublication *removalrecord.Owner
	ParentBeforePublication          *testhierarchydeletion.HierarchyCoordinationRecord
	EvidenceBeforePublication        func()
	writerBeforePublication          *taskMaterializationWriterRecord
	prepared                         VolumeRemovalBackupPolicyPreparation
	policy                           *backupPolicyReplacementFixture
	store                            *volumePolicyDesiredAuditStore
}

func (fixture *VolumePolicyDesiredFixture) TaskRepository(t *testing.T) *TaskRepository {
	t.Helper()
	repository, err := newTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	return repository
}

func (fixture *VolumePolicyDesiredFixture) ParentDeletionBegin(
	kind testhierarchydeletion.HierarchyDeletionTargetKind,
) HierarchyDeletionBegin {
	id, operation := fixture.Task.Owner.EnvironmentID, testhierarchydeletion.HierarchyDeletionOperationEnvironment
	switch kind {
	case testhierarchydeletion.HierarchyDeletionTargetProject:
		id, operation = fixture.Task.Owner.ProjectID, testhierarchydeletion.HierarchyDeletionOperationProject
	case testhierarchydeletion.HierarchyDeletionTargetTenant:
		id, operation = fixture.Task.Owner.TenantID, testhierarchydeletion.HierarchyDeletionOperationTenant
	}
	return hierarchyDeletionCreationTestBegin(fixture.Task.CreatedAt, kind, id, operation, "7")
}

// PrepareRemovalRecords supplies the real closed initial-record input with a
// canonical empty-consumer evidence snapshot. Source discovery is not exercised
// by this storage fixture; evidence publication tests run actual staging/sealing.
func (fixture *VolumePolicyDesiredFixture) PrepareRemovalRecords(t *testing.T) removalrecord.Runtime {
	t.Helper()
	task, marker := &fixture.Task, &fixture.Marker
	task.TimeoutSeconds = removalrecord.TimeoutSeconds
	task.IdempotencyKey = marker.Locator.Key
	runtime := removalrecord.Runtime{
		OperationID: task.OperationID, EnvironmentID: task.Owner.EnvironmentID, VolumeID: task.Target,
		Key: fixture.Request.Mutation.Volume.Key, DesiredRevisionID: task.ID,
		DesiredGeneration: uint64(task.RenderGeneration),
		ImpactSHA256:      sha256.Sum256([]byte("fixture impact")),
		IntentSHA256: sha256.Sum256(
			marker.Intent.Ciphertext,
		), RootResponseSHA256: sha256.Sum256(marker.Response.Body),
		RootLocator: removalrecord.ReplayLocator{
			ScopeKind: string(marker.Locator.ScopeKind), ScopeID: marker.Locator.ScopeID,
			Method: marker.Locator.Method, Route: marker.Locator.Route, Key: marker.Locator.Key,
		},
		OriginTaskID: task.ID, CurrentTaskID: task.ID, AttemptOrdinal: 1, StepID: task.Steps[0].ID,
		Checkpoint: removalrecord.DesiredPublished, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	runtime.EvidenceManifestSHA256 = fixture.seedRemovalEvidence(t, runtime)
	task.Params = EnvironmentVolumeRemovalTaskParams(runtime, 1)
	initial, err := removalrecord.PrepareInitialPublication(runtime)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Initial = &initial
	return runtime
}

func (fixture *VolumePolicyDesiredFixture) seedRemovalEvidence(
	t *testing.T,
	runtime removalrecord.Runtime,
) [sha256.Size]byte {
	t.Helper()
	ctx := context.Background()
	hierarchy, err := newHierarchyRepository(fixture.Store)
	if err != nil {
		t.Fatal(err)
	}
	source, found, err := hierarchy.GetEnvironmentComposeProjection(ctx, runtime.EnvironmentID)
	if err != nil || !found {
		t.Fatal("read evidence baseline", err)
	}
	manifest := removalrecord.EvidenceManifest{
		OperationID: runtime.OperationID, EnvironmentID: runtime.EnvironmentID, VolumeID: runtime.VolumeID,
		Key: runtime.Key, ReadRevision: fixture.Revision(), SourceRevisionID: source.Record.RevisionID,
		DesiredRevisionID: runtime.DesiredRevisionID, ImpactSHA256: runtime.ImpactSHA256,
		OrderedSHA256: removalrecord.EmptyEvidenceDigest(),
	}
	manifestValue, err := removalrecord.EncodeEvidenceManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := removalrecord.InitialEvidenceCursor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cursorValue, err := removalrecord.EncodeEvidenceCursor(cursor, manifest)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.Store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   removalrecord.EvidenceManifestKey(runtime.OperationID),
			Value: manifestValue,
		},
		{Type: testkeyvalue.MutationPut, Key: removalrecord.EvidenceCursorKey(runtime.OperationID), Value: cursorValue},
	})
	if err != nil || !result.Succeeded {
		t.Fatal("seed evidence metadata", err)
	}
	sealValue, err := removalrecord.EncodeEvidenceSeal(removalrecord.EvidenceSeal{
		ManifestSHA256: cursor.ManifestSHA256, ManifestRevision: result.Revision,
		CursorRevision: result.Revision, VerifiedRevision: result.Revision,
	}, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Store.Put(ctx, removalrecord.EvidenceSealKey(runtime.OperationID), sealValue); err != nil {
		t.Fatal(err)
	}
	return cursor.ManifestSHA256
}

type volumePolicyDesiredAuditStore struct {
	testkeyvalue.Store

	conditions                       []testkeyvalue.Condition
	mutations                        []testkeyvalue.Mutation
	bytes                            int
	finalPublications                int
	ownerBeforePublication           *removalrecord.Owner
	environmentLockBeforePublication *removalrecord.Owner
	parentBeforePublication          *testhierarchydeletion.HierarchyCoordinationRecord
	writerBeforePublication          *taskMaterializationWriterRecord
	terminalBeforeCommit             func()
	loseTerminalResponse             bool
	evidenceBeforePublication        func()
}

func (audit *volumePolicyDesiredAuditStore) TransactEnvironmentBlueprint(
	ctx context.Context, conditions []testkeyvalue.Condition, mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if err := validateEnvironmentBlueprintTransactionBudget(conditions, mutations); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	if audit.evidenceBeforePublication != nil {
		before := audit.evidenceBeforePublication
		audit.evidenceBeforePublication = nil
		before()
	}
	if audit.parentBeforePublication != nil {
		parent := *audit.parentBeforePublication
		value, err := testhierarchydeletion.EncodeHierarchyCoordination(parent)
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		defer clear(value)
		if _, err := audit.Store.Put(ctx, testhierarchydeletion.HierarchyCoordinationKey(string(parent.TargetKind), parent.TargetID), value); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		audit.parentBeforePublication = nil
	}
	if audit.writerBeforePublication != nil {
		writer := *audit.writerBeforePublication
		value, err := encodeTaskMaterializationWriter(writer)
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		defer clear(value)
		if _, err := audit.Store.Put(ctx, testtaskjournal.TaskMaterializationWriterKey(writer.EnvironmentID), value); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		audit.writerBeforePublication = nil
	}
	if audit.ownerBeforePublication != nil {
		owner := *audit.ownerBeforePublication
		value, err := removalrecord.EncodeOwner(owner)
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		defer clear(value)
		if _, err := audit.Store.Put(ctx, removalrecord.OwnerKey(owner.VolumeID), value); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		audit.ownerBeforePublication = nil
	}
	if audit.environmentLockBeforePublication != nil {
		owner := *audit.environmentLockBeforePublication
		value, err := removalrecord.EncodeOwner(owner)
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		defer clear(value)
		if _, err := audit.Store.Put(ctx, removalrecord.EnvironmentLockKey(owner.EnvironmentID), value); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		audit.environmentLockBeforePublication = nil
	}
	audit.finalPublications++
	return audit.Transact(ctx, conditions, mutations)
}

func (audit *volumePolicyDesiredAuditStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	audit.conditions = append([]testkeyvalue.Condition(nil), conditions...)
	audit.mutations = append([]testkeyvalue.Mutation(nil), mutations...)
	sizer := &store{root: "/groundplane"}
	bytes, err := sizer.transactionSize(conditions, mutations)
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	audit.bytes = bytes
	// The older hierarchy double implements exact-key compares only. Model
	// prefix absence at the same MVCC revision before its synchronous commit.
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		keys[index] = condition.Key
	}
	read, err := audit.Store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	prefixConflict := false
	for index, condition := range conditions {
		if !condition.Prefix {
			continue
		}
		if condition.ModRevision != 0 {
			return testkeyvalue.TransactionResult{}, errs.New(
				errs.KindInternal,
				"policy publication fixture only supports prefix absence",
			)
		}
		matching, err := audit.Store.Range(
			ctx, testkeyvalue.RangeRequest{Prefix: condition.Key, Limit: 1, Revision: read.ReadRevision},
		)
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		if len(matching.Values) != 0 {
			read.Values[index] = &matching.Values[0]
			prefixConflict = true
		}
	}
	if prefixConflict {
		return testkeyvalue.TransactionResult{Revision: read.ReadRevision, FailureReads: read.Values}, nil
	}
	terminal := false
	for _, mutation := range mutations {
		if mutation.Type == testkeyvalue.MutationDelete && mutation.Prefix {
			terminal = true
		}
	}
	if terminal && audit.terminalBeforeCommit != nil {
		beforeCommit := audit.terminalBeforeCommit
		audit.terminalBeforeCommit = nil
		beforeCommit()
	}
	result, err := audit.Store.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && terminal && audit.loseTerminalResponse {
		audit.loseTerminalResponse = false
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindRequestFailed, "fixture lost terminal response")
	}
	return result, err
}

func (fixture *VolumePolicyDesiredFixture) BeforeRemovalTerminalCommit(action func()) {
	fixture.store.terminalBeforeCommit = action
}

func (fixture *VolumePolicyDesiredFixture) LoseRemovalTerminalResponse() {
	fixture.store.loseTerminalResponse = true
}

func (fixture *VolumePolicyDesiredFixture) AssertRemovalAttemptBudget(t *testing.T, status testtaskjournal.TaskStatus) {
	t.Helper()
	compares, writes, size := len(fixture.store.conditions), len(fixture.store.mutations), fixture.store.bytes
	if compares > 24 || writes > 24 || compares+writes > 48 || size > 900*1024 {
		t.Fatalf("attempt transaction exceeds ADR0049 budget: %d/%d/%d", compares, writes, size)
	}
	wantBytes := 7969
	if status == testtaskjournal.TaskStatusTimedOut {
		wantBytes = 7972
	}
	if compares != 23 || writes != 9 || size != wantBytes {
		t.Fatalf("attempt transaction shape changed: %d/%d/%d", compares, writes, size)
	}
	t.Logf("attempt transaction: %d comparisons, %d mutations, %d protobuf bytes", compares, writes, size)
}

func (fixture *VolumePolicyDesiredFixture) AssertRemovalRetryBudget(t *testing.T) {
	t.Helper()
	compares, writes, size := len(fixture.store.conditions), len(fixture.store.mutations), fixture.store.bytes
	if compares > 24 || writes > 24 || compares+writes > 48 || size > 900*1024 {
		t.Fatalf("retry transaction exceeds ADR0049 budget: %d/%d/%d", compares, writes, size)
	}
	if compares != 23 || writes != 11 || size != 9421 {
		t.Fatalf("retry transaction shape changed: %d/%d/%d", compares, writes, size)
	}
	t.Logf("retry transaction: %d comparisons, %d mutations, %d protobuf bytes", compares, writes, size)
}

func (fixture *VolumePolicyDesiredFixture) AssertRemovalRecoveryBudget(t *testing.T, stage string) {
	t.Helper()
	compares, writes, size := len(fixture.store.conditions), len(fixture.store.mutations), fixture.store.bytes
	var maximum, wantCompares, wantWrites, wantBytes int
	switch stage {
	case "attempt":
		maximum, wantCompares, wantWrites, wantBytes = 24, 24, 11, 9658
	case "retry":
		maximum, wantCompares, wantWrites, wantBytes = 24, 24, 11, 9541
	case "completion":
		maximum, wantCompares, wantWrites, wantBytes = 16, 11, 4, 3316
	case "progress":
		maximum, wantCompares, wantWrites, wantBytes = 16, 11, 3, 2697
	case "next request":
		maximum, wantCompares, wantWrites, wantBytes = 16, 9, 1, 2176
	case "next completion":
		maximum, wantCompares, wantWrites, wantBytes = 16, 10, 4, 3196
	default:
		t.Fatalf("unknown pending recovery stage %s", stage)
		return
	}
	if compares > maximum || writes > maximum || compares+writes > 2*maximum || size > 900*1024 {
		t.Fatalf("%s recovery transaction exceeds ADR0049 budget: %d/%d/%d", stage, compares, writes, size)
	}
	if compares != wantCompares || writes != wantWrites || size != wantBytes {
		t.Fatalf("%s recovery transaction shape changed: %d/%d/%d", stage, compares, writes, size)
	}
	t.Logf("%s recovery transaction: %d comparisons, %d mutations, %d protobuf bytes", stage, compares, writes, size)
}

func (fixture *VolumePolicyDesiredFixture) AssertRemovalTerminal(t *testing.T, revision int64, at time.Time) {
	fixture.assertRemovalTerminal(t, revision, at, 24, 16, 11035)
}

func (fixture *VolumePolicyDesiredFixture) AssertRemovalSuccessorTerminal(t *testing.T, revision int64, at time.Time) {
	fixture.assertRemovalTerminal(t, revision, at, 26, 18, 13250)
}

func (fixture *VolumePolicyDesiredFixture) PutRemovalDerivedIndexes(t *testing.T, taskID string, at time.Time) {
	t.Helper()
	value, err := testidempotency.EncodeTaskReference(taskID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.store.Store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testtaskjournal.TaskQueueKey(testtaskjournal.TaskExecutorAgent, taskID),
			Value: value,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testtaskjournal.TaskRetentionIndexKey(taskID, at.Add(testidempotency.MarkerRetention)),
			Value: value,
		},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("seed late Task indexes: %v", err)
	}
}

func (fixture *VolumePolicyDesiredFixture) assertRemovalTerminal(t *testing.T, revision int64, at time.Time,
	compares, writes, size int) {
	t.Helper()
	markerKey, err := testidempotency.IdempotencyMarkerKey(fixture.Marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	retentionKey, err := testidempotency.IdempotencyRetentionKey(markerKey, at.Add(testidempotency.MarkerRetention))
	if err != nil {
		t.Fatal(err)
	}
	read, err := fixture.Store.Get(context.Background(), retentionKey)
	if err != nil || read.Entry == nil || read.Entry.ModRevision != revision {
		t.Fatalf("full-TTL root retention was not atomic: %v", err)
	}
	for _, mutation := range fixture.store.mutations {
		if mutation.Key == testblueprints.EnvironmentBlueprintHeadKey(fixture.Task.Owner.EnvironmentID) ||
			mutation.Key == testbackuppolicy.BackupPolicyKey(fixture.Task.Owner.EnvironmentID) {
			t.Fatal("runtime finalization republished desired state or Backup policy")
		}
	}
	if len(fixture.store.conditions) != compares || len(fixture.store.mutations) != writes ||
		fixture.store.bytes != size {
		t.Fatalf("terminal shape changed: %d/%d/%d", len(fixture.store.conditions),
			len(fixture.store.mutations), fixture.store.bytes)
	}
	t.Logf("terminal transaction: %d comparisons, %d mutations, %d protobuf bytes",
		len(fixture.store.conditions), len(fixture.store.mutations), fixture.store.bytes)
}

func NewVolumePolicyDesiredFixture(t *testing.T) *VolumePolicyDesiredFixture {
	t.Helper()
	ctx := context.Background()
	policy := newBackupPolicyReplacementFixture(t, true)
	volume := policy.sources[0].Source.Record
	seed, err := policy.repository.PrepareBackupPolicyReplacement(ctx, testbackuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: policy.environment.Record.ID, Enabled: true, Frequency: "*-*-* 02:00:00",
		Keep: 7, Encryption: "none", ConnectorID: policy.connector.Record.Connector.ID,
		Sources: []testbackuppolicy.BackupPolicySourceSelection{
			{Kind: core.BackupSourceVolume, TargetID: volume.TargetID},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Destroy()
	seed, err = seed.FinalizeSchedule(policy.now)
	if err != nil {
		t.Fatal(err)
	}
	result, err := policy.repository.ReplaceBackupPolicyProtected(ctx, seed,
		backupPolicyReplacementMarker(policy.environment.Record.ID, "volume-policy-desired-seed-0001"))
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("seed policy: %v", err)
	}
	seedServiceRepositoryTestDesiredProjection(
		t,
		policy.store,
		withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: policy.environment.Record.ID, RevisionID: ids.New(ids.KindTask), RenderGeneration: 1,
			Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{
				{ID: volume.TargetID, Slug: "backup-data", Key: "backup-data"},
			},
			Backup: &testenvironmentprojection.EnvironmentBlueprintBackupPolicy{
				Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 7, Encryption: "none",
				ConnectorID: policy.connector.Record.Connector.ID,
				Sources: []testenvironmentprojection.EnvironmentBlueprintBackupPolicySource{
					{ID: volume.ID, Kind: volume.Kind, TargetID: volume.TargetID},
				},
			},
		}),
	)
	prepared, err := policy.repository.PrepareVolumeRemovalBackupPolicy(
		ctx,
		policy.environment.Record.ID,
		volume.TargetID,
		policy.store.revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	store := &volumePolicyDesiredAuditStore{Store: &releasePlanningTestStore{memoryHierarchyStore: policy.store}}
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	head, found, err := hierarchy.GetEnvironmentBlueprintHead(ctx, policy.environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("read baseline: %v", err)
	}
	current, found, err := hierarchy.GetEnvironmentComposeProjection(ctx, policy.environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("read projection: %v", err)
	}
	task := environmentBlueprintTestTask(t, policy.project.Record, policy.environment.Record, 32000)
	task.Type, task.Target, task.RenderGeneration = testtaskjournal.TaskRemove, volume.TargetID, 2
	task.Params[testtaskjournal.TaskResourceKindParam] = testtaskjournal.TaskResourceVolume
	marker := environmentBlueprintTestMarker(task, policy.environment.Record.ID)
	marker.Locator.Method, marker.Locator.Route = "DELETE", "/volumes/{id}"
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetVolume,
		ID:   volume.TargetID,
	}
	projection := testenvironmentprojection.CloneEnvironmentComposeProjection(current.Record)
	projection.RevisionID, projection.RenderGeneration = task.ID, 2
	projection.Volumes, projection.VolumeMounts = nil, nil
	projection.Backup = prepared.Projection()
	projection = withTestEnvironmentComposeArtifact(projection)
	digest, err := testblueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	precondition, err := testblueprints.EnvironmentBlueprintDependencyDigest(current.Record)
	if err != nil {
		t.Fatal(err)
	}
	return &VolumePolicyDesiredFixture{
		Store: store, Task: task, Marker: marker, HeadRevision: head.Revision, prepared: prepared, policy: policy, store: store,
		Request: testblueprints.EnvironmentBlueprintStageRequest{
			Projection: projection, DependencyDigest: digest,
			Mutation: &testblueprints.EnvironmentDesiredMutationAudit{
				Volume: &testblueprints.EnvironmentVolumeMutationAudit{
					Action: testblueprints.EnvironmentVolumeMutationRemove, VolumeID: volume.TargetID,
					Slug: "backup-data", Key: "backup-data", KeySupplied: true, PreconditionDigest: precondition,
				},
			},
		},
	}
}

func (fixture *VolumePolicyDesiredFixture) Publish(ctx context.Context) (IdempotencyTransactionResult, error) {
	fixture.store.ownerBeforePublication = fixture.OwnerBeforePublication
	fixture.store.environmentLockBeforePublication = fixture.EnvironmentLockBeforePublication
	fixture.store.writerBeforePublication = fixture.writerBeforePublication
	fixture.store.parentBeforePublication = fixture.ParentBeforePublication
	fixture.store.evidenceBeforePublication = fixture.EvidenceBeforePublication
	if fixture.Initial != nil {
		publisher, err := newEnvironmentBlueprintRepository(fixture.Store, fixture.store)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		return publisher.PublishEnvironmentVolumeRemovalWithTask(
			ctx,
			fixture.policy.project,
			fixture.policy.environment,
			fixture.HeadRevision,
			fixture.Request.Claim,
			fixture.Request.Projection,
			fixture.prepared,
			*fixture.Initial,
			fixture.Task,
			fixture.Marker,
		)
	}
	hierarchy, err := newHierarchyRepository(fixture.Store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return hierarchy.publishEnvironmentDesiredRevisionWithTask(
		ctx,
		netip.Prefix{},
		fixture.policy.environment.Record.NetworkPool,
		fixture.policy.project,
		fixture.policy.environment,
		fixture.HeadRevision,
		fixture.Request.Claim,
		testblueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: fixture.Task.Owner.EnvironmentID,
			RevisionID:    fixture.Task.ID,
		},
		fixture.Request.Projection,
		nil,
		nil,
		nil,
		testreleasegroups.ReleaseGroupBlueprintPreparedMutation{},
		testcomponentplanning.ComponentTaskPreparation{},
		testblueprintplanning.BlueprintAttachTaskPreparation{},
		testblueprintplanning.BlueprintBackupPolicyPreparation{},
		BlueprintScriptPublication{},
		BlueprintReleasePublication{},
		BlueprintRequirementGate{},
		fixture.prepared,
		fixture.Initial,
		fixture.Task,
		fixture.Marker,
		fixture.store,
	)
}

func (fixture *VolumePolicyDesiredFixture) HoldMaterializationWriter(t *testing.T, late bool) {
	t.Helper()
	writer := taskMaterializationWriterRecord{
		EnvironmentID: fixture.Task.Owner.EnvironmentID, TaskID: ids.New(ids.KindTask), RenderGeneration: 1,
	}
	if late {
		fixture.writerBeforePublication = &writer
		return
	}
	value, err := encodeTaskMaterializationWriter(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	if _, err := fixture.Store.Put(context.Background(), testtaskjournal.TaskMaterializationWriterKey(writer.EnvironmentID), value); err != nil {
		t.Fatal(err)
	}
}

// UseMaximumSelection seeds the legal 12-source policy, then prepares the real
// removal against that same desired baseline. It does not change any limit.
func (fixture *VolumePolicyDesiredFixture) UseMaximumSelection(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: fixture.Task.Owner.EnvironmentID, RevisionID: ids.New(ids.KindTask), RenderGeneration: 1,
		Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{
			{ID: fixture.Task.Target, Slug: "backup-data", Key: "backup-data"},
		},
	})
	first := fixture.policy.sources[0].Source.Record
	input := testblueprintplanning.EnvironmentBlueprintBackupPolicyInput{
		EnvironmentID: projection.EnvironmentID, TaskID: projection.RevisionID,
		ReadRevision: fixture.Revision(), Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 7, Encryption: "none",
		ConnectorName: fixture.policy.connector.Record.Connector.Name, CreatedAt: fixture.policy.now.Add(time.Minute),
		Sources: []testblueprintplanning.EnvironmentBlueprintBackupPolicySourceInput{
			{CandidateID: first.ID, Kind: first.Kind, TargetID: first.TargetID},
		},
	}
	for index := 1; index < testbackuppolicy.MaximumBackupPolicySources; index++ {
		volumeID := ids.New(ids.KindVolume)
		label := "volume-" + string(rune('a'+index))
		projection.Volumes = append(
			projection.Volumes,
			testenvironmentprojection.EnvironmentVolumeIdentity{ID: volumeID, Slug: label, Key: label},
		)
		input.Sources = append(input.Sources, testblueprintplanning.EnvironmentBlueprintBackupPolicySourceInput{
			CandidateID: ids.New(ids.KindBackupSource), Kind: core.BackupSourceVolume, TargetID: volumeID,
		})
	}
	input.Projection = projection
	seed, err := fixture.policy.repository.PrepareEnvironmentBlueprintBackupPolicy(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	defer seed.Clear()
	projection.Backup = seed.Projection()
	publication, err := testblueprintplanning.PrepareBlueprintBackupPolicyPublication(
		testblueprintplanning.TaskIdentity{ID: projection.RevisionID, Target: projection.EnvironmentID},
		projection,
		testblueprintplanning.BlueprintAttachTaskPreparation{},
		seed,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer testblueprintplanning.ClearPreparedBlueprintBackupPolicyPublication(publication)
	result, err := fixture.policy.store.Transact(ctx, publication.Conditions(), publication.Mutations())
	if err != nil || !result.Succeeded {
		t.Fatalf("seed maximum policy: %v", err)
	}
	projection = withTestEnvironmentComposeArtifact(projection)
	seedServiceRepositoryTestDesiredProjection(t, fixture.policy.store, projection)
	fixture.HeadRevision = fixture.Revision()
	fixture.prepared, err = fixture.policy.repository.PrepareVolumeRemovalBackupPolicy(
		ctx,
		projection.EnvironmentID,
		fixture.Task.Target,
		fixture.Revision(),
	)
	if err != nil {
		t.Fatal(err)
	}
	precondition, err := testblueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	projection.RevisionID, projection.RenderGeneration = fixture.Task.ID, 2
	projection.Volumes = projection.Volumes[1:]
	projection.Backup = fixture.prepared.Projection()
	projection = withTestEnvironmentComposeArtifact(projection)
	digest, err := testblueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Request.Projection, fixture.Request.DependencyDigest = projection, digest
	fixture.Request.Mutation.Volume.PreconditionDigest = precondition
}

func (fixture *VolumePolicyDesiredFixture) AssertMaximumSelectionPublished(t *testing.T) {
	t.Helper()
	if fixture.store.finalPublications != 1 {
		t.Fatal("Volume removal did not use the dedicated final publisher")
	}
	task := fixture.policy.store.valueAt(testtaskjournal.TaskStorageKey(fixture.Task.ID), fixture.Revision())
	policy, found, err := fixture.policy.repository.GetBackupPolicy(
		context.Background(),
		fixture.Task.Owner.EnvironmentID,
	)
	if err != nil || !found || task == nil || task.ModRevision != policy.Revision || !policy.Record.Enabled ||
		len(policy.Record.SourceIDs) != testbackuppolicy.MaximumBackupPolicySources-1 {
		t.Fatalf("maximum policy was not replaced with its Task: %v", err)
	}
	for _, source := range fixture.prepared.state.sources {
		value := fixture.policy.store.valueAt(testbackuppolicy.BackupSourceKey(source.ID), fixture.Revision())
		if value == nil {
			t.Fatal("historical source was removed")
		}
		found := false
		for _, condition := range fixture.store.conditions {
			if condition.Key == value.Key && condition.ModRevision == value.ModRevision && !condition.Prefix {
				found = true
			}
		}
		if !found {
			t.Fatal("publication lost an exact source primary fence")
		}
	}
	// Publication-time JSON timestamps trim trailing fractional zeros; the
	// nine-digit representation is the upper bound, not every request's size.
	wantConditions, wantMutations, maximumBytes := 38, 15, 13286
	if fixture.Initial != nil {
		wantConditions += 9
		wantMutations += 7
		maximumBytes = 17392
	} else {
		wantConditions++ // Ordinary desired mutation must exclude the Environment removal lock too.
	}
	if len(fixture.store.conditions) != wantConditions || len(fixture.store.mutations) != wantMutations ||
		fixture.store.bytes > maximumBytes || fixture.store.bytes > 1024*1024 {
		t.Fatalf(
			"maximum policy/desired publication shape: %d/%d/%d bytes",
			len(fixture.store.conditions),
			len(fixture.store.mutations),
			fixture.store.bytes,
		)
	}
	t.Logf(
		"maximum policy + desired publication: %d comparisons, %d mutations, %d protobuf bytes",
		len(fixture.store.conditions),
		len(fixture.store.mutations),
		fixture.store.bytes,
	)
}

func (fixture *VolumePolicyDesiredFixture) AssertAtomicPolicy(t *testing.T, earliest time.Time) {
	t.Helper()
	ctx := context.Background()
	task := fixture.policy.store.valueAt(testtaskjournal.TaskStorageKey(fixture.Task.ID), fixture.policy.store.revision)
	if task == nil {
		t.Fatal("publisher did not write Task")
	}
	for _, key := range []string{testbackuppolicy.BackupPolicyKey(fixture.Task.Owner.EnvironmentID), testenvironmentcoordination.Key(fixture.Task.Owner.EnvironmentID), testblueprints.EnvironmentBlueprintHeadKey(fixture.Task.Owner.EnvironmentID)} {
		value := fixture.policy.store.valueAt(key, fixture.policy.store.revision)
		if value == nil || value.ModRevision != task.ModRevision {
			t.Fatalf("%s was not published in the Task transaction", key)
		}
	}
	policy, found, err := fixture.policy.repository.GetBackupPolicy(ctx, fixture.Task.Owner.EnvironmentID)
	if err != nil || !found || policy.Record.Enabled || len(policy.Record.SourceIDs) != 0 || policy.Record.Keep != 7 ||
		policy.Record.UpdatedAt.Before(earliest) {
		t.Fatalf("last-source replacement or publication clock incorrect: %v", err)
	}
	coordinationValue := fixture.policy.store.valueAt(
		testenvironmentcoordination.Key(fixture.Task.Owner.EnvironmentID),
		fixture.Revision(),
	)
	coordination, err := testenvironmentcoordination.Decode(coordinationValue.Value)
	if err != nil || coordination.CurrentBackupScheduleState != nil ||
		!coordination.ScheduleClockFloor.Equal(policy.Record.UpdatedAt) {
		t.Fatalf("scheduling floor does not use the exact publication clock: %v", err)
	}
	if value := fixture.policy.store.valueAt(testbackuppolicy.BackupSourceKey(fixture.policy.sources[0].Source.Record.ID), fixture.policy.store.revision); value == nil {
		t.Fatal("historical source was removed")
	}
	if value := fixture.policy.store.valueAt(testbackuppolicy.BackupPolicyConnectorReferenceKey(fixture.policy.connector.Record.Connector.ID, fixture.Task.Owner.EnvironmentID), fixture.policy.store.revision); value != nil {
		t.Fatal("disabled policy retained its active Connector reference")
	}
	wantConditions, wantMutations, maximumBytes := 28, 16, 11076
	if fixture.Initial != nil {
		wantConditions, wantMutations = 36, 23
		maximumBytes = 15182
	}
	if len(fixture.store.conditions) != wantConditions || len(fixture.store.mutations) != wantMutations ||
		fixture.store.bytes > maximumBytes || fixture.store.bytes > 900*1024 {
		t.Fatalf("combined policy/desired publication shape: %d/%d/%d bytes",
			len(fixture.store.conditions), len(fixture.store.mutations), fixture.store.bytes)
	}
	t.Logf(
		"desired + last-source policy publication: %d comparisons, %d mutations, %d protobuf bytes",
		len(fixture.store.conditions),
		len(fixture.store.mutations),
		fixture.store.bytes,
	)
}

func (fixture *VolumePolicyDesiredFixture) Revision() int64 { return fixture.policy.store.revision }

// Rationale: fixed clock samples cannot be exact byte expectations for live
// publication. Both policy and coordination timestamps trim fractional zeros.
func TestVolumePublicationTimestampEncodingChangesWireSize(t *testing.T) {
	fixture := NewVolumePolicyDesiredFixture(t)
	sizes := make([]int, 2)
	for index, nanos := range []int{1, 10} {
		publication, err := prepareVolumeRemovalBackupPolicyPublication(fixture.prepared,
			time.Date(2030, 1, 1, 0, 0, 0, nanos, time.UTC))
		if err != nil {
			t.Fatal(err)
		}
		sizes[index], err = (&store{root: "/groundplane"}).transactionSize(
			publication.conditions,
			publication.mutations,
		)
		testkeyvalue.ClearMutationValues(publication.mutations)
		if err != nil {
			t.Fatal(err)
		}
	}
	if sizes[0]-sizes[1] != 2 {
		t.Fatalf("timestamp wire-size delta: %v", sizes)
	}
}

func (fixture *VolumePolicyDesiredFixture) AssertRemovalAncestry(t *testing.T, baselineRevision int64) {
	t.Helper()
	for _, identity := range []testhierarchydeletion.HierarchyCoordinationRecord{
		{TargetKind: testhierarchydeletion.HierarchyDeletionTargetProject, TargetID: fixture.Task.Owner.ProjectID},
		{TargetKind: testhierarchydeletion.HierarchyDeletionTargetTenant, TargetID: fixture.Task.Owner.TenantID},
	} {
		key := testhierarchydeletion.HierarchyCoordinationKey(string(identity.TargetKind), identity.TargetID)
		previous, err := fixture.Store.GetMany(
			context.Background(), testkeyvalue.GetManyRequest{Keys: []string{key}, Revision: baselineRevision},
		)
		if err != nil || previous == nil || len(previous.Values) != 1 || previous.Values[0] == nil {
			t.Fatalf("parent baseline: %v", err)
		}
		before, err := testhierarchydeletion.DecodeHierarchyCoordination(previous.Values[0].Value)
		if err != nil {
			t.Fatal(err)
		}
		current, err := fixture.Store.Get(context.Background(), key)
		if err != nil || current == nil || current.Entry == nil {
			t.Fatalf("parent publication: %v", err)
		}
		after, err := testhierarchydeletion.DecodeHierarchyCoordination(current.Entry.Value)
		before.MutationEpoch++
		if err != nil || after != before || current.Entry.ModRevision != fixture.Revision() {
			t.Fatalf("removal did not atomically advance %s deletion epoch: %v", identity.TargetKind, err)
		}
	}
}

func (fixture *VolumePolicyDesiredFixture) CorruptRemovalParent(
	t *testing.T, kind testhierarchydeletion.HierarchyDeletionTargetKind, change string,
) {
	t.Helper()
	id := fixture.Task.Owner.ProjectID
	idKind := ids.KindProject
	if kind == testhierarchydeletion.HierarchyDeletionTargetTenant {
		id, idKind = fixture.Task.Owner.TenantID, ids.KindTenant
	}
	key := testhierarchydeletion.HierarchyCoordinationKey(string(kind), id)
	if change == "missing" {
		if _, err := fixture.Store.Transact(context.Background(), nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: key}}); err != nil {
			t.Fatal(err)
		}
		return
	}
	value := []byte("corrupt")
	if change == "identity" {
		var err error
		value, err = testhierarchydeletion.EncodeHierarchyCoordination(
			testhierarchydeletion.HierarchyCoordinationRecord{
				Schema: 1, TargetKind: kind, TargetID: ids.New(idKind), MutationEpoch: 1,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := fixture.Store.Put(context.Background(), key, value); err != nil {
		t.Fatal(err)
	}
}

func (fixture *VolumePolicyDesiredFixture) Race(t *testing.T, authority string) {
	t.Helper()
	keys := map[string]string{
		"policy":       testbackuppolicy.BackupPolicyKey(fixture.Task.Owner.EnvironmentID),
		"coordination": testenvironmentcoordination.Key(fixture.Task.Owner.EnvironmentID),
		"epoch":        testhierarchy.EnvironmentMutationEpochKey(fixture.Task.Owner.EnvironmentID),
		"source":       testbackuppolicy.BackupSourceKey(fixture.policy.sources[0].Source.Record.ID),
		"connector reference": testbackuppolicy.BackupPolicyConnectorReferenceKey(
			fixture.policy.connector.Record.Connector.ID,
			fixture.Task.Owner.EnvironmentID,
		),
	}
	key := keys[authority]
	value := fixture.policy.store.valueAt(key, fixture.policy.store.revision)
	if value == nil {
		t.Fatal("race authority is absent")
	}
	if _, err := fixture.Store.Put(context.Background(), key, value.Value); err != nil {
		t.Fatal(err)
	}
}

func (fixture *VolumePolicyDesiredFixture) AssertUnpublished(t *testing.T, revision int64) {
	t.Helper()
	if fixture.Revision() != revision {
		t.Fatal("rejected publication wrote storage")
	}
	if value := fixture.policy.store.valueAt(testtaskjournal.TaskStorageKey(fixture.Task.ID), revision); value != nil {
		t.Fatal("rejected publication wrote Task")
	}
	head := fixture.policy.store.valueAt(
		testblueprints.EnvironmentBlueprintHeadKey(fixture.Task.Owner.EnvironmentID),
		revision,
	)
	if head == nil || head.ModRevision != fixture.HeadRevision {
		t.Fatal("rejected publication changed desired head")
	}
	markerKey, err := testidempotency.IdempotencyMarkerKey(fixture.Marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	if value := fixture.policy.store.valueAt(markerKey, revision); value != nil {
		t.Fatal("rejected publication wrote replay marker")
	}
	policy, found, err := fixture.policy.repository.GetBackupPolicy(
		context.Background(),
		fixture.Task.Owner.EnvironmentID,
	)
	if err != nil || !found || !policy.Record.Enabled || len(policy.Record.SourceIDs) != 1 {
		t.Fatalf("rejection replaced policy: %v", err)
	}
}

// Rationale: policy preparation is usable only for its named Volume removal,
// never a different capability, scope, route, or Blueprint Apply.
func TestVolumePolicyDesiredPreparationBinding(t *testing.T) {
	fixture := NewVolumePolicyDesiredFixture(t)
	for _, changed := range []string{"control", "source kind", "environment", "removed Volume", "Task type", "Task target", "resource kind", "method", "route", "unprepared"} {
		t.Run(changed, func(t *testing.T) {
			prepared := fixture.prepared
			claim := testblueprints.EnvironmentBlueprintStageClaim{
				SourceKind: testblueprints.EnvironmentBlueprintSourceMutation, EnvironmentID: fixture.Task.Owner.EnvironmentID,
			}
			task, marker := cloneTaskRecord(fixture.Task), fixture.Marker
			removed := fixture.Task.Target
			switch changed {
			case "source kind":
				claim.SourceKind = testblueprints.EnvironmentBlueprintSourceApply
			case "environment":
				claim.EnvironmentID = ids.New(ids.KindEnvironment)
			case "removed Volume":
				removed = ids.New(ids.KindVolume)
			case "Task type":
				task.Type = testtaskjournal.TaskUpdate
			case "Task target":
				task.Target = ids.New(ids.KindVolume)
			case "resource kind":
				task.Params[testtaskjournal.TaskResourceKindParam] = testtaskjournal.TaskResourceEntry
			case "method":
				marker.Locator.Method = "PATCH"
			case "route":
				marker.Locator.Route = "/entries/{id}"
			case "unprepared":
				prepared = VolumeRemovalBackupPolicyPreparation{}
			}
			err := prepared.validateDesiredPublication(claim, fixture.Request.Projection, task, marker, removed)
			if changed == "control" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !isKind(err, errs.KindValidationFailed) {
				t.Fatalf("changed binding accepted: %v", err)
			}
		})
	}
}
