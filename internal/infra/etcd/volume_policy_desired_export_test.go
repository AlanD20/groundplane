package etcd

import (
	"context"
	"crypto/sha256"
	"net/netip"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VolumePolicyDesiredFixture uses real policy preparation and publication;
// only the pre-existing desired baseline and storage are hermetic fixtures.
type VolumePolicyDesiredFixture struct {
	Store                            Store
	Task                             TaskRecord
	Marker                           IdempotencyMarker
	Request                          EnvironmentBlueprintStageRequest
	HeadRevision                     int64
	Initial                          *removalrecord.InitialPublication
	OwnerBeforePublication           *removalrecord.Owner
	EnvironmentLockBeforePublication *removalrecord.Owner
	ParentBeforePublication          *HierarchyCoordinationRecord
	writerBeforePublication          *taskMaterializationWriterRecord
	prepared                         VolumeRemovalBackupPolicyPreparation
	policy                           *backupPolicyReplacementFixture
	store                            *volumePolicyDesiredAuditStore
}

func (fixture *VolumePolicyDesiredFixture) ParentDeletionBegin(
	kind HierarchyDeletionTargetKind,
) HierarchyDeletionBegin {
	id, operation := fixture.Task.Owner.EnvironmentID, HierarchyDeletionOperationEnvironment
	switch kind {
	case HierarchyDeletionTargetProject:
		id, operation = fixture.Task.Owner.ProjectID, HierarchyDeletionOperationProject
	case HierarchyDeletionTargetTenant:
		id, operation = fixture.Task.Owner.TenantID, HierarchyDeletionOperationTenant
	}
	return hierarchyDeletionCreationTestBegin(fixture.Task.CreatedAt, kind, id, operation, "7")
}

// PrepareRemovalRecords supplies the real closed initial-record input. This
// fixture does not claim to prepare the still-unwired bounded removal evidence.
func (fixture *VolumePolicyDesiredFixture) PrepareRemovalRecords(t *testing.T) removalrecord.Runtime {
	t.Helper()
	task, marker := &fixture.Task, &fixture.Marker
	task.TimeoutSeconds = removalrecord.TimeoutSeconds
	task.IdempotencyKey = marker.Locator.Key
	runtime := removalrecord.Runtime{
		OperationID: task.OperationID, EnvironmentID: task.Owner.EnvironmentID, VolumeID: task.Target,
		Key: fixture.Request.Mutation.Volume.Key, DesiredRevisionID: task.ID,
		DesiredGeneration:      uint64(task.RenderGeneration),
		ImpactSHA256:           sha256.Sum256([]byte("fixture impact")),
		EvidenceManifestSHA256: sha256.Sum256([]byte("fixture evidence")),
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
	task.Params = EnvironmentVolumeRemovalTaskParams(runtime, 1)
	initial, err := removalrecord.PrepareInitialPublication(runtime)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Initial = &initial
	return runtime
}

type volumePolicyDesiredAuditStore struct {
	Store
	conditions                       []Condition
	mutations                        []Mutation
	bytes                            int
	finalPublications                int
	ownerBeforePublication           *removalrecord.Owner
	environmentLockBeforePublication *removalrecord.Owner
	parentBeforePublication          *HierarchyCoordinationRecord
	writerBeforePublication          *taskMaterializationWriterRecord
}

func (audit *volumePolicyDesiredAuditStore) TransactEnvironmentBlueprint(
	ctx context.Context, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	if err := validateEnvironmentBlueprintTransactionBudget(conditions, mutations); err != nil {
		return TransactionResult{}, err
	}
	if audit.parentBeforePublication != nil {
		parent := *audit.parentBeforePublication
		value, err := encodeHierarchyCoordination(parent)
		if err != nil {
			return TransactionResult{}, err
		}
		defer clear(value)
		if _, err := audit.Store.Put(ctx, HierarchyCoordinationKey(string(parent.TargetKind), parent.TargetID), value); err != nil {
			return TransactionResult{}, err
		}
		audit.parentBeforePublication = nil
	}
	if audit.writerBeforePublication != nil {
		writer := *audit.writerBeforePublication
		value, err := encodeTaskMaterializationWriter(writer)
		if err != nil {
			return TransactionResult{}, err
		}
		defer clear(value)
		if _, err := audit.Store.Put(ctx, taskMaterializationWriterKey(writer.EnvironmentID), value); err != nil {
			return TransactionResult{}, err
		}
		audit.writerBeforePublication = nil
	}
	if audit.ownerBeforePublication != nil {
		owner := *audit.ownerBeforePublication
		value, err := removalrecord.EncodeOwner(owner)
		if err != nil {
			return TransactionResult{}, err
		}
		defer clear(value)
		if _, err := audit.Store.Put(ctx, removalrecord.OwnerKey(owner.VolumeID), value); err != nil {
			return TransactionResult{}, err
		}
		audit.ownerBeforePublication = nil
	}
	if audit.environmentLockBeforePublication != nil {
		owner := *audit.environmentLockBeforePublication
		value, err := removalrecord.EncodeOwner(owner)
		if err != nil {
			return TransactionResult{}, err
		}
		defer clear(value)
		if _, err := audit.Store.Put(ctx, removalrecord.EnvironmentLockKey(owner.EnvironmentID), value); err != nil {
			return TransactionResult{}, err
		}
		audit.environmentLockBeforePublication = nil
	}
	audit.finalPublications++
	return audit.Transact(ctx, conditions, mutations)
}

func (audit *volumePolicyDesiredAuditStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	audit.conditions = append([]Condition(nil), conditions...)
	audit.mutations = append([]Mutation(nil), mutations...)
	sizer := &store{root: "/groundplane"}
	bytes, err := sizer.transactionSize(conditions, mutations)
	if err != nil {
		return TransactionResult{}, err
	}
	audit.bytes = bytes
	// The older hierarchy double implements exact-key compares only. Model
	// prefix absence at the same MVCC revision before its synchronous commit.
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		keys[index] = condition.Key
	}
	read, err := audit.Store.GetMany(ctx, GetManyRequest{Keys: keys})
	if err != nil {
		return TransactionResult{}, err
	}
	prefixConflict := false
	for index, condition := range conditions {
		if !condition.Prefix {
			continue
		}
		if condition.ModRevision != 0 {
			return TransactionResult{}, errs.New(
				errs.KindInternal,
				"policy publication fixture only supports prefix absence",
			)
		}
		matching, err := audit.Store.Range(
			ctx,
			RangeRequest{Prefix: condition.Key, Limit: 1, Revision: read.ReadRevision},
		)
		if err != nil {
			return TransactionResult{}, err
		}
		if len(matching.Values) != 0 {
			read.Values[index] = &matching.Values[0]
			prefixConflict = true
		}
	}
	if prefixConflict {
		return TransactionResult{Revision: read.ReadRevision, FailureReads: read.Values}, nil
	}
	return audit.Store.Transact(ctx, conditions, mutations)
}

func NewVolumePolicyDesiredFixture(t *testing.T) *VolumePolicyDesiredFixture {
	t.Helper()
	ctx := context.Background()
	policy := newBackupPolicyReplacementFixture(t, true)
	volume := policy.sources[0].Source.Record
	seed, err := policy.repository.PrepareBackupPolicyReplacement(ctx, BackupPolicyReplacementInput{
		EnvironmentID: policy.environment.Record.ID, Enabled: true, Frequency: "*-*-* 02:00:00",
		Keep: 7, Encryption: "none", ConnectorID: policy.connector.Record.Connector.ID,
		Sources: []BackupPolicySourceSelection{{Kind: core.BackupSourceVolume, TargetID: volume.TargetID}},
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
		withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
			EnvironmentID: policy.environment.Record.ID, RevisionID: ids.New(ids.KindTask), RenderGeneration: 1,
			Volumes: []EnvironmentVolumeIdentity{{ID: volume.TargetID, Slug: "backup-data", Key: "backup-data"}},
			Backup: &EnvironmentBlueprintBackupPolicy{
				Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 7, Encryption: "none",
				ConnectorID: policy.connector.Record.Connector.ID,
				Sources: []EnvironmentBlueprintBackupPolicySource{
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
	task.Type, task.Target, task.RenderGeneration = TaskRemove, volume.TargetID, 2
	task.Params[TaskResourceKindParam] = TaskResourceVolume
	marker := environmentBlueprintTestMarker(task, policy.environment.Record.ID)
	marker.Locator.Method, marker.Locator.Route = "DELETE", "/volumes/{id}"
	marker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetVolume, ID: volume.TargetID}
	projection := cloneEnvironmentComposeProjection(current.Record)
	projection.RevisionID, projection.RenderGeneration = task.ID, 2
	projection.Volumes, projection.VolumeMounts = nil, nil
	projection.Backup = prepared.Projection()
	projection = withTestEnvironmentComposeArtifact(projection)
	digest, err := EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	precondition, err := EnvironmentBlueprintDependencyDigest(current.Record)
	if err != nil {
		t.Fatal(err)
	}
	return &VolumePolicyDesiredFixture{
		Store: store, Task: task, Marker: marker, HeadRevision: head.Revision, prepared: prepared, policy: policy, store: store,
		Request: EnvironmentBlueprintStageRequest{
			Projection: projection, DependencyDigest: digest,
			Mutation: &EnvironmentDesiredMutationAudit{Volume: &EnvironmentVolumeMutationAudit{
				Action: EnvironmentVolumeMutationRemove, VolumeID: volume.TargetID,
				Slug: "backup-data", Key: "backup-data", KeySupplied: true, PreconditionDigest: precondition,
			}},
		},
	}
}

func (fixture *VolumePolicyDesiredFixture) Publish(ctx context.Context) (IdempotencyTransactionResult, error) {
	fixture.store.ownerBeforePublication = fixture.OwnerBeforePublication
	fixture.store.environmentLockBeforePublication = fixture.EnvironmentLockBeforePublication
	fixture.store.writerBeforePublication = fixture.writerBeforePublication
	fixture.store.parentBeforePublication = fixture.ParentBeforePublication
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
		EnvironmentDesiredRevisionIdentity{
			EnvironmentID: fixture.Task.Owner.EnvironmentID,
			RevisionID:    fixture.Task.ID,
		},
		fixture.Request.Projection,
		nil,
		nil,
		nil,
		ReleaseGroupBlueprintPreparedMutation{},
		ComponentTaskPreparation{},
		BlueprintAttachTaskPreparation{},
		BlueprintBackupPolicyPreparation{},
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
	if _, err := fixture.Store.Put(context.Background(), taskMaterializationWriterKey(writer.EnvironmentID), value); err != nil {
		t.Fatal(err)
	}
}

// UseMaximumSelection seeds the legal 12-source policy, then prepares the real
// removal against that same desired baseline. It does not change any limit.
func (fixture *VolumePolicyDesiredFixture) UseMaximumSelection(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: fixture.Task.Owner.EnvironmentID, RevisionID: ids.New(ids.KindTask), RenderGeneration: 1,
		Volumes: []EnvironmentVolumeIdentity{{ID: fixture.Task.Target, Slug: "backup-data", Key: "backup-data"}},
	})
	first := fixture.policy.sources[0].Source.Record
	input := EnvironmentBlueprintBackupPolicyInput{
		EnvironmentID: projection.EnvironmentID, TaskID: projection.RevisionID,
		ReadRevision: fixture.Revision(), Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 7, Encryption: "none",
		ConnectorName: fixture.policy.connector.Record.Connector.Name, CreatedAt: fixture.policy.now.Add(time.Minute),
		Sources: []EnvironmentBlueprintBackupPolicySourceInput{
			{CandidateID: first.ID, Kind: first.Kind, TargetID: first.TargetID},
		},
	}
	for index := 1; index < MaximumBackupPolicySources; index++ {
		volumeID := ids.New(ids.KindVolume)
		label := "volume-" + string(rune('a'+index))
		projection.Volumes = append(
			projection.Volumes,
			EnvironmentVolumeIdentity{ID: volumeID, Slug: label, Key: label},
		)
		input.Sources = append(input.Sources, EnvironmentBlueprintBackupPolicySourceInput{
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
	publication, err := prepareBlueprintBackupPolicyPublication(
		TaskRecord{ID: projection.RevisionID, Target: projection.EnvironmentID}, projection,
		BlueprintAttachTaskPreparation{}, seed,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clearPreparedBlueprintBackupPolicyPublication(publication)
	result, err := fixture.policy.store.Transact(ctx, publication.conditions, publication.mutations)
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
	precondition, err := EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		t.Fatal(err)
	}
	projection.RevisionID, projection.RenderGeneration = fixture.Task.ID, 2
	projection.Volumes = projection.Volumes[1:]
	projection.Backup = fixture.prepared.Projection()
	projection = withTestEnvironmentComposeArtifact(projection)
	digest, err := EnvironmentBlueprintDependencyDigest(projection)
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
	task := fixture.policy.store.valueAt(taskKey(fixture.Task.ID), fixture.Revision())
	policy, found, err := fixture.policy.repository.GetBackupPolicy(
		context.Background(),
		fixture.Task.Owner.EnvironmentID,
	)
	if err != nil || !found || task == nil || task.ModRevision != policy.Revision || !policy.Record.Enabled ||
		len(policy.Record.SourceIDs) != MaximumBackupPolicySources-1 {
		t.Fatalf("maximum policy was not replaced with its Task: %v", err)
	}
	for _, source := range fixture.prepared.state.sources {
		value := fixture.policy.store.valueAt(backupSourceKey(source.ID), fixture.Revision())
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
	wantConditions, wantMutations := 38, 15
	if fixture.Initial != nil {
		wantConditions += 6
		wantMutations += 7
	} else {
		wantConditions++ // Ordinary desired mutation must exclude the Environment removal lock too.
	}
	if len(fixture.store.conditions) != wantConditions || len(fixture.store.mutations) != wantMutations ||
		fixture.store.bytes > 1024*1024 {
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
	task := fixture.policy.store.valueAt(taskKey(fixture.Task.ID), fixture.policy.store.revision)
	if task == nil {
		t.Fatal("publisher did not write Task")
	}
	for _, key := range []string{backupPolicyKey(fixture.Task.Owner.EnvironmentID), environmentCoordinationKey(fixture.Task.Owner.EnvironmentID), environmentBlueprintHeadKey(fixture.Task.Owner.EnvironmentID)} {
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
		environmentCoordinationKey(fixture.Task.Owner.EnvironmentID),
		fixture.Revision(),
	)
	coordination, err := decodeEnvironmentCoordinationRecord(coordinationValue.Value)
	if err != nil || coordination.CurrentBackupScheduleState != nil ||
		!coordination.ScheduleClockFloor.Equal(policy.Record.UpdatedAt) {
		t.Fatalf("scheduling floor does not use the exact publication clock: %v", err)
	}
	if value := fixture.policy.store.valueAt(backupSourceKey(fixture.policy.sources[0].Source.Record.ID), fixture.policy.store.revision); value == nil {
		t.Fatal("historical source was removed")
	}
	if value := fixture.policy.store.valueAt(backupPolicyConnectorReferenceKey(fixture.policy.connector.Record.Connector.ID, fixture.Task.Owner.EnvironmentID), fixture.policy.store.revision); value != nil {
		t.Fatal("disabled policy retained its active Connector reference")
	}
	wantConditions, wantMutations := 28, 16
	if fixture.Initial != nil {
		wantConditions, wantMutations = 33, 23
	}
	if len(fixture.store.conditions) != wantConditions || len(fixture.store.mutations) != wantMutations ||
		fixture.store.bytes > 900*1024 {
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

func (fixture *VolumePolicyDesiredFixture) AssertRemovalAncestry(t *testing.T, baselineRevision int64) {
	t.Helper()
	for _, identity := range []HierarchyCoordinationRecord{
		{TargetKind: HierarchyDeletionTargetProject, TargetID: fixture.Task.Owner.ProjectID},
		{TargetKind: HierarchyDeletionTargetTenant, TargetID: fixture.Task.Owner.TenantID},
	} {
		key := HierarchyCoordinationKey(string(identity.TargetKind), identity.TargetID)
		previous, err := fixture.Store.GetMany(
			context.Background(),
			GetManyRequest{Keys: []string{key}, Revision: baselineRevision},
		)
		if err != nil || previous == nil || len(previous.Values) != 1 || previous.Values[0] == nil {
			t.Fatalf("parent baseline: %v", err)
		}
		before, err := decodeHierarchyCoordination(previous.Values[0].Value)
		if err != nil {
			t.Fatal(err)
		}
		current, err := fixture.Store.Get(context.Background(), key)
		if err != nil || current == nil || current.Entry == nil {
			t.Fatalf("parent publication: %v", err)
		}
		after, err := decodeHierarchyCoordination(current.Entry.Value)
		before.MutationEpoch++
		if err != nil || after != before || current.Entry.ModRevision != fixture.Revision() {
			t.Fatalf("removal did not atomically advance %s deletion epoch: %v", identity.TargetKind, err)
		}
	}
}

func (fixture *VolumePolicyDesiredFixture) CorruptRemovalParent(
	t *testing.T, kind HierarchyDeletionTargetKind, change string,
) {
	t.Helper()
	id := fixture.Task.Owner.ProjectID
	idKind := ids.KindProject
	if kind == HierarchyDeletionTargetTenant {
		id, idKind = fixture.Task.Owner.TenantID, ids.KindTenant
	}
	key := HierarchyCoordinationKey(string(kind), id)
	if change == "missing" {
		if _, err := fixture.Store.Transact(context.Background(), nil, []Mutation{{Type: MutationDelete, Key: key}}); err != nil {
			t.Fatal(err)
		}
		return
	}
	value := []byte("corrupt")
	if change == "identity" {
		var err error
		value, err = encodeHierarchyCoordination(HierarchyCoordinationRecord{
			Schema: 1, TargetKind: kind, TargetID: ids.New(idKind), MutationEpoch: 1,
		})
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
		"policy":       backupPolicyKey(fixture.Task.Owner.EnvironmentID),
		"coordination": environmentCoordinationKey(fixture.Task.Owner.EnvironmentID),
		"epoch":        environmentMutationEpochKey(fixture.Task.Owner.EnvironmentID),
		"source":       backupSourceKey(fixture.policy.sources[0].Source.Record.ID),
		"connector reference": backupPolicyConnectorReferenceKey(
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
	if value := fixture.policy.store.valueAt(taskKey(fixture.Task.ID), revision); value != nil {
		t.Fatal("rejected publication wrote Task")
	}
	head := fixture.policy.store.valueAt(environmentBlueprintHeadKey(fixture.Task.Owner.EnvironmentID), revision)
	if head == nil || head.ModRevision != fixture.HeadRevision {
		t.Fatal("rejected publication changed desired head")
	}
	markerKey, err := idempotencyMarkerKey(fixture.Marker.Locator)
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
			claim := EnvironmentBlueprintStageClaim{
				SourceKind: EnvironmentBlueprintSourceMutation, EnvironmentID: fixture.Task.Owner.EnvironmentID,
			}
			task, marker := cloneTaskRecord(fixture.Task), fixture.Marker
			removed := fixture.Task.Target
			switch changed {
			case "source kind":
				claim.SourceKind = EnvironmentBlueprintSourceApply
			case "environment":
				claim.EnvironmentID = ids.New(ids.KindEnvironment)
			case "removed Volume":
				removed = ids.New(ids.KindVolume)
			case "Task type":
				task.Type = TaskUpdate
			case "Task target":
				task.Target = ids.New(ids.KindVolume)
			case "resource kind":
				task.Params[TaskResourceKindParam] = TaskResourceEntry
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
