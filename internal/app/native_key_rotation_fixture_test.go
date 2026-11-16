package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type nativeRotationStore struct {
	*memoryHierarchyStore
	failNext  bool
	loseReply bool
}

func (store *nativeRotationStore) Transact(ctx context.Context, conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	if store.failNext {
		store.failNext = false
		return keyvalue.TransactionResult{}, errors.New("injected rotation storage failure")
	}
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.loseReply {
		store.loseReply = false
		return keyvalue.TransactionResult{}, errors.New("injected lost committed rotation response")
	}
	return result, err
}

type nativeRotationFixture struct {
	store           *nativeRotationStore
	repository      *etcd.BackupPolicyRepository
	queue           *workqueue.Repository
	input           etcd.BackupKeyRotationInput
	marker          idempotency.IdempotencyMarker
	oldKey, nextKey ageinfra.Keypair
	nextCiphertext  []byte
}

func newNativeRotationFixture(t *testing.T) *nativeRotationFixture {
	t.Helper()
	f := &nativeRotationFixture{store: &nativeRotationStore{memoryHierarchyStore: newMemoryHierarchyStore()}}
	at := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Minute)
	put := func(key string, raw []byte, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		defer clear(raw)
		if _, err := f.store.Put(t.Context(), key, raw); err != nil {
			t.Fatal(err)
		}
	}
	tenant := hierarchy.TenantRecord{ID: ids.New(ids.KindTenant), Slug: "rotation", Name: "Rotation"}
	raw, err := hierarchy.EncodeTenant(tenant)
	put(hierarchy.TenantKey(tenant.ID), raw, err)
	project := hierarchy.ProjectRecord{ID: ids.New(ids.KindProject), TenantID: tenant.ID,
		Slug: "rotation", Name: "Rotation", Kind: hierarchy.ProjectKindTenant}
	raw, err = hierarchy.EncodeProject(project)
	put(hierarchy.ProjectKey(project.ID), raw, err)
	for kind, id := range map[hierarchydeletion.HierarchyDeletionTargetKind]string{
		hierarchydeletion.HierarchyDeletionTargetTenant:  tenant.ID,
		hierarchydeletion.HierarchyDeletionTargetProject: project.ID,
	} {
		raw, err = hierarchydeletion.EncodeInitialCoordination(kind, id)
		put(hierarchydeletion.HierarchyCoordinationKey(string(kind), id), raw, err)
	}
	environment, err := hierarchy.NewProvisioningEnvironment("/var/lib/groundplane/vol", project,
		ids.New(ids.KindEnvironment), "qa", "10.40.0.0/24", ids.New(ids.KindTask), at)
	if err != nil {
		t.Fatal(err)
	}
	environment, err = hierarchy.CompleteEnvironmentProvisioning(environment, environment.CreateTaskID, true)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = hierarchy.EncodeEnvironment(environment)
	put(hierarchy.EnvironmentKey(environment.ID), raw, err)
	raw, err = backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: environment.ID,
	})
	put(hierarchy.EnvironmentMutationEpochKey(environment.ID), raw, err)
	f.oldKey, err = ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	f.nextKey, err = ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	wrapping, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	oldCiphertext, err := ageinfra.Encrypt(wrapping.Recipient, []byte(f.oldKey.Identity))
	if err != nil {
		t.Fatal(err)
	}
	defer clear(oldCiphertext)
	f.nextCiphertext, err = ageinfra.Encrypt(wrapping.Recipient, []byte(f.nextKey.Identity))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(f.nextCiphertext); clear(f.marker.Intent.Ciphertext); clear(f.marker.Response.Body) })
	raw, err = backuppolicy.EncodeBackupKeyRecord(backuppolicy.BackupKeyRecord{
		EnvironmentID: environment.ID, Recipient: f.oldKey.Recipient, KeyEra: 1, CreatedAt: at, RotatedAt: at,
	})
	put(backuppolicy.BackupKeyKey(environment.ID), raw, err)
	raw, err = backuppolicy.EncodeBackupKeyEncryptedValue(backuppolicy.BackupKeyEncryptedValue{
		EnvironmentID: environment.ID, KeyEra: 1, Ciphertext: oldCiphertext,
	})
	put(backuppolicy.BackupKeyValueKey(environment.ID), raw, err)
	f.repository, err = etcd.NewBackupPolicyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.queue, err = workqueue.NewRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.input = etcd.BackupKeyRotationInput{EnvironmentID: environment.ID, TaskID: ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem), OperationID: ids.New(ids.KindOperation),
		PlanID: ids.New(ids.KindPlan), CreatedAt: at}
	intent, err := ageinfra.Encrypt(wrapping.Recipient, []byte("captured rotation request"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(intent)
	body, err := json.Marshal(map[string]string{"task_id": f.input.TaskID})
	if err != nil {
		t.Fatal(err)
	}
	f.marker = idempotency.IdempotencyMarker{Kind: idempotency.IdempotencyMarkerTask,
		State: idempotency.IdempotencyMarkerPending, TaskID: f.input.TaskID, CreatedAt: at, UpdatedAt: at,
		Locator: idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
			ScopeID: environment.ID, Method: "POST", Route: "/environments/{id}/rotate-key", Key: "native-rotation-request"},
		Intent: idempotency.ProtectedIntentRecord{EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: intent},
		Response: idempotency.IdempotencyResponse{Status: 202, ContentKind: "application/json", Body: body}}
	publishRotationFixture(
		t,
		t.Context(),
		f.store,
		f.repository,
		f.input,
		backuppolicymutations.BackupPolicyInitialKeyMaterial{
			Recipient:  f.nextKey.Recipient,
			Ciphertext: f.nextCiphertext,
		},
		f.marker,
	)
	return f
}

func (f *nativeRotationFixture) settlement(t *testing.T, outcome workexecution.Outcome) workqueue.Change {
	t.Helper()
	view, err := f.queue.ReadWork(t.Context(), f.input.TaskID, f.input.WorkItemID, 0)
	if err != nil {
		t.Fatal(err)
	}
	change, err := f.repository.PrepareBackupKeyRotationSettlement(t.Context(), view, outcome,
		f.input.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { keyvalue.ClearMutationValues(change.Mutations) })
	return change
}

func (f *nativeRotationFixture) state(t *testing.T, era int, phase workexecution.Phase,
	outcome workexecution.Outcome, rotationState backupruntime.BackupKeyRotationState, locked bool,
) {
	t.Helper()
	read, err := f.store.GetMany(t.Context(), keyvalue.GetManyRequest{Keys: []string{
		backuppolicy.BackupKeyKey(f.input.EnvironmentID), backuppolicy.BackupKeyValueKey(f.input.EnvironmentID),
		backupruntime.BackupKeyRotationKey(
			f.input.TaskID,
		), hierarchy.EnvironmentOperationLockKey(f.input.EnvironmentID),
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearValues(read.Values)
	key, err := backuppolicy.DecodeBackupKeyRecord(read.Values[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	value, err := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[1].Value)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value.Ciphertext)
	rotation, err := backupruntime.DecodeBackupKeyRotationRecord(read.Values[2].Value)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(rotation.NextEncryptedIdentity)
	view, err := f.queue.ReadWork(t.Context(), f.input.TaskID, f.input.WorkItemID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if key.KeyEra != era || value.KeyEra != era || rotation.State != rotationState ||
		(read.Values[3] != nil) != locked || view.Item.Item.Phase != phase || view.Item.Item.Outcome != outcome {
		t.Fatalf(
			"inconsistent rotation: era=%d/%d state=%s locked=%t phase=%s outcome=%s",
			key.KeyEra,
			value.KeyEra,
			rotation.State,
			read.Values[3] != nil,
			view.Item.Item.Phase,
			view.Item.Item.Outcome,
		)
	}
}

func publishRotationFixture(
	t *testing.T,
	ctx context.Context,
	store keyvalue.Store,
	repository *etcd.BackupPolicyRepository,
	input etcd.BackupKeyRotationInput,
	material backuppolicymutations.BackupPolicyInitialKeyMaterial,
	marker idempotency.IdempotencyMarker,
) {
	t.Helper()
	owner, err := repository.ReadBackupTaskAcceptance(ctx, input.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	protector, encrypted := manualJourneyEncryptedValue(t, "rotation-request")
	clear(encrypted)
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	submitter, err := newAcceptedActions(store, protector, queue, replay, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := submitter.Begin(
		ctx,
		requestidempotency.CanonicalIntentV1{
			Method: marker.Locator.Method,
			Route:  marker.Locator.Route,
			Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: input.EnvironmentID},
			Path:   []requestidempotency.PathBinding{{Name: "id", Value: input.EnvironmentID}},
			Query:  requestidempotency.Object(),
			Body:   requestidempotency.JSONBody(requestidempotency.Object()),
		},
		marker.Locator,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Destroy()
	task := workexecution.Task{
		ID:         input.TaskID,
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionRotate,
		TargetID:   input.EnvironmentID,
		TargetName: owner.Name,
		Scope: workexecution.Scope{
			TenantID:      owner.Owner.TenantID,
			ProjectID:     owner.Owner.ProjectID,
			EnvironmentID: input.EnvironmentID,
		},
		CreatedAt: input.CreatedAt,
		UpdatedAt: input.CreatedAt,
		Deadline:  input.CreatedAt.Add(acceptedaction.QueueTimeout),
	}
	if _, err := acceptedaction.Submit(ctx, submitter, intent.Submission(task, "backup-key-rotate"), struct{}{}); err != nil {
		t.Fatal(err)
	}
	accepted, _, err := queue.ReadTask(ctx, task.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := repository.PrepareBackupKeyRotation(ctx, input, material)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Clear()
	captured, err := repository.PrepareBackupKeyRotationAuthority(ctx, prepared, accepted.Task, input.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(captured.Authority.Mutations)
	promotion, err := queue.PrepareAcceptedTaskPromotion(
		ctx,
		accepted,
		captured.Task,
		[]workexecution.Item{captured.Item},
		nil,
		false,
		input.CreatedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer keyvalue.ClearMutationValues(promotion.Mutations)
	result, err := queue.Commit(ctx, promotion, captured.Authority)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil || !result.Succeeded {
		t.Fatalf("rotation promotion failed: %v", err)
	}
}
