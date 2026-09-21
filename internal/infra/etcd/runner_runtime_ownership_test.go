package etcd

import (
	"context"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testrunners "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strings"
	"testing"
	"time"
)

// Rationale: socket attestation must atomically establish the sole lifecycle
// container identity and immutable byte-replayable runtime ownership epoch.
func TestRunnerRuntimeOwnershipAttestationAndReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(230, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	task := runnerTestTask(desired, testtaskjournal.TaskCreate, 231, "runner-runtime-attest-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, task, runnerTestMarker(task, desired),
	); err != nil {
		t.Fatal(err)
	}
	current, err := repository.GetRunner(ctx, desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	containerID := strings.Repeat("c", testrunners.RunnerContainerIDEncodedLength)
	ownership := runnerTestRuntimeOwnership(desired.ID, current.Record.RuntimeEpoch+1)
	attested, err := repository.AttestRunnerRuntimeOwnership(ctx, current, containerID, ownership)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := repository.AttestRunnerRuntimeOwnership(ctx, current, containerID, ownership)
	if err != nil || replay.Revision != attested.Revision || replay.Record != ownership {
		t.Fatalf("attestation replay = %#v, %v", replay, err)
	}
	bound, err := repository.GetRunner(ctx, desired.ID)
	if err != nil || bound.Record.ContainerID != containerID ||
		bound.Record.RuntimeEpoch != ownership.RuntimeEpoch || bound.Revision != current.Revision {
		t.Fatalf("bound lifecycle = %#v, %v", bound, err)
	}
	altered := ownership
	altered.SocketInode++
	if _, err := repository.AttestRunnerRuntimeOwnership(ctx, current, containerID, altered); err == nil {
		t.Fatal("altered attestation replay succeeded")
	}
}

// Rationale: an attested create failure must retain exact ownership until a
// cleanup epoch proves removal; retry may start only after that proof deletes
// the old ownership record.
func TestRunnerFailedAttestedRuntimeCleanupThenRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(235, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	source := runnerTestTask(desired, testtaskjournal.TaskCreate, 236, "runner-failed-runtime-create-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, source, runnerTestMarker(source, desired),
	); err != nil {
		t.Fatal(err)
	}
	provisioning, err := repository.GetRunner(ctx, desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	containerID := strings.Repeat("e", testrunners.RunnerContainerIDEncodedLength)
	ownership := runnerTestRuntimeOwnership(desired.ID, provisioning.Record.RuntimeEpoch+1)
	if _, err := repository.AttestRunnerRuntimeOwnership(ctx, provisioning, containerID, ownership); err != nil {
		t.Fatal(err)
	}
	failed := runnerTestFinishCreate(t, store, repository, source, testtaskjournal.TaskStatusFailed)
	if failed.Record.ProvisioningState != testrunners.RunnerProvisioningFailed ||
		failed.Record.ContainerID != containerID || failed.Record.RuntimeEpoch != ownership.RuntimeEpoch {
		t.Fatalf("failed attested lifecycle = %#v", failed.Record.RunnerLifecycleRecord)
	}
	retry := runnerTestTask(desired, testtaskjournal.TaskCreate, 237, source.IdempotencyKey)
	retry.RetryOf = source.ID
	retry.OperationID = source.OperationID
	retryMarker := runnerTestMarker(retry, desired)
	retryMarker.Locator.Key = "runner-failed-runtime-retry-01"
	blocked, err := repository.RetryRunnerCreationWithTask(ctx, source.ID, retry, retryMarker)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, conflict, classifyErr := blocked.Classify(); classifyErr != nil || conflict == nil {
		t.Fatalf("retry with retained ownership conflict/error = %#v/%v", conflict, classifyErr)
	}
	cleanup, err := repository.BeginFailedRunnerRuntimeCleanup(ctx, failed)
	if err != nil {
		t.Fatal(err)
	}
	if cleanup.Record.RuntimeEpoch != failed.Record.RuntimeEpoch+1 || cleanup.Record.ContainerID != containerID {
		t.Fatalf("cleanup lifecycle = %#v", cleanup.Record.RunnerLifecycleRecord)
	}
	replay, err := repository.BeginFailedRunnerRuntimeCleanup(ctx, failed)
	if err != nil || replay.Record.RuntimeEpoch != cleanup.Record.RuntimeEpoch ||
		replay.Record.LifecycleRevision != cleanup.Record.LifecycleRevision {
		t.Fatalf("cleanup replay = %#v, %v", replay, err)
	}
	wrong := ownership
	wrong.SocketInode++
	if _, err := repository.DeleteRunnerRuntimeOwnershipAfterCleanup(ctx, cleanup, wrong); err == nil {
		t.Fatal("failed-attempt cleanup accepted altered ownership evidence")
	}
	if _, err := repository.DeleteRunnerRuntimeOwnershipAfterCleanup(ctx, cleanup, ownership); err != nil {
		t.Fatal(err)
	}
	if _, found, err := repository.GetRunnerRuntimeOwnership(ctx, desired.ID); err != nil || found {
		t.Fatalf("ownership after failed-attempt cleanup found/error = %t/%v", found, err)
	}
	if _, err := repository.RetryRunnerCreationWithTask(ctx, source.ID, retry, retryMarker); err != nil {
		t.Fatal(err)
	}
	retried, err := repository.GetRunner(ctx, desired.ID)
	if err != nil || retried.Record.ProvisioningState != testrunners.RunnerProvisioningProvisioning ||
		retried.Record.CreateTaskID != retry.ID || retried.Record.ContainerID != "" ||
		retried.Record.RuntimeEpoch != cleanup.Record.RuntimeEpoch+1 {
		t.Fatalf("retried lifecycle = %#v, %v", retried.Record.RunnerLifecycleRecord, err)
	}
}

func TestRunnerSuccessfulCreateAcknowledgementFailsClosedWithoutReadinessProof(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(238, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	task := runnerTestTask(desired, testtaskjournal.TaskCreate, 239, "runner-readiness-fail-closed-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, task, runnerTestMarker(task, desired),
	); err != nil {
		t.Fatal(err)
	}
	provisioning, err := repository.GetRunner(ctx, desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	containerID := strings.Repeat("f", testrunners.RunnerContainerIDEncodedLength)
	ownership := runnerTestRuntimeOwnership(desired.ID, provisioning.Record.RuntimeEpoch+1)
	if _, err := repository.AttestRunnerRuntimeOwnership(ctx, provisioning, containerID, ownership); err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("claim create found/error = %t/%v", found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
	); err == nil {
		t.Fatal("successful create acknowledgement published without exact readiness proof")
	}
	current, err := repository.GetRunner(ctx, desired.ID)
	if err != nil || current.Record.ProvisioningState != testrunners.RunnerProvisioningProvisioning ||
		current.Record.ContainerID != containerID || current.Record.RuntimeEpoch != ownership.RuntimeEpoch {
		t.Fatalf("lifecycle after blocked success = %#v, %v", current.Record.RunnerLifecycleRecord, err)
	}
	stored, found, err := repository.GetRunnerRuntimeOwnership(ctx, desired.ID)
	if err != nil || !found || stored.Record != ownership {
		t.Fatalf("ownership after blocked success = %#v/%t/%v", stored, found, err)
	}
}

// Rationale: Runner readiness is a single-use durable authorization, not an
// inference from container ownership or a replayable flag.
func TestRunnerSuccessfulCreateAcknowledgementConsumesExactReadinessProof(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(243, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	task := runnerTestTask(desired, testtaskjournal.TaskCreate, 244, "runner-readiness-consume-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, task, runnerTestMarker(task, desired),
	); err != nil {
		t.Fatal(err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("claim create found/error = %t/%v", found, err)
	}
	proof := runnerTestRecordReadinessProof(t, repository, task)
	terminal, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask() = %#v, %v", terminal, err)
	}
	stored, err := store.Get(ctx, testrunners.RunnerReadinessProofKey(task.ID))
	if err != nil || stored.Entry != nil {
		t.Fatalf("consumed readiness proof = %#v, %v; recorded revision %d", stored, err, proof.Revision)
	}
	ready, err := repository.GetRunner(ctx, desired.ID)
	if err != nil || ready.Record.ProvisioningState != testrunners.RunnerProvisioningReady {
		t.Fatalf("ready Runner = %#v, %v", ready, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
	}
}

// Rationale: deletion may release ownership only while holding the exact
// deletion-epoch lifecycle and byte-identical daemon ownership evidence.
func TestRunnerRuntimeOwnershipCleanupFencesRemoval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(240, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	createTask := runnerTestTask(desired, testtaskjournal.TaskCreate, 241, "runner-runtime-create-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, createTask, runnerTestMarker(createTask, desired),
	); err != nil {
		t.Fatal(err)
	}
	provisioning, err := repository.GetRunner(ctx, desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	containerID := strings.Repeat("d", testrunners.RunnerContainerIDEncodedLength)
	ownership := runnerTestRuntimeOwnership(desired.ID, provisioning.Record.RuntimeEpoch+1)
	if _, err := repository.AttestRunnerRuntimeOwnership(ctx, provisioning, containerID, ownership); err != nil {
		t.Fatal(err)
	}
	ready := runnerTestFinishCreate(t, store, repository, createTask, testtaskjournal.TaskStatusCompleted)
	removeTask := runnerTestTask(desired, testtaskjournal.TaskRemove, 242, "runner-runtime-remove-01")
	removeTask.Params = testrunners.RunnerRemovalTaskParams(ready.Record)
	tombstone := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetRunner, TargetID: desired.ID, TargetRevision: ready.Revision,
		TaskID: removeTask.ID, Phase: testdeletions.DeletionPhaseFinalizing,
		CreatedAt: removeTask.CreatedAt, UpdatedAt: removeTask.CreatedAt,
	}
	if _, err := repository.BeginRunnerRemovalWithTask(
		ctx, ready, tombstone, removeTask, runnerTestMarker(removeTask, desired),
	); err != nil {
		t.Fatal(err)
	}
	deleting, err := repository.GetRunner(ctx, desired.ID)
	if err != nil || deleting.Record.RuntimeEpoch <= ownership.RuntimeEpoch {
		t.Fatalf("deletion lifecycle = %#v, %v", deleting, err)
	}
	wrong := ownership
	wrong.DaemonInstanceNonce = strings.Repeat("b", 64)
	if _, err := repository.DeleteRunnerRuntimeOwnershipAfterCleanup(ctx, deleting, wrong); err == nil {
		t.Fatal("mismatched cleanup proof succeeded")
	}
	if _, err := repository.DeleteRunnerRuntimeOwnershipAfterCleanup(ctx, deleting, ownership); err != nil {
		t.Fatal(err)
	}
	if _, found, err := repository.GetRunnerRuntimeOwnership(ctx, desired.ID); err != nil || found {
		t.Fatalf("runtime ownership after cleanup found/error = %t/%v", found, err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(
		ctx,
		removeTask.CreatedAt.Add(time.Second),
	); err != nil ||
		!found {
		t.Fatalf("claim removal found/error = %t/%v", found, err)
	}
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, removeTask.ID, testtaskjournal.TaskStatusCompleted, removeTask.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.GetRunner(ctx, desired.ID); err == nil {
		t.Fatal("removed Runner remained readable")
	}
}

// Rationale: schema-one runtime ownership must reject ambiguous socket paths,
// noncanonical nonce encodings, zero kernel identity, and non-UTC timestamps.
func TestRunnerRuntimeOwnershipValidationAndEncoding(t *testing.T) {
	t.Parallel()
	valid := runnerTestRuntimeOwnership(runnerTestDesired(
		250, testrunners.RunnerOwnerTenant, "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV", "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	).ID, 2)
	value, err := testrunners.EncodeRunnerRuntimeOwnership(valid)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(value), `"socket_device":123`) ||
		!strings.Contains(string(value), `"socket_inode":456`) {
		t.Fatalf("uint64 ownership encoding = %s", value)
	}
	for _, mutate := range []func(*testrunners.RunnerRuntimeOwnershipRecord){
		func(record *testrunners.RunnerRuntimeOwnershipRecord) {
			record.DaemonSocketEndpoint = "unix://relative.sock"
		},
		func(record *testrunners.RunnerRuntimeOwnershipRecord) {
			record.DaemonSocketEndpoint = "unix:///run/../tmp/runner.sock"
		},
		func(record *testrunners.RunnerRuntimeOwnershipRecord) {
			record.DaemonSocketEndpoint = "unix:///run/groundplane/runners/other/xdg/docker.sock"
		},
		func(record *testrunners.RunnerRuntimeOwnershipRecord) {
			record.DaemonInstanceNonce = strings.Repeat("A", 64)
		},
		func(record *testrunners.RunnerRuntimeOwnershipRecord) { record.SocketDevice = 0 },
		func(record *testrunners.RunnerRuntimeOwnershipRecord) {
			record.CreatedAt = record.CreatedAt.In(time.FixedZone("UTC", 0))
		},
	} {
		candidate := valid
		mutate(&candidate)
		if _, err := testrunners.EncodeRunnerRuntimeOwnership(candidate); err == nil {
			t.Fatalf("invalid ownership passed: %#v", candidate)
		}
	}
}

func TestRunnerPersistenceStrictBoundsAndEpochExhaustion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(255, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	task := runnerTestTask(desired, testtaskjournal.TaskCreate, 256, "runner-strict-boundaries-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, task, runnerTestMarker(task, desired),
	); err != nil {
		t.Fatal(err)
	}
	current, err := repository.GetRunner(ctx, desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, containerID := range []string{
		"", strings.Repeat("a", testrunners.RunnerContainerIDEncodedLength-1),
		strings.Repeat("A", testrunners.RunnerContainerIDEncodedLength), strings.Repeat("g", testrunners.RunnerContainerIDEncodedLength),
	} {
		if _, err := testrunners.BindRunnerContainerID(current.Record, task.ID, containerID); err == nil {
			t.Fatalf("invalid container id passed: %q", containerID)
		}
	}
	exhausted := current.Record
	exhausted.RuntimeEpoch = ^uint64(0)
	if _, err := testrunners.BindRunnerContainerID(
		exhausted, task.ID, strings.Repeat("a", testrunners.RunnerContainerIDEncodedLength),
	); err == nil {
		t.Fatal("container binding advanced an exhausted runtime epoch")
	}
	exhausted.ProvisioningState = testrunners.RunnerProvisioningFailed
	exhausted.ContainerID = strings.Repeat("a", testrunners.RunnerContainerIDEncodedLength)
	if _, err := testrunners.TakeRunnerRuntimeCleanupOwnership(exhausted); err == nil {
		t.Fatal("cleanup advanced an exhausted runtime epoch")
	}
	badTime := current.Record.RunnerLifecycleRecord
	badTime.CreatedAt = badTime.CreatedAt.In(time.FixedZone("UTC", 0))
	if _, err := testrunners.EncodeRunnerLifecycleRecord(badTime); err == nil {
		t.Fatal("noncanonical UTC timestamp encoded")
	}
	oversized := make([]byte, testrunners.MaximumRunnerPersistenceBytes+1)
	decoders := []func([]byte) error{
		func(value []byte) error { _, err := testrunners.DecodeRunnerDesiredRecord(value); return err },
		func(value []byte) error { _, err := testrunners.DecodeRunnerLifecycleRecord(value); return err },
		func(value []byte) error { _, err := testrunners.DecodeRunnerRuntimeOwnership(value); return err },
		func(value []byte) error { _, err := testrunners.DecodeRunnerObservation(value); return err },
		func(value []byte) error { _, err := testrunners.DecodeRunnerHostSlotRecord(value); return err },
		func(value []byte) error { _, err := testrunners.DecodeRunnerTenantQuota(value); return err },
		func(value []byte) error { _, err := testrunners.DecodeSystemPoolRegistry(value); return err },
		func(value []byte) error { _, err := testrunners.DecodeRunnerRemovalIntent(value); return err },
		func(value []byte) error { _, err := testrunners.DecodeRunnerDeletionTombstone(value); return err },
	}
	for index, decode := range decoders {
		if err := decode(oversized); err == nil {
			t.Fatalf("oversized Runner persistence decoder %d succeeded", index)
		}
	}
}

func runnerTestRuntimeOwnership(runnerID string, epoch uint64) testrunners.RunnerRuntimeOwnershipRecord {
	return testrunners.RunnerRuntimeOwnershipRecord{
		RunnerID: runnerID, RuntimeEpoch: epoch,
		DaemonSocketEndpoint: "unix:///run/groundplane/runners/" + runnerID + "/xdg/docker.sock",
		DaemonInstanceNonce:  strings.Repeat("a", 64), SocketDevice: 123, SocketInode: 456,
		CreatedAt: taskJournalTime().Add(time.Second),
	}
}
