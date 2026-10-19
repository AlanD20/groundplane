package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	testrunners "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
)

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
	decoded, err := testrunners.DecodeRunnerRuntimeOwnership(value)
	if err != nil || decoded != valid {
		t.Fatalf("ownership round trip = %#v, %v", decoded, err)
	}
	for _, mutate := range []func(*testrunners.RunnerRuntimeOwnershipRecord){
		func(record *testrunners.RunnerRuntimeOwnershipRecord) {
			record.DaemonSocketEndpoint = "unix://relative.sock"
		},
		func(record *testrunners.RunnerRuntimeOwnershipRecord) {
			record.DaemonSocketEndpoint = "unix:///run/../tmp/runner.sock"
		},
		func(record *testrunners.RunnerRuntimeOwnershipRecord) {
			record.DaemonSocketEndpoint = "unix:///run/groundplane-runners/other/xdg/docker.sock"
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
	fixture, err := prepareRunnerCreationFixture(
		t,
		repository,
		runnerTestAllocationConfig(),
		desired,
		256,
		"runner-strict-boundaries-01",
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.publish(t, repository)
	current, err := repository.GetRunner(ctx, desired.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, containerID := range []string{
		"", strings.Repeat("a", testrunners.RunnerContainerIDEncodedLength-1),
		strings.Repeat("A", testrunners.RunnerContainerIDEncodedLength), strings.Repeat("g", testrunners.RunnerContainerIDEncodedLength),
	} {
		if _, err := testrunners.BindRunnerContainerID(current.Record, fixture.task.ID, containerID); err == nil {
			t.Fatalf("invalid container id passed: %q", containerID)
		}
	}
	exhausted := current.Record
	exhausted.RuntimeEpoch = ^uint64(0)
	if _, err := testrunners.BindRunnerContainerID(
		exhausted, fixture.task.ID, strings.Repeat("a", testrunners.RunnerContainerIDEncodedLength),
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
		DaemonSocketEndpoint: "unix:///run/groundplane-runners/" + runnerID + "/xdg/docker.sock",
		DaemonInstanceNonce:  strings.Repeat("a", 64), SocketDevice: 123, SocketInode: 456,
		CreatedAt: runnerStorageTime().Add(time.Second),
	}
}
