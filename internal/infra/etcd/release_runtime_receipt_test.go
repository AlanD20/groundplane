package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func terminalRuntimeFixture(
	t *testing.T,
	environmentID, serviceID, releaseID, planID, artifactID string,
) executionplan.CandidateRuntime {
	t.Helper()
	config := []byte(`{"apps":{"http":{"servers":{"gp_g1_` + strings.ToLower(releaseID) + `_p8080":{}}}}}`)
	configHash := sha256.Sum256(config)
	yaml := []byte("services:\n  api-blue:\n    image: sha256:" + strings.Repeat("a", 64) +
		"\n    networks: [current-backing]\nnetworks:\n  current-backing:\n    external: true\n")
	yamlHash := sha256.Sum256(yaml)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID),
		CanonicalYaml: yaml, YamlSha256: yamlHash[:],
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ComposeName: "api-blue", Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			Slot: "blue", ExpectedReplicas: 1, HasHealthcheck: true, ImageReference: "sha256:" + strings.Repeat("a", 64),
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.plan-id", Value: planID},
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.runtime-role", Value: "slot"},
				{Key: "com.groundplane.slot", Value: "blue"},
			},
		}, {
			ServiceId: serviceID, ComposeName: "api-proxy", Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ProxyConfigJson: config, ProxyConfigSha256: configHash[:],
			ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}},
		}},
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	return executionplan.CandidateRuntime{
		ServiceID: serviceID, ReleaseID: releaseID, Target: string(domain.WorkloadBlue),
		ProxyGeneration: 1, ProxyConfigSHA256: configHash[:], CurrentArtifact: encoded,
	}
}

// Rationale: preparation alone must not become applied authority. Exact success
// selects only completed members; failed/skipped/unknown members preserve bytes.
func TestReleaseRuntimeReceiptPromotesOnlySuccessfulMembers(t *testing.T) {
	t.Parallel()
	f := newReleaseTerminalFixture(t, 3)
	ctx := context.Background()
	prior := []byte("existing acknowledged runtime retained byte-for-byte")
	for _, member := range f.head.Members {
		key := serviceruntimerecord.Key(member.ServiceID)
		before, err := f.store.Get(ctx, key)
		if err != nil || before.Entry != nil {
			t.Fatal("prepared publication incorrectly promoted runtime")
		}
		if _, err := f.store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: key, Value: prior}}); err != nil {
			t.Fatal(err)
		}
	}
	f.result.FailedStepID = f.task.Steps[5].ID
	f.result.Diagnostic = testtaskjournal.TaskResultDiagnosticTimeoutBeforeEffect
	if processed, err := f.finalize(t, testtaskjournal.TaskStatusFailed); err != nil || !processed {
		t.Fatalf("selected terminal batch=%v, %v", processed, err)
	}
	for index, member := range f.head.Members {
		got, err := f.store.Get(ctx, serviceruntimerecord.Key(member.ServiceID))
		if err != nil || got.Entry == nil {
			t.Fatal("runtime disappeared")
		}
		if index > 0 && !bytes.Equal(got.Entry.Value, prior) {
			t.Fatal("failed or skipped member changed acknowledged runtime")
		}
		if index == 0 && bytes.Equal(got.Entry.Value, prior) {
			t.Fatal("successful selected member did not advance runtime")
		}
	}
}

// Rationale: wrong activation/epoch/source proof must fail before any member
// checkpoint or runtime is written, not acknowledge a differently sealed state.
func TestReleaseRuntimeReceiptRejectsMismatchedAuthority(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*releaseTerminalFixture){
		"proxy generation": func(f *releaseTerminalFixture) { f.result.ProxyEvidence[0].ProxyGeneration++ },
		"proxy digest":     func(f *releaseTerminalFixture) { f.result.ProxyEvidence[0].ConfigSHA256 = strings.Repeat("f", 64) },
		"plan hash":        func(f *releaseTerminalFixture) { f.task.PlanHash = strings.Repeat("f", 64) },
		"epoch":            func(f *releaseTerminalFixture) { f.result.ExecutionEpoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			f := newReleaseTerminalFixture(t, 2)
			mutate(&f)
			if _, err := f.finalize(t, testtaskjournal.TaskStatusCompleted); !errors.Is(
				err,
				errs.New(errs.KindStateConflict, ""),
			) {
				t.Fatalf("mismatched authority error=%v", err)
			}
			for _, member := range f.head.Members {
				for _, key := range []string{serviceruntimerecord.Key(member.ServiceID), testreleases.ReleaseTerminalKey(member.ReleaseID)} {
					got, err := f.store.Get(context.Background(), key)
					if err != nil || got.Entry != nil {
						t.Fatal("rejected acknowledgement partially wrote terminal state")
					}
				}
			}
		})
	}
}

// Rationale: current-runtime CAS and the caller's proof conditions join the
// member transaction, so neither a lost runtime race nor stale proof promotes it.
func TestReleaseRuntimeReceiptCASAndProofGuards(t *testing.T) {
	t.Parallel()
	f := newReleaseTerminalFixture(t, 2)
	f.store.raceKey = serviceruntimerecord.Key(f.head.Members[0].ServiceID)
	if _, err := f.finalize(t, testtaskjournal.TaskStatusCompleted); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("runtime CAS race error=%v", err)
	}
	for _, member := range f.head.Members {
		terminal, err := f.store.Get(context.Background(), testreleases.ReleaseTerminalKey(member.ReleaseID))
		if err != nil || terminal.Entry != nil {
			t.Fatal("lost runtime CAS partially wrote terminal history")
		}
	}
	if _, err := f.finalize(t, testtaskjournal.TaskStatusCompleted, testkeyvalue.Condition{Key: "/proof/not-present", ModRevision: 1}); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("stale proof error=%v", err)
	}
}

// Rationale: serialized physical key bytes can bind before operation count.
// Each restart resumes from committed members without dropping a receipt.
func TestReleaseRuntimeReceiptBatchesPhysicalBytesAndProofConditions(t *testing.T) {
	t.Parallel()
	f := newReleaseTerminalFixture(t, domain.MaximumGroupMembers)
	f.store.keyPrefix = "/" + strings.Repeat("p", 10000) + "/"
	guards := []testkeyvalue.Condition{{Key: "/proof/one"}, {Key: "/proof/two"}}
	for attempts := 0; ; attempts++ {
		processed, err := f.finalize(t, testtaskjournal.TaskStatusCompleted, guards...)
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
		if attempts > domain.MaximumGroupMembers {
			t.Fatal("byte-bounded terminal cursor did not finish")
		}
	}
	if f.store.maximumAggregate >= 92 || f.store.maximumBytes > testkeyvalue.MaximumBytes {
		t.Fatalf("batch bounds operations=%d bytes=%d", f.store.maximumAggregate, f.store.maximumBytes)
	}
}

// Rationale: successful compensation preserves the predecessor's receipt;
// completed recovery steps do not acknowledge the failed candidate's inputs.
func TestReleaseRuntimeReceiptPreservesCompensatedMember(t *testing.T) {
	t.Parallel()
	f := newReleaseTerminalFixture(t, 2)
	member := f.head.Members[0]
	priorRelease := ids.New(ids.KindDeployment)
	key := testreleases.ReleaseIntentStagingKey(f.head.PublicationID, member.ReleaseID)
	stored, err := f.store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := testreleases.DecodeReleaseRecord[domain.Intent](stored.Entry.Value, "release-intent")
	if err != nil {
		t.Fatal(err)
	}
	intent.PriorServingReleaseID, intent.PriorSuccessfulReleaseID = priorRelease, priorRelease
	encoded, err := testreleases.EncodeReleaseRecord("release-intent", intent)
	if err != nil {
		t.Fatal(err)
	}
	priorReceipt := []byte("pre-operation acknowledged input")
	if _, err := f.store.memoryHierarchyStore.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: key, Value: encoded},
		{Type: testkeyvalue.MutationPut, Key: serviceruntimerecord.Key(member.ServiceID), Value: priorReceipt},
	}); err != nil {
		t.Fatal(err)
	}
	for index := range f.result.ProxyEvidence {
		if f.result.ProxyEvidence[index].ServiceID == member.ServiceID {
			f.result.ProxyEvidence[index].ReleaseID = priorRelease
			f.result.ProxyEvidence[index].Compensated = true
		}
	}
	f.result.FailedStepID = f.task.Steps[0].ID
	if processed, err := f.finalize(t, testtaskjournal.TaskStatusFailed); err != nil || !processed {
		t.Fatalf("compensation terminal=%v, %v", processed, err)
	}
	got, err := f.store.Get(context.Background(), serviceruntimerecord.Key(member.ServiceID))
	if err != nil || got.Entry == nil || !bytes.Equal(got.Entry.Value, priorReceipt) {
		t.Fatal("compensation replaced captured predecessor runtime")
	}
}

func runtimeFixtureHash(value executionplan.CandidateRuntime) string {
	return hex.EncodeToString(value.ProxyConfigSHA256)
}
