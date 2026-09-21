package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: native HTTP recreate restores a replicated singleton through its
// sealed recreate probe, even when that same artifact includes a stable proxy.
func TestNativeRecreateRecoveryProofWithStableProxy(t *testing.T) {
	artifact := &agentpb.ComposeArtifact{ArtifactId: "prior", Services: []*agentpb.ComposeService{
		{ServiceId: "service", Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ExpectedReplicas: 2, ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.release-id", Value: "release"}}},
		{ServiceId: "service", Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY},
	}}
	encoded, err := proto.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	assignment := testtaskassignments.TaskAssignmentRecord{
		RestorationAuthority: &testtaskassignments.ReleaseRestorationAuthority{
			Candidates: []testtaskassignments.ReleaseRestorationCandidate{
				{ServiceID: "service", Target: testtaskassignments.ReleaseRestorationServingPredecessor},
			},
			AppliedPredecessor: &testtaskassignments.ReleaseAppliedPredecessorAuthority{ComposeArtifact: encoded},
		},
	}
	result := testtaskjournal.TaskResultRecord{RecreateEvidence: []testtaskjournal.TaskRecreateEvidence{{
		ServiceID: "service", ArtifactID: "prior", ReleaseID: "release", Target: "singleton", Compensated: true,
	}}}
	if err := validateReleaseRecoveryProof(assignment, nil, result, []releaseRecoveryProofExpectation{{kind: releaseRecoveryProofRecreate, priorTopologyArtifactID: "prior"}}); err != nil {
		t.Fatalf("exact native recreate recovery proof rejected: %v", err)
	}
}

// Rationale: a failure must identify a contradictory proxy generation or digest
// without dumping secret-bearing artifact bytes or weakening exact validation.
func TestRecoveryProxyMismatchIdentifiesFailedCheck(t *testing.T) {
	for _, field := range []string{"generation", "configuration digest"} {
		t.Run(field, func(t *testing.T) {
			fixture, assignment, result, revision, _ := recoveryProofFixture(t, true)
			if field == "generation" {
				result.ProxyEvidence[0].ProxyGeneration++
			} else {
				result.ProxyEvidence[0].ConfigSHA256 = strings.Repeat("f", 64)
			}
			_, err := fixture.repository.releaseRecoveryAcknowledgementAtRevision(t.Context(),
				fixture.claim.Task.Record, assignment, testtaskjournal.TaskStatusCompleted, result, revision)
			if err == nil || !strings.Contains(err.Error(), "proxy "+field+" differs from native predecessor") {
				t.Fatalf("mismatch did not identify %s: %v", field, err)
			}
		})
	}
}

// Rationale: repository final acknowledgement must select exactly the immutable
// ordinary render strategy, and source replacement or pruning must lose its CAS.
func TestOrdinaryRecoveryProofSelectionAndSourceCAS(t *testing.T) {
	for _, variation := range []string{"exact", "bluegreen", "bluegreen-recreate", "proxy-only", "mixed", "artifact", "captured-artifact", "foreign-artifact", "release", "target", "render-digest", "replace", "prune"} {
		t.Run(variation, func(t *testing.T) {
			fixture, assignment, result, revision, renderKey := recoveryProofFixture(
				t,
				strings.HasPrefix(variation, "bluegreen"),
			)
			ctx := context.Background()
			switch variation {
			case "bluegreen-recreate":
				result.ProxyEvidence = nil
				result.RecreateEvidence = []testtaskjournal.TaskRecreateEvidence{{ServiceID: fixture.serviceID}}
			case "proxy-only":
				result.RecreateEvidence = nil
				result.ProxyEvidence = []testtaskjournal.TaskProxyEvidence{{ServiceID: fixture.serviceID}}
			case "mixed":
				result.ProxyEvidence = []testtaskjournal.TaskProxyEvidence{{ServiceID: fixture.serviceID}}
			case "artifact":
				result.RecreateEvidence[0].ArtifactID = fixture.artifactID
			case "captured-artifact":
				captured := &agentpb.ComposeArtifact{}
				if err := proto.Unmarshal(assignment.RestorationAuthority.AppliedPredecessor.ComposeArtifact, captured); err != nil {
					t.Fatal(err)
				}
				result.RecreateEvidence[0].ArtifactID = captured.ArtifactId
			case "foreign-artifact":
				result.RecreateEvidence[0].ArtifactID = ids.NewAt(ids.KindConfig, fixture.now, 209)
			case "release":
				result.RecreateEvidence[0].ReleaseID = fixture.releaseID
			case "target":
				result.RecreateEvidence[0].Target = "blue"
			case "render-digest":
				read, err := fixture.repository.store.GetMany(
					ctx, testkeyvalue.GetManyRequest{Keys: []string{renderKey}, Revision: revision},
				)
				if err != nil {
					t.Fatal(err)
				}
				changed := []byte(
					strings.Replace(string(read.Values[0].Value), `"replica_count":3`, `"replica_count":4`, 1),
				)
				if string(changed) == string(read.Values[0].Value) {
					t.Fatal("fixture mutation did not change render")
				}
				txn, err := fixture.repository.store.Transact(
					ctx,
					nil,
					[]testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: renderKey, Value: changed}},
				)
				if err != nil {
					t.Fatal(err)
				}
				revision = txn.Revision
			}
			ack, err := fixture.repository.releaseRecoveryAcknowledgementAtRevision(
				ctx, fixture.claim.Task.Record, assignment, testtaskjournal.TaskStatusCompleted, result, revision,
			)
			valid := variation == "exact" || variation == "bluegreen" || variation == "replace" || variation == "prune"
			if !valid {
				if err == nil {
					t.Fatal("invalid proof or render authority accepted")
				}
				return
			}
			if err != nil || !ack.final {
				t.Fatalf("exact recovery acknowledgement rejected: %v", err)
			}
			if len(ack.conditions) != 6 {
				t.Fatalf("source/recovery guards = %d", len(ack.conditions))
			}
			if variation == "replace" || variation == "prune" {
				read, err := fixture.repository.store.GetMany(
					ctx, testkeyvalue.GetManyRequest{Keys: []string{renderKey}, Revision: revision},
				)
				if err != nil {
					t.Fatal(err)
				}
				mutation := testkeyvalue.Mutation{
					Type:  testkeyvalue.MutationPut,
					Key:   renderKey,
					Value: read.Values[0].Value,
				}
				if variation == "prune" {
					mutation.Type = testkeyvalue.MutationDelete
				}
				if _, err := fixture.repository.store.Transact(ctx, nil, []testkeyvalue.Mutation{mutation}); err != nil {
					t.Fatal(err)
				}
			}
			txn, err := fixture.repository.store.Transact(ctx, ack.conditions, nil)
			if err != nil || txn.Succeeded != (variation == "exact" || variation == "bluegreen") {
				t.Fatalf("source CAS = %t, %v", txn.Succeeded, err)
			}
		})
	}
}

func recoveryProofFixture(
	t *testing.T,
	blueGreen bool,
) (ordinaryReleaseClaimFixture, testtaskassignments.TaskAssignmentRecord, testtaskjournal.TaskResultRecord, int64, string) {
	t.Helper()
	f := newOrdinaryReleaseClaimFixtureForTarget(t, true)
	task, assignment := f.claim.Task.Record, f.claim.Assignment.Record
	priorRelease := ids.NewAt(ids.KindDeployment, f.now, 200)
	priorArtifact := ids.NewAt(ids.KindConfig, f.now, 201)
	priorTopologyArtifact := ids.NewAt(ids.KindConfig, f.now, 203)
	config := []byte(`{"apps":{"http":{"servers":{"gp_g1_` + strings.ToLower(priorRelease) + `_p":{}}}}}`)
	configDigest := sha256.Sum256(config)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:  priorArtifact,
		ProjectName: "gp-" + strings.ToLower(task.Owner.EnvironmentID),
		OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:     task.Owner.EnvironmentID,
		Services: []*agentpb.ComposeService{
			{
				ServiceId:        f.serviceID,
				ComposeName:      "worker",
				ImageReference:   releaseTestPriorWorkload("registry.example/worker:prior").LocalImageID,
				ExpectedReplicas: 2,
				HasHealthcheck:   true,
				Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.release-id", Value: priorRelease},
					{Key: "com.groundplane.runtime-role", Value: "singleton"},
				},
			},
			{
				ServiceId:         f.serviceID,
				ComposeName:       "proxy",
				Role:              agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
				ProxyConfigJson:   config,
				ProxyConfigSha256: configDigest[:],
				ExpectedLabels:    []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}},
			},
		},
	}
	if blueGreen {
		artifact.Services[0].ExpectedReplicas = 1
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	authority := assignment.RestorationAuthority
	authority.Candidates[0].Target = testtaskassignments.ReleaseRestorationServingPredecessor
	authority.AppliedPredecessor = &testtaskassignments.ReleaseAppliedPredecessorAuthority{
		KeyRevision:           1,
		RevisionID:            ids.NewAt(ids.KindTask, f.now, 202),
		RenderGeneration:      1,
		ComposeArtifact:       encoded,
		ComposeArtifactSHA256: hex.EncodeToString(digest[:]),
	}
	native := proto.CloneOf(artifact)
	native.ArtifactId = priorTopologyArtifact
	nativeBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	authority.NativePredecessors = []testtaskassignments.ReleaseNativePredecessorAuthority{{
		ServiceID: f.serviceID, CurrentArtifact: nativeBytes,
	}}
	assignment.RestorationAuthoritySHA256, err = testtaskassignments.ReleaseRestorationAuthoritySHA256(*authority)
	if err != nil {
		t.Fatal(err)
	}
	render := portlessReleaseRenderInput(domain.StrategyRecreate)
	render.ReleaseID, render.PlanID, render.ArtifactID = f.releaseID, task.PlanID, f.artifactID
	render.ServiceID, render.EnvironmentID = f.serviceID, task.Owner.EnvironmentID
	render.Projection.EnvironmentID = task.Owner.EnvironmentID
	render.Projection.DesiredServices[0].EnvironmentID = task.Owner.EnvironmentID
	render.Projection.DesiredServices[0].Desired.ID = f.serviceID
	// Ordinary publication compiles a fresh prior topology from a newer desired
	// projection. Neither its artifact ID nor revision is the captured serving one.
	render.Projection.RevisionID = ids.NewAt(ids.KindTask, f.now, 204)
	render.Projection.RenderGeneration = 5
	render.Projection = withTestEnvironmentComposeArtifact(render.Projection)
	render.PriorArtifactID, render.PriorWorkload = priorTopologyArtifact, releaseTestPriorWorkload(
		"registry.example/worker:prior",
	)
	render.PriorRuntime = &authority.NativePredecessors[0]
	render.CandidateWorkload.ReplicaCount, render.PriorWorkload.ReplicaCount = 3, 2
	render.ProxyPorts, render.ProxyImage = []uint16{8080}, addressableReleaseProxyFixture().ProxyImage
	render.ProxyGeneration, render.PriorProxyGeneration = 2, 1
	render.ProxyConfigDigest, render.PriorProxyDigest = strings.Repeat("a", 64), hex.EncodeToString(configDigest[:])
	if blueGreen {
		render.Strategy, render.Slot, render.CandidateTarget = domain.StrategyBlueGreen, domain.SlotBlue, domain.WorkloadBlue
		render.CandidateWorkload.ReplicaCount, render.PriorWorkload.ReplicaCount = 1, 1
	}
	raw, err := testreleaserender.EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatal(err)
	}
	renderDigest, err := domain.Digest(raw)
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.Intent{
		ID:                    f.releaseID,
		EnvironmentID:         task.Owner.EnvironmentID,
		ServiceID:             f.serviceID,
		OperationID:           task.OperationID,
		OperationKind:         domain.OperationDeploy,
		CandidateWorkload:     render.CandidateWorkload,
		Tag:                   "stable",
		Strategy:              domain.StrategyRecreate,
		OnFailure:             domain.OnFailureSwitchBack,
		RenderInputID:         f.artifactID,
		RenderInputDigest:     renderDigest,
		PriorServingReleaseID: priorRelease,
		CreatedAt:             f.now,
		Actor:                 "operator",
		OriginatingTaskID:     task.ID,
		Workspace: domain.Workspace{
			Kind:          domain.WorkspaceTenant,
			TenantID:      task.Owner.TenantID,
			ProjectID:     task.Owner.ProjectID,
			EnvironmentID: task.Owner.EnvironmentID,
		},
	}
	intent.Strategy, intent.Slot = render.Strategy, render.Slot
	intentDigest, err := domain.Digest(intent)
	if err != nil {
		t.Fatal(err)
	}
	publication := task.Params[testreleaserender.TaskReleasePublicationParam]
	manifest := testreleases.ReleaseStagedManifest{
		PublicationID: publication,
		OperationID:   task.OperationID,
		CreatedAt:     f.now,
		Members: []testreleases.ReleaseStagedMemberRef{
			{
				ServiceID:        f.serviceID,
				ReleaseID:        f.releaseID,
				IntentDigest:     intentDigest,
				RenderDigest:     renderDigest,
				CheckpointDigest: strings.Repeat("d", 64),
			},
		},
	}
	manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	procedure := &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{ServiceId: f.serviceID,
		CandidateReleaseId: f.releaseID, CandidateArtifactId: f.artifactID, ForwardStepIds: []string{f.forwardStepID},
		ServingPredecessor: &agentpb.ServingPredecessorRestoration{
			ProbeStepId:      task.Steps[1].ID,
			CompensateStepId: task.Steps[2].ID,
			PriorArtifactId:  priorTopologyArtifact, PriorReleaseId: priorRelease, PriorTarget: "singleton",
		}}}}
	procedureBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(procedure)
	if err != nil {
		t.Fatal(err)
	}
	planHash, err := hex.DecodeString(task.PlanHash)
	if err != nil {
		t.Fatal(err)
	}
	marker := testreleases.ReleasePublicationMarker{
		PublicationID:  publication,
		OperationID:    task.OperationID,
		ManifestDigest: manifest.Digest,
		PublishedAt:    f.now,
		CandidateReleaseDescriptor: executionplan.CandidateReleaseDescriptor{
			PlanID:         task.PlanID,
			PlanHash:       planHash,
			Operation:      agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
			ProcedureBytes: procedureBytes,
		},
	}
	primary := testtaskjournal.TaskResultRecord{ExecutionEpoch: 1, ReconciliationRequired: true}
	primaryDigest, err := testtaskassignments.CanonicalPrimaryReportSHA256(testtaskjournal.TaskStatusFailed, primary)
	if err != nil {
		t.Fatal(err)
	}
	recovery := testtaskassignments.ReleaseRecoveryRecord{
		Schema:                     1,
		TaskID:                     task.ID,
		AssignmentID:               assignment.AssignmentID,
		OperationID:                task.OperationID,
		PlanHash:                   task.PlanHash,
		RestorationAuthoritySHA256: assignment.RestorationAuthoritySHA256,
		PrimaryReportSHA256:        primaryDigest,
		PrimaryStatus:              testtaskjournal.TaskStatusFailed,
		PrimaryResult:              primary,
		RecoveryDeadline:           assignment.RecoveryDeadline,
		RecoveryStepIDs: []string{
			task.Steps[1].ID,
			task.Steps[2].ID,
		},
		Cursor:           2,
		Phase:            testtaskassignments.ReleaseRecoveryPhaseProven,
		EvidenceRevision: 1,
	}
	assignment.ExecutionMode, assignment.ExecutionEpoch = testtaskassignments.TaskExecutionModeRecoveryOnly, 2
	assignment.ReleaseRecoveryRecordSHA256, err = testtaskassignments.ReleaseRecoveryRecordSHA256(recovery)
	if err != nil {
		t.Fatal(err)
	}
	markerBytes, err := testreleases.EncodeReleaseRecord("release-publication", marker)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := testreleases.EncodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		t.Fatal(err)
	}
	intentBytes, err := testreleases.EncodeReleaseRecord("release-intent", intent)
	if err != nil {
		t.Fatal(err)
	}
	renderBytes, err := testreleases.EncodeReleaseRecord("release-render-input", raw)
	if err != nil {
		t.Fatal(err)
	}
	recoveryBytes, err := testtaskassignments.EncodeReleaseRecoveryRecord(recovery)
	if err != nil {
		t.Fatal(err)
	}
	renderKey := testreleases.ReleaseRenderInputStagingKey(publication, f.releaseID)
	head := testreleases.ReleaseOperationHead{
		OperationID:   task.OperationID,
		PublicationID: publication,
		EnvironmentID: task.Owner.EnvironmentID,
		State:         domain.StatePending,
		FailurePolicy: domain.OnFailureSwitchBack,
		LatestTaskID:  task.ID,
		Attempts:      []domain.Attempt{{ID: task.ID, TaskID: task.ID, StartedAt: f.now}},
		Members: []domain.GroupMember{
			{Ordinal: 1, ServiceID: f.serviceID, ReleaseID: f.releaseID},
		},
		CreatedAt: f.now,
		UpdatedAt: f.now,
	}
	headBytes, err := testreleases.EncodeReleaseRecord("release-operation", head)
	if err != nil {
		t.Fatal(err)
	}
	txn, err := f.repository.store.Transact(context.Background(), nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testreleases.ReleaseOperationKey(task.OperationID), Value: headBytes},
		{Type: testkeyvalue.MutationPut, Key: testreleases.ReleasePublicationKey(publication), Value: markerBytes},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseManifestStagingKey(publication),
			Value: manifestBytes,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseIntentStagingKey(publication, f.releaseID),
			Value: intentBytes,
		},
		{Type: testkeyvalue.MutationPut, Key: renderKey, Value: renderBytes},
		{Type: testkeyvalue.MutationPut, Key: testtaskassignments.ReleaseRecoveryKey(task.ID), Value: recoveryBytes},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := testtaskjournal.TaskResultRecord{
		ExecutionEpoch:              2,
		ReleaseRecoveryRecordSHA256: assignment.ReleaseRecoveryRecordSHA256,
		RecreateEvidence: []testtaskjournal.TaskRecreateEvidence{
			{
				ServiceID:   f.serviceID,
				ReleaseID:   priorRelease,
				ArtifactID:  priorTopologyArtifact,
				Target:      "singleton",
				Compensated: true,
			},
		},
	}
	if blueGreen {
		result.RecreateEvidence = nil
		result.ProxyEvidence = []testtaskjournal.TaskProxyEvidence{{ServiceID: f.serviceID, ReleaseID: priorRelease,
			Target: "singleton", Compensated: true, ConfigSHA256: hex.EncodeToString(configDigest[:]), ProxyGeneration: 1}}
	}
	return f, assignment, result, txn.Revision, renderKey
}
