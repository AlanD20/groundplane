package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestBlueprintFinalRecoveryAcknowledgementIsAtomicReplayableAndCleansAuthority(t *testing.T) {
	blueprintFinalRecoveryAcknowledgement(t, false, false)
}

func TestBlueprintComponentRunningPreventsWorkloadOnlyRecoveryClosure(t *testing.T) {
	blueprintFinalRecoveryAcknowledgement(t, true, true)
}

func TestBlueprintUnstartedComponentAllowsWorkloadRecovery(t *testing.T) {
	blueprintFinalRecoveryAcknowledgement(t, true, false)
}

func blueprintFinalRecoveryAcknowledgement(t *testing.T, componentDeclared, componentRunning bool) {
	ctx := context.Background()
	shape := environmentBlueprintAtomicShape{
		name: "final recovery acknowledgement", releases: 1, hooks: 1, physicalSources: 1,
	}
	published, err := publishEnvironmentBlueprintAtomicShape(t, shape, false)
	if err != nil {
		t.Fatal(err)
	}
	outcome, _, conflict, err := published.result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("Blueprint publication = %v/%v/%v", outcome, conflict, err)
	}
	repository, err := newTaskRepository(published.store)
	if err != nil {
		t.Fatal(err)
	}
	repository.blueprintTerminalStore = published.store
	task := published.task
	publicationID := published.releasePublicationID
	read, err := published.store.GetMany(ctx, GetManyRequest{Keys: []string{
		releasePublicationKey(publicationID), releaseManifestStagingKey(publicationID),
		environmentMutationEpochKey(published.environmentID),
	}})
	if err != nil || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		t.Fatalf("read Blueprint Release authority = %#v, %v", read, err)
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](read.Values[0].Value, "release-publication")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](read.Values[1].Value, "release-staged-manifest")
	if err != nil || len(manifest.Members) != 1 {
		t.Fatalf("decode staged manifest = %#v, %v", manifest, err)
	}
	member := manifest.Members[0]
	projection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID: published.environmentID, RevisionID: task.ID, RenderGeneration: uint64(task.RenderGeneration),
		DesiredServices: []EnvironmentServiceProjection{{
			EnvironmentID: published.environmentID,
			Desired: core.Service{
				ID:       member.ServiceID,
				Name:     "api",
				Image:    "example/api:1",
				Strategy: core.StrategyRecreate,
			},
		}},
		ServiceDependencyPlans: core.ServiceDependencyPlans{},
	})
	render := ReleaseRenderInput{
		ReleaseID: member.ReleaseID, PlanID: task.PlanID, ArtifactID: task.Params[TaskComposeArtifactParam],
		ServiceID: member.ServiceID, ServiceName: "api", CandidateWorkload: releaseTestWorkloadSeal("example/api:1"),
		Strategy: domain.StrategyRecreate, CandidateTarget: domain.WorkloadSingleton,
		PriorArtifactID: ids.NewAt(
			ids.KindConfig,
			task.CreatedAt,
			19991,
		), PriorWorkload: releaseTestPriorWorkload("example/api:previous"),
		PriorStrategy: domain.StrategyRecreate, PriorTarget: domain.WorkloadSingleton,
		TenantID: task.Owner.TenantID, TenantSlug: "tenant", ProjectID: task.Owner.ProjectID, ProjectSlug: "project",
		EnvironmentID: published.environmentID, EnvironmentName: "production",
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes", Projection: projection,
		ServiceDependencyPlans: core.ServiceDependencyPlans{},
	}
	rawRender, err := EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatal(err)
	}
	renderDigest, err := domain.Digest(rawRender)
	if err != nil {
		t.Fatal(err)
	}
	intent := domain.Intent{
		ID: member.ReleaseID, EnvironmentID: published.environmentID, ServiceID: member.ServiceID,
		OperationID: task.OperationID, OperationKind: domain.OperationBlueprintApply,
		GroupOperationID: task.OperationID, GroupMemberOrdinal: 1,
		CandidateWorkload: render.CandidateWorkload, Tag: "stable", Strategy: domain.StrategyRecreate,
		OnFailure: domain.OnFailureSwitchBack, RenderInputID: render.ArtifactID, RenderInputDigest: renderDigest,
		CreatedAt: task.CreatedAt, Actor: "operator", OriginatingTaskID: task.ID,
		Workspace: domain.Workspace{
			Kind: domain.WorkspaceTenant, TenantID: task.Owner.TenantID,
			ProjectID: task.Owner.ProjectID, EnvironmentID: published.environmentID,
		},
	}
	if err := domain.ValidateIntent(intent); err != nil {
		t.Fatal(err)
	}
	checkpoint := domain.Checkpoint{ReleaseID: member.ReleaseID, State: domain.StatePending, UpdatedAt: task.CreatedAt}
	manifest.Members[0].IntentDigest, err = domain.Digest(intent)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Members[0].RenderDigest = renderDigest
	manifest.Members[0].CheckpointDigest, err = domain.Digest(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	marker.ManifestDigest = manifest.Digest
	values := make(map[string][]byte)
	componentStepID := ids.NewAt(ids.KindStep, task.CreatedAt, 19995)
	if componentDeclared {
		stored, readErr := published.store.Get(ctx, taskKey(task.ID))
		if readErr != nil || stored.Entry == nil {
			t.Fatalf("read published Task: %v", readErr)
		}
		task, err = decodeTaskRecord(stored.Entry.Value)
		if err != nil {
			t.Fatal(err)
		}
		root, readErr := published.store.Get(ctx, scriptSourceRootKey(task.OperationID))
		if readErr != nil || root.Entry == nil {
			t.Fatalf("read Script source root: %v", readErr)
		}
		values[scriptSourceRootKey(task.OperationID)] = root.Entry.Value
		task.ComponentActionStepIDs = []string{componentStepID}
		task.Steps = append(task.Steps, TaskStepRecord{Kind: TaskStepOperation, ID: componentStepID})
		marker.CandidateReleaseDescriptor.ComponentActionStepIDs = []string{componentStepID}
		values[taskKey(task.ID)], err = encodeTaskRecord(task)
		if err != nil {
			t.Fatal(err)
		}
	}
	for key, record := range map[string]struct {
		typeName string
		value    any
	}{
		releasePublicationKey(publicationID):                          {"release-publication", marker},
		releaseManifestStagingKey(publicationID):                      {"release-staged-manifest", manifest},
		releaseIntentStagingKey(publicationID, member.ReleaseID):      {"release-intent", intent},
		releaseRenderInputStagingKey(publicationID, member.ReleaseID): {"release-render-input", json.RawMessage(rawRender)},
		releaseCheckpointStagingKey(publicationID, member.ReleaseID):  {"release-checkpoint", checkpoint},
	} {
		values[key], err = encodeReleaseRecord(record.typeName, record.value)
		if err != nil {
			t.Fatal(err)
		}
	}
	mutations := make([]Mutation, 0, len(values))
	for key, value := range values {
		mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: value})
	}
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: environmentMutationEpochKey(published.environmentID), Value: read.Values[2].Value,
	})
	updated, err := published.store.Transact(ctx, nil, mutations)
	if err != nil || !updated.Succeeded {
		t.Fatalf("install exact candidate ledger = %#v, %v", updated, err)
	}
	agentID := ids.NewAt(ids.KindAgent, task.CreatedAt, 19990)
	if _, checkErr := validateReleaseCandidateDescriptor(marker.CandidateReleaseDescriptor, task, manifest); checkErr != nil {
		t.Fatalf(
			"fixture descriptor mismatch: %+v taskhash=%s descriptor=%+v",
			checkErr,
			task.PlanHash,
			marker.CandidateReleaseDescriptor,
		)
	}
	if componentDeclared {
		persisted, err := published.store.Get(ctx, taskKey(task.ID))
		if err != nil {
			t.Fatal(err)
		}
		actualTask, err := decodeTaskRecord(persisted.Entry.Value)
		if err != nil {
			t.Fatal(err)
		}
		persisted, err = published.store.Get(ctx, releasePublicationKey(publicationID))
		if err != nil {
			t.Fatal(err)
		}
		actualMarker, err := decodeReleaseRecord[ReleasePublicationMarker](persisted.Entry.Value, "release-publication")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validateReleaseCandidateDescriptor(actualMarker.CandidateReleaseDescriptor, actualTask, manifest); err != nil {
			t.Fatalf(
				"stored descriptor mismatch taskIDs=%v descriptorIDs=%v: %v",
				actualTask.ComponentActionStepIDs,
				actualMarker.CandidateReleaseDescriptor.ComponentActionStepIDs,
				err,
			)
		}
		omitted := cloneTaskRecord(actualTask)
		omitted.ComponentActionStepIDs = nil
		if _, err := validateReleaseCandidateDescriptor(actualMarker.CandidateReleaseDescriptor, omitted, manifest); err == nil {
			t.Fatal("accepted omitted Task Component authority")
		}
		tampered := executionplan.CloneCandidateReleaseDescriptor(actualMarker.CandidateReleaseDescriptor)
		tampered.ComponentActionStepIDs = nil
		if _, err := validateReleaseCandidateDescriptor(tampered, actualTask, manifest); err == nil {
			t.Fatal("accepted omitted descriptor Component authority")
		}
		tampered = executionplan.CloneCandidateReleaseDescriptor(actualMarker.CandidateReleaseDescriptor)
		tampered.PlanHash = bytes.Repeat([]byte{0xff}, 32)
		if _, err := validateReleaseCandidateDescriptor(tampered, actualTask, manifest); err == nil {
			t.Fatal("accepted Component authority from changed plan hash")
		}
	}
	claim, found, err := repository.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(2*time.Minute))
	if err != nil || !found || claim.Assignment.Record.RestorationAuthority == nil ||
		claim.Assignment.Record.RestorationAuthority.Candidates[0].Target != ReleaseRestorationCandidateAbsence {
		t.Fatalf("claim recovery fixture = %#v, %t, %v", claim, found, err)
	}
	procedure, err := executionplan.OpenCandidateReleaseDescriptor(marker.CandidateReleaseDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	primary := TaskResultRecord{
		Kind: TaskResultCompose, ExitCode: 17, FailedStepID: procedure.GetMembers()[0].GetForwardStepIds()[0],
		Diagnostic: TaskResultDiagnosticComposeFailed, ReconciliationRequired: true, ExecutionEpoch: 1,
	}
	if componentRunning {
		_, err = repository.AppendTaskEvent(
			ctx,
			TaskEventInput{
				Identity: TaskEventIdentity{
					AssignmentID:    claim.Assignment.Record.AssignmentID,
					AgentID:         agentID,
					AgentGeneration: 1,
					TaskID:          task.ID,
					StepID:          componentStepID,
					Attempt:         1,
					Ordinal:         1,
				},
				State:   TaskEventStateRunning,
				Payload: json.RawMessage(`{"message":"Component activation started"}`),
			},
			task.CreatedAt.Add(150*time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	transitioned, err := repository.AcknowledgeTask(
		ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID,
		TaskStatusFailed, primary, task.CreatedAt.Add(3*time.Minute),
	)
	if err != nil || transitioned.Record.Status != TaskStatusRunning || transitioned.Record.Result != nil {
		t.Fatalf("transition recovery fixture = %#v, %v", transitioned, err)
	}
	recovery, err := repository.GetTaskAssignment(ctx, task.ID)
	if err != nil || recovery.Assignment.Record.ExecutionMode != TaskExecutionModeRecoveryOnly ||
		recovery.Assignment.Record.ExecutionEpoch != 2 || recovery.ReleaseRecovery == nil {
		t.Fatalf("recovery assignment = %#v, %v", recovery, err)
	}
	ordinal := uint64(1)
	for _, stepID := range recovery.ReleaseRecovery.StepIDs {
		for _, state := range []TaskEventState{TaskEventStateRunning, TaskEventStateCompleted} {
			_, err = repository.AppendTaskEvent(ctx, TaskEventInput{
				Identity: TaskEventIdentity{
					AssignmentID: recovery.Assignment.Record.AssignmentID, AgentID: agentID, AgentGeneration: 1,
					TaskID: task.ID, StepID: stepID, Attempt: 2, Ordinal: ordinal,
				},
				State: state, Payload: json.RawMessage(`{"message":"recovery"}`),
			}, task.CreatedAt.Add(time.Duration(3+ordinal)*time.Minute))
			if err != nil {
				t.Fatalf("append recovery event %s/%s = %v", stepID, state, err)
			}
			ordinal++
		}
	}
	authority := recovery.Assignment.Record.RestorationAuthority
	final := TaskResultRecord{
		Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone, ExecutionEpoch: 2,
		ReleaseRecoveryRecordSHA256: recovery.Assignment.Record.ReleaseRecoveryRecordSHA256,
		CandidateAbsenceEvidence: &TaskCandidateAbsenceEvidence{
			AssignmentID: recovery.Assignment.Record.AssignmentID, PlanHash: authority.PlanHash,
			AuthoritySHA256:     recovery.Assignment.Record.RestorationAuthoritySHA256,
			ComposeProjectName:  procedure.GetMembers()[0].GetCandidateAbsence().GetComposeProjectName(),
			CandidateArtifactID: authority.CandidateArtifactID, AbsenceProven: true,
			Candidates: []TaskCandidateAbsenceCandidate{{ServiceID: member.ServiceID, ReleaseID: member.ReleaseID}},
		},
	}
	terminal, err := repository.AcknowledgeTask(
		ctx, agentID, 1, task.ID, recovery.Assignment.Record.AssignmentID,
		TaskStatusCompleted, final, task.CreatedAt.Add(10*time.Minute),
	)
	if componentRunning {
		if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf(
				"Component effect allowed workload-only terminal closure: task=%v error=%v",
				terminal.Record.Status,
				err,
			)
		}
		retained, readErr := repository.GetTaskAssignment(ctx, task.ID)
		if readErr != nil || retained.Assignment.Record.ExecutionMode != TaskExecutionModeRecoveryOnly {
			t.Fatalf("recovery ownership lost: %#v %v", retained, readErr)
		}
		return
	}
	if err != nil || terminal.Record.Status != TaskStatusFailed || terminal.Record.Result == nil ||
		terminal.Record.Result.ExitCode != primary.ExitCode || terminal.Record.Result.FailedStepID != primary.FailedStepID ||
		terminal.Record.Result.ReconciliationRequired {
		t.Fatalf("final recovery acknowledgement = %#v, %v", terminal, err)
	}
	cleanup, err := published.store.GetMany(ctx, GetManyRequest{Keys: []string{
		taskAssignmentKey(agentID, task.ID), taskAssignmentIndexKey(task.ID),
		taskActiveOperationKey(
			task.OperationID,
		), taskTimeoutIndexKey(task.ID, recovery.Assignment.Record.RecoveryDeadline),
		taskMaterializationWriterKey(published.environmentID), releaseRecoveryKey(task.ID),
		scriptSourceRootKey(task.OperationID), environmentComposeProjectionKey(published.environmentID),
	}})
	if err != nil {
		t.Fatal(err)
	}
	for index, value := range cleanup.Values {
		if value != nil {
			t.Fatalf("final recovery cleanup key %d retained: %#v", index, value)
		}
	}
	executionID := ""
	for _, step := range task.Steps {
		if selected := task.Params[ReleaseHookStepExecutionParam(step.ID)]; selected != "" {
			executionID = selected
		}
	}
	executionRead, err := published.store.Get(ctx, scriptExecutionKey(executionID))
	if err != nil || executionRead.Entry == nil {
		t.Fatalf("released Script execution = %#v, %v", executionRead, err)
	}
	execution, err := decodeEnvelope[ScriptExecutionRecord](executionRead.Entry.Value, "script-execution")
	if err != nil || !releaseRecoveryParentFailureExecutionMatches(execution) {
		t.Fatalf("parent-failure Script execution = %#v, %v", execution, err)
	}
	replay, err := repository.AcknowledgeTask(
		ctx, agentID, 1, task.ID, recovery.Assignment.Record.AssignmentID,
		TaskStatusCompleted, final, task.CreatedAt.Add(11*time.Minute),
	)
	if err != nil || replay.Revision != terminal.Revision || replay.Record.Status != terminal.Record.Status {
		t.Fatalf("exact final recovery replay = %#v, %v", replay, err)
	}
	changed := final
	changed.CandidateAbsenceEvidence = cloneTaskCandidateAbsenceEvidence(final.CandidateAbsenceEvidence)
	changed.CandidateAbsenceEvidence.AuthoritySHA256 = strings.Repeat("f", 64)
	if _, err := repository.AcknowledgeTask(
		ctx, agentID, 1, task.ID, recovery.Assignment.Record.AssignmentID,
		TaskStatusCompleted, changed, task.CreatedAt.Add(11*time.Minute),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("mismatched final recovery replay error = %v", err)
	}
}

// Rationale: recovery replay must reuse Task-result equality without weakening optional authority identity.
func TestReleaseRecoveryAbsenceEvidenceEqualityUsesCanonicalTaskResultSemantics(t *testing.T) {
	left := &TaskCandidateAbsenceEvidence{
		AssignmentID: "assignment", PlanHash: strings.Repeat("a", 64), AuthoritySHA256: strings.Repeat("b", 64),
		ComposeProjectName: "project", CandidateArtifactID: "artifact", AbsenceProven: true,
	}
	right := cloneTaskCandidateAbsenceEvidence(left)
	right.Candidates = []TaskCandidateAbsenceCandidate{}
	if !taskCandidateAbsenceEvidenceEqual(left, right) {
		t.Fatal("canonical absence evidence equality distinguished nil and empty candidates")
	}
	if taskCandidateAbsenceEvidenceEqual(nil, right) {
		t.Fatal("canonical absence evidence equality collapsed optional evidence presence")
	}
	right.AuthoritySHA256 = strings.Repeat("c", 64)
	if taskCandidateAbsenceEvidenceEqual(left, right) {
		t.Fatal("canonical absence evidence equality accepted changed authority")
	}
}

// Rationale: completed Blueprint replay rejects every terminal or retention identity drift, including expiry.
func TestBlueprintTerminalReplayTypedEqualityPreservesExactLedgerIdentity(t *testing.T) {
	if sameBlueprintAttachStrings(nil, []string{}) ||
		!sameBlueprintAttachStrings([]string{"first", "second"}, []string{"first", "second"}) ||
		sameBlueprintAttachStrings([]string{"first", "second"}, []string{"second", "first"}) {
		t.Fatal("terminal string equality did not preserve nil or ordered identity")
	}
	base := domain.RollbackMaterial{ReleaseID: "release", Status: domain.RetentionAvailable,
		References: []string{"image@sha256:digest"}, Digest: "digest", Revision: 7}
	tests := []struct {
		name   string
		mutate func(*domain.RollbackMaterial)
	}{
		{"release id", func(value *domain.RollbackMaterial) { value.ReleaseID = "changed" }},
		{"status", func(value *domain.RollbackMaterial) { value.Status = domain.RetentionExpired }},
		{"references", func(value *domain.RollbackMaterial) { value.References[0] = "changed" }},
		{"digest", func(value *domain.RollbackMaterial) { value.Digest = "changed" }},
		{"revision", func(value *domain.RollbackMaterial) { value.Revision++ }},
	}
	if !blueprintCompletedRollbackMaterialEqual(base, base) {
		t.Fatal("identical completed rollback material was unequal")
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			changed.References = slices.Clone(base.References)
			test.mutate(&changed)
			if blueprintCompletedRollbackMaterialEqual(base, changed) {
				t.Fatalf("rollback material equality accepted drifted %s", test.name)
			}
		})
	}
	emptyReferences := base
	emptyReferences.References = []string{}
	nilReferences := emptyReferences
	nilReferences.References = nil
	if blueprintCompletedRollbackMaterialEqual(emptyReferences, nilReferences) {
		t.Fatal("rollback material equality collapsed nil and empty references")
	}
	expiredUTC := time.Date(2026, time.September, 4, 12, 30, 0, 0, time.UTC)
	expiredFixedZone := expiredUTC.In(time.FixedZone("UTC", 0))
	withExpiry := func(expiredAt *time.Time) domain.RollbackMaterial {
		value := base
		value.ExpiredAt = expiredAt
		return value
	}
	for _, test := range []struct {
		name        string
		left, right domain.RollbackMaterial
	}{
		{"left expiry", withExpiry(&expiredUTC), base},
		{"right expiry", base, withExpiry(&expiredUTC)},
		{"both same expiry", withExpiry(&expiredUTC), withExpiry(&expiredUTC)},
		{"both separately allocated zones", withExpiry(&expiredUTC), withExpiry(&expiredFixedZone)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if blueprintCompletedRollbackMaterialEqual(test.left, test.right) {
				t.Fatal("completed rollback material equality accepted non-null expiry")
			}
		})
	}
}

func TestOrdinaryReleaseClaimCarriesDescriptorRestorationAuthority(t *testing.T) {
	fixture := newOrdinaryReleaseClaimFixture(t)
	authority := fixture.claim.Assignment.Record.RestorationAuthority
	if authority == nil || len(authority.Candidates) != 1 ||
		authority.Candidates[0].Target != ReleaseRestorationCandidateAbsence ||
		authority.CandidateArtifactID != fixture.artifactID ||
		authority.Candidates[0].ServiceID != fixture.serviceID ||
		authority.Candidates[0].ReleaseID != fixture.releaseID {
		t.Fatalf("ordinary Release claim authority = %#v", authority)
	}
}

// Rationale: Service-target releases have no materialization writer but need durable, reconnectable authority.
func TestOrdinaryServiceReleaseClaimWithoutWriterCarriesDurableAuthority(t *testing.T) {
	fixture := newOrdinaryReleaseClaimFixtureForTarget(t, true)
	ctx := context.Background()
	claim := fixture.claim
	authority := claim.Assignment.Record.RestorationAuthority
	if authority == nil {
		t.Fatal("Service-target release claim has no restoration authority")
	}
	digest, err := releaseRestorationAuthoritySHA256(*authority)
	if err != nil || digest != claim.Assignment.Record.RestorationAuthoritySHA256 {
		t.Fatalf("restoration authority digest mismatch: %v", err)
	}
	if claim.Task.Record.Target != fixture.serviceID ||
		claim.Task.Record.Params[TaskMaterializationEnvironmentParam] != "" {
		t.Fatal("fixture does not represent a Service-target release without a writer")
	}
	read, err := fixture.repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		taskAssignmentKey(fixture.agentID, claim.Task.Record.ID), taskAssignmentIndexKey(claim.Task.Record.ID),
		taskTimeoutIndexKey(claim.Task.Record.ID, claim.Assignment.Record.Deadline),
	}})
	if err != nil || len(read.Values) != 3 {
		t.Fatalf("read assignment copies: %v", err)
	}
	defer clearKeyValues(read.Values)
	encoded, err := encodeTaskAssignment(claim.Assignment.Record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(encoded)
	for _, value := range read.Values {
		if value == nil || value.ModRevision != claim.Assignment.Revision || !bytes.Equal(value.Value, encoded) {
			t.Fatal("assignment copies were not atomically published with restoration authority")
		}
	}
	current, err := fixture.repository.GetTaskAssignment(ctx, claim.Task.Record.ID)
	if err != nil || current.Assignment.Record.RestorationAuthoritySHA256 != digest {
		t.Fatalf("GetTaskAssignment rejected durable authority: %v", err)
	}
	listed, err := fixture.repository.ListAgentAssignments(ctx, fixture.agentID, 1, 1)
	if err != nil || len(listed) != 1 || listed[0].Assignment.Record.RestorationAuthoritySHA256 != digest {
		t.Fatalf("ListAgentAssignments rejected durable authority: %v", err)
	}
	reconnected, err := fixture.repository.ReconnectAgentAssignment(ctx, current)
	if err != nil || reconnected.Assignment.Record.ExecutionEpoch != 2 ||
		reconnected.Assignment.Record.RestorationAuthoritySHA256 != digest {
		t.Fatalf("reconnect did not preserve durable authority: %v", err)
	}
	listed, err = fixture.repository.ListAgentAssignments(ctx, fixture.agentID, 1, 1)
	if err != nil || len(listed) != 1 || listed[0].Assignment.Record.ExecutionEpoch != 2 {
		t.Fatalf("reconnected assignment is not listable: %v", err)
	}
}

func TestReleaseReconnectAndMutationEventRaceIsCASFenced(t *testing.T) {
	fixture := newOrdinaryReleaseClaimFixture(t)
	start := make(chan struct{})
	reconnectResult := make(chan error, 1)
	eventResult := make(chan error, 1)
	go func() {
		<-start
		_, err := fixture.repository.ReconnectAgentAssignment(context.Background(), fixture.claim)
		reconnectResult <- err
	}()
	go func() {
		<-start
		_, err := fixture.repository.AppendTaskEvent(context.Background(), TaskEventInput{
			Identity: TaskEventIdentity{
				AssignmentID: fixture.claim.Assignment.Record.AssignmentID,
				AgentID:      fixture.agentID, AgentGeneration: 1, TaskID: fixture.claim.Task.Record.ID,
				StepID: fixture.forwardStepID, Attempt: 1, Ordinal: 1,
			},
			State: TaskEventStateRunning, Payload: json.RawMessage(`{"message":"running"}`),
		}, fixture.now.Add(2*time.Second))
		eventResult <- err
	}()
	close(start)
	reconnectErr, eventErr := <-reconnectResult, <-eventResult
	if reconnectErr != nil || eventErr != nil && !errors.Is(eventErr, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("reconnect/event race errors = %v / %v", reconnectErr, eventErr)
	}
	current, err := fixture.repository.GetTaskAssignment(context.Background(), fixture.claim.Task.Record.ID)
	if err != nil || current.Assignment.Record.ExecutionEpoch != 2 {
		t.Fatalf("reconnect/event race assignment = %#v, %v", current, err)
	}
	if eventErr == nil && current.Assignment.Record.ExecutionMode != TaskExecutionModeRecoveryOnly ||
		eventErr != nil && current.Assignment.Record.ExecutionMode != TaskExecutionModeForward {
		t.Fatalf("reconnect/event race mode = %s, event error %v", current.Assignment.Record.ExecutionMode, eventErr)
	}
}

// Rationale: forward and recovery reconnects must advance the epoch without separating the Task from its assignment.
func TestReleaseReconnectPreservesListableAssignmentCoRevision(t *testing.T) {
	fixture := newOrdinaryReleaseClaimFixture(t)
	ctx := context.Background()
	forward, err := fixture.repository.ReconnectAgentAssignment(ctx, fixture.claim)
	if err != nil || forward.Assignment.Record.ExecutionMode != TaskExecutionModeForward ||
		forward.Assignment.Record.ExecutionEpoch != 2 || forward.Task.Revision != forward.Assignment.Revision {
		t.Fatalf("forward reconnect = %#v, %v", forward, err)
	}
	listed, err := fixture.repository.ListAgentAssignments(ctx, fixture.agentID, 1, 1)
	if err != nil || len(listed) != 1 || listed[0].Assignment.Record.ExecutionEpoch != 2 ||
		listed[0].Task.Revision != listed[0].Assignment.Revision {
		t.Fatalf("list forward reconnect assignment = %#v, %v", listed, err)
	}
	result := TaskResultRecord{
		Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticComposeFailed,
		ReconciliationRequired: true, ExecutionEpoch: forward.Assignment.Record.ExecutionEpoch,
	}
	if _, err := fixture.repository.AcknowledgeTask(
		ctx, fixture.agentID, 1, fixture.claim.Task.Record.ID, fixture.claim.Assignment.Record.AssignmentID,
		TaskStatusFailed, result, fixture.now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	recovery, err := fixture.repository.GetTaskAssignment(ctx, fixture.claim.Task.Record.ID)
	if err != nil || recovery.Assignment.Record.ExecutionMode != TaskExecutionModeRecoveryOnly {
		t.Fatalf("recovery assignment = %#v, %v", recovery, err)
	}
	reconnected, err := fixture.repository.ReconnectAgentAssignment(ctx, recovery)
	if err != nil || reconnected.Assignment.Record.ExecutionEpoch != recovery.Assignment.Record.ExecutionEpoch+1 ||
		reconnected.Task.Revision != reconnected.Assignment.Revision {
		t.Fatalf("recovery reconnect = %#v, %v", reconnected, err)
	}
	listed, err = fixture.repository.ListAgentAssignments(ctx, fixture.agentID, 1, 1)
	if err != nil || len(listed) != 1 || listed[0].Assignment.Record.ExecutionEpoch != 4 ||
		listed[0].Task.Revision != listed[0].Assignment.Revision {
		t.Fatalf("list recovery reconnect assignment = %#v, %v", listed, err)
	}
}

func TestOldPrimaryAcknowledgementReplayAfterRecoveryIsExact(t *testing.T) {
	fixture := newOrdinaryReleaseClaimFixture(t)
	result := TaskResultRecord{
		Kind: TaskResultCompose, ExitCode: 17, FailedStepID: fixture.forwardStepID,
		Diagnostic: TaskResultDiagnosticComposeFailed, ReconciliationRequired: true, ExecutionEpoch: 1,
	}
	transitioned, err := fixture.repository.AcknowledgeTask(
		context.Background(), fixture.agentID, 1, fixture.claim.Task.Record.ID,
		fixture.claim.Assignment.Record.AssignmentID, TaskStatusFailed, result, fixture.now.Add(2*time.Second),
	)
	if err != nil || transitioned.Record.Status != TaskStatusRunning || transitioned.Record.Result != nil {
		t.Fatalf("primary recovery transition = %#v, %v", transitioned, err)
	}
	listed, err := fixture.repository.ListAgentAssignments(context.Background(), fixture.agentID, 1, 1)
	if err != nil || len(listed) != 1 || listed[0].Task.Revision != listed[0].Assignment.Revision ||
		transitioned.Revision != listed[0].Task.Revision {
		t.Fatalf("list assignment after failed acknowledgement = %#v, %v; transition %#v", listed, err, transitioned)
	}
	replay, err := fixture.repository.AcknowledgeTask(
		context.Background(), fixture.agentID, 1, fixture.claim.Task.Record.ID,
		fixture.claim.Assignment.Record.AssignmentID, TaskStatusFailed, result, fixture.now.Add(3*time.Second),
	)
	if err != nil || replay.Record.Status != TaskStatusRunning || replay.Record.Result != nil {
		t.Fatalf("old primary replay = %#v, %v", replay, err)
	}
	changed := result
	changed.ExitCode++
	if _, err := fixture.repository.AcknowledgeTask(
		context.Background(), fixture.agentID, 1, fixture.claim.Task.Record.ID,
		fixture.claim.Assignment.Record.AssignmentID, TaskStatusFailed, changed, fixture.now.Add(3*time.Second),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("changed old primary acknowledgement error = %v", err)
	}
}

type ordinaryReleaseClaimFixture struct {
	now                  time.Time
	repository           *TaskRepository
	claim                TaskAssignment
	agentID, artifactID  string
	serviceID, releaseID string
	forwardStepID        string
}

func newOrdinaryReleaseClaimFixture(t *testing.T) ordinaryReleaseClaimFixture {
	t.Helper()
	return newOrdinaryReleaseClaimFixtureForTarget(t, false)
}

func newOrdinaryReleaseClaimFixtureForTarget(t *testing.T, serviceTarget bool) ordinaryReleaseClaimFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 9, 4, 19, 0, 0, 0, time.UTC)
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	taskID := ids.NewAt(ids.KindTask, now, 100)
	operationID := ids.NewAt(ids.KindOperation, now, 101)
	planID := ids.NewAt(ids.KindPlan, now, 102)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 103)
	serviceID := ids.NewAt(ids.KindService, now, 104)
	releaseID := ids.NewAt(ids.KindDeployment, now, 105)
	artifactID := ids.NewAt(ids.KindConfig, now, 106)
	forwardStepID := ids.NewAt(ids.KindStep, now, 107)
	probeStepID := ids.NewAt(ids.KindStep, now, 108)
	compensateStepID := ids.NewAt(ids.KindStep, now, 109)
	publicationID := ids.NewULID()
	procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Members: []executionplan.CandidateReleaseMemberInput{{
			ServiceID: serviceID, CandidateReleaseID: releaseID, CandidateArtifactID: artifactID,
			ForwardStepIDs: []string{forwardStepID},
			CandidateAbsence: &executionplan.CandidateAbsenceInput{
				ComposeProjectName: "gp-" + environmentID, ProbeStepID: probeStepID, CompensateStepID: compensateStepID,
				Services: []executionplan.CandidateServiceIdentity{{ServiceID: serviceID, ReleaseID: releaseID}},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	procedureBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(procedure)
	if err != nil {
		t.Fatal(err)
	}
	manifest := ReleaseStagedManifest{
		PublicationID: publicationID, OperationID: operationID, CreatedAt: now,
		Members: []ReleaseStagedMemberRef{{
			ServiceID: serviceID, ReleaseID: releaseID, IntentDigest: strings.Repeat("b", 64),
			RenderDigest: strings.Repeat("c", 64), CheckpointDigest: strings.Repeat("d", 64),
		}},
	}
	manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := encodeReleaseRecord("release-publication", ReleasePublicationMarker{
		PublicationID: publicationID, OperationID: operationID, ManifestDigest: manifest.Digest, PublishedAt: now,
		CandidateReleaseDescriptor: executionplan.CandidateReleaseDescriptor{
			PlanID: planID, PlanHash: bytes.Repeat([]byte{0xaa}, sha256.Size),
			Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, ProcedureBytes: procedureBytes,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestValue, err := encodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		t.Fatal(err)
	}
	epochValue, err := encodeEnvironmentMutationEpochRecord(
		EnvironmentMutationEpochRecord{EnvironmentID: environmentID},
	)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationPut, Key: releasePublicationKey(publicationID), Value: markerValue},
		{Type: MutationPut, Key: releaseManifestStagingKey(publicationID), Value: manifestValue},
		{Type: MutationPut, Key: environmentMutationEpochKey(environmentID), Value: epochValue},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed ordinary Release authority = %#v, %v", seeded, err)
	}
	task := newTaskRecord(
		taskID, operationID,
		TaskOwner{
			WorkspaceType: TaskWorkspaceTenant, TenantID: ids.NewAt(ids.KindTenant, now, 110),
			ProjectID: ids.NewAt(ids.KindProject, now, 111), EnvironmentID: environmentID,
		},
		TaskActorOperator, TaskDeploy, environmentID, 120, now,
	)
	task.Executor = TaskExecutorAgent
	task.IdempotencyKey = "ordinary-release-claim"
	task.PlanID, task.PlanHash = planID, strings.Repeat("a", 64)
	task.RenderGeneration = 1
	task.Params = map[string]string{
		TaskReleasePublicationParam: publicationID, TaskComposeArtifactParam: artifactID,
		TaskMaterializationEnvironmentParam: environmentID,
	}
	if serviceTarget {
		task.Target = serviceID
		delete(task.Params, TaskMaterializationEnvironmentParam)
	}
	task.Steps = []TaskStepRecord{
		{Kind: TaskStepOperation, ID: forwardStepID},
		{Kind: TaskStepOperation, ID: probeStepID},
		{Kind: TaskStepOperation, ID: compensateStepID},
	}
	seedBlueprintRequirementTask(t, store, task)
	agentID := ids.NewAt(ids.KindAgent, now, 113)
	claim, found, err := repository.ClaimNextTask(ctx, agentID, 1, now.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("ordinary Release ClaimNextTask() = %#v, %t, %v", claim, found, err)
	}
	return ordinaryReleaseClaimFixture{
		now: now, repository: repository, claim: claim, agentID: agentID,
		artifactID: artifactID, serviceID: serviceID, releaseID: releaseID, forwardStepID: forwardStepID,
	}
}

func TestReleaseRecoveryRecordCreateOnceReplayAndConflict(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, at, 1)
	status := TaskStatusFailed
	result := TaskResultRecord{
		Kind:                   TaskResultCompose,
		ExitCode:               1,
		Diagnostic:             TaskResultDiagnosticComposeFailed,
		ReconciliationRequired: true,
	}
	primaryDigest, err := canonicalPrimaryReportSHA256(status, result)
	if err != nil {
		t.Fatal(err)
	}
	record := releaseRecoveryRecord{
		Schema: 1, TaskID: taskID, AssignmentID: ids.NewAt(ids.KindAssignment, at, 2),
		OperationID: ids.NewAt(ids.KindOperation, at, 3), PlanHash: strings.Repeat("1", 64),
		RestorationAuthoritySHA256: strings.Repeat("2", 64), PrimaryReportSHA256: primaryDigest,
		PrimaryStatus: status, PrimaryResult: result,
		RecoveryDeadline: at.Add(time.Hour),
		RecoveryStepIDs:  []string{ids.NewAt(ids.KindStep, at, 4), ids.NewAt(ids.KindStep, at, 5)},
		Phase:            ReleaseRecoveryPhaseProbe, EvidenceRevision: 7,
	}
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := repository.createReleaseRecoveryRecord(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := repository.createReleaseRecoveryRecord(context.Background(), record)
	if err != nil || !reflect.DeepEqual(replay.Record, first.Record) || replay.Revision != first.Revision {
		t.Fatalf("exact replay = %#v, %v, want revision %d", replay, err, first.Revision)
	}
	changed := record
	changed.EvidenceRevision++
	if _, err := repository.createReleaseRecoveryRecord(context.Background(), changed); !isKind(
		err,
		errs.KindStateConflict,
	) {
		t.Fatalf("changed replay error = %v, want state conflict", err)
	}
	corrupt := record
	corrupt.PrimaryReportSHA256 = strings.Repeat("f", 64)
	if _, err := encodeReleaseRecoveryRecord(corrupt); err == nil {
		t.Fatal("recovery record accepted a mismatched immutable primary report")
	}
}

func TestReleaseRecoveryAuthorityDigestAndCanonicalProgress(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 4, 15, 0, 0, 0, time.UTC)
	probeA, probeB := ids.NewAt(ids.KindStep, at, 1), ids.NewAt(ids.KindStep, at, 2)
	compensateA, compensateB := ids.NewAt(ids.KindStep, at, 3), ids.NewAt(ids.KindStep, at, 4)
	forwardA, forwardB := ids.NewAt(ids.KindStep, at, 8), ids.NewAt(ids.KindStep, at, 9)
	procedure := &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{
		{
			ForwardStepIds:   []string{forwardA},
			CandidateAbsence: &agentpb.CandidateAbsenceRestoration{ProbeStepId: probeA, CompensateStepId: compensateA},
		},
		{
			ForwardStepIds:   []string{forwardB},
			CandidateAbsence: &agentpb.CandidateAbsenceRestoration{ProbeStepId: probeB, CompensateStepId: compensateB},
		},
	}}
	candidates := make([]ReleaseRestorationCandidate, len(procedure.Members))
	for index, member := range procedure.Members {
		member.ServiceId = ids.NewAt(ids.KindService, at, int64(20+index))
		member.CandidateReleaseId = ids.NewAt(ids.KindDeployment, at, int64(30+index))
		candidates[index] = ReleaseRestorationCandidate{
			ServiceID: member.ServiceId,
			ReleaseID: member.CandidateReleaseId,
			Target:    ReleaseRestorationCandidateAbsence,
		}
	}
	steps, err := releaseRestorationStepIDs(procedure, candidates)
	if err != nil || !reflect.DeepEqual(steps, []string{probeA, probeB, compensateB, compensateA}) {
		t.Fatalf("canonical recovery steps = %v, %v", steps, err)
	}
	result := TaskResultRecord{
		Kind:                   TaskResultCompose,
		ExitCode:               1,
		Diagnostic:             TaskResultDiagnosticComposeFailed,
		ReconciliationRequired: true,
	}
	report, err := canonicalPrimaryReportSHA256(TaskStatusFailed, result)
	if err != nil {
		t.Fatal(err)
	}
	record := releaseRecoveryRecord{
		Schema: 1, TaskID: ids.NewAt(ids.KindTask, at, 5), AssignmentID: ids.NewAt(ids.KindAssignment, at, 6),
		OperationID: ids.NewAt(ids.KindOperation, at, 7), PlanHash: strings.Repeat("1", 64),
		RestorationAuthoritySHA256: strings.Repeat("2", 64), PrimaryReportSHA256: report,
		PrimaryStatus: TaskStatusFailed, PrimaryResult: result, RecoveryDeadline: at.Add(time.Hour),
		MutationEvidence: []releaseRecoveryMutationEvidence{
			{StepID: forwardB, Running: true, Completed: true},
		}, RecoveryStepIDs: steps,
		Phase: ReleaseRecoveryPhaseProbe, EvidenceRevision: 9,
	}
	digest, err := releaseRecoveryRecordSHA256(record)
	if err != nil {
		t.Fatal(err)
	}
	applicable, err := releaseApplicableCompensationStepIDs(procedure, candidates, record.MutationEvidence)
	if err != nil || !reflect.DeepEqual(applicable, []string{compensateB}) {
		t.Fatalf("recorded mutation compensation = %v, %v", applicable, err)
	}
	changedEvidence := record
	changedEvidence.MutationEvidence = []releaseRecoveryMutationEvidence{{StepID: forwardB, Running: true}}
	changedEvidenceDigest, err := releaseRecoveryRecordSHA256(changedEvidence)
	if err != nil || changedEvidenceDigest == digest {
		t.Fatalf("changed mutation evidence digest = %q, %v", changedEvidenceDigest, err)
	}
	changedDeadline := record
	changedDeadline.RecoveryDeadline = changedDeadline.RecoveryDeadline.Add(time.Nanosecond)
	changedDigest, err := releaseRecoveryRecordSHA256(changedDeadline)
	if err != nil || changedDigest == digest {
		t.Fatalf("changed recovery deadline digest = %q, %v", changedDigest, err)
	}
	for index, stepID := range steps {
		next, changed, advanceErr := advanceReleaseRecoveryRecord(record, TaskEventInput{
			Identity: TaskEventIdentity{StepID: stepID}, State: TaskEventStateCompleted,
		}, int64(10+index))
		if advanceErr != nil || !changed {
			t.Fatalf("advance %d = %#v, %v", index, next, advanceErr)
		}
		nextDigest, digestErr := releaseRecoveryRecordSHA256(next)
		if digestErr != nil || nextDigest != digest {
			t.Fatalf("progress digest = %q, %v, want %q", nextDigest, digestErr, digest)
		}
		record = next
	}
	if record.Phase != ReleaseRecoveryPhaseProven || int(record.Cursor) != len(steps) {
		t.Fatalf("terminal recovery progress = %#v", record)
	}
}

func TestTaskAssignmentRequiresEpochModeDeadlinesAndRecoveryDigest(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	record := TaskAssignmentRecord{
		AssignmentID: ids.NewAt(ids.KindAssignment, at, 1), TaskID: ids.NewAt(ids.KindTask, at, 2),
		Executor: TaskExecutorAgent, AgentID: ids.NewAt(ids.KindAgent, at, 3), AgentGeneration: 1,
		ClaimedTaskRevision: 4, AssignedAt: at, Deadline: at.Add(time.Minute), RecoveryDeadline: at.Add(2 * time.Minute),
		ExecutionMode: TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	if _, err := encodeTaskAssignment(record); err != nil {
		t.Fatalf("forward assignment rejected: %v", err)
	}
	record.ExecutionMode = TaskExecutionModeRecoveryOnly
	if _, err := encodeTaskAssignment(record); err == nil {
		t.Fatal("recovery assignment accepted without recovery record digest")
	}
	record.ReleaseRecoveryRecordSHA256 = strings.Repeat("3", 64)
	if _, err := encodeTaskAssignment(record); err == nil {
		t.Fatal("recovery assignment accepted without restoration authority")
	}
}

func TestBlueprintRestorationAuthorityPreservesConfiguredAppliedWitness(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 4, 13, 0, 0, 0, time.UTC)
	task := TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 10), OperationID: ids.NewAt(ids.KindOperation, at, 11),
		Owner: TaskOwner{EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 12)}, Type: TaskUpdate,
		PlanHash: strings.Repeat("4", 64), RenderGeneration: 2,
		Params: map[string]string{
			TaskReleasePublicationParam: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
			TaskComposeArtifactParam:    ids.NewAt(ids.KindConfig, at, 13),
		},
	}
	manifest := ReleaseStagedManifest{
		PublicationID: task.Params[TaskReleasePublicationParam], OperationID: task.OperationID,
		Members: []ReleaseStagedMemberRef{
			{ServiceID: ids.NewAt(ids.KindService, at, 14), ReleaseID: ids.NewAt(ids.KindDeployment, at, 15)},
		},
	}
	absent, absentDigest, err := buildBlueprintAbsenceAuthorityForTest(
		task,
		taskMaterializationAppliedPredecessor{},
		manifest,
		nil,
	)
	if err != nil || absent.Candidates[0].Target != ReleaseRestorationCandidateAbsence ||
		absent.AppliedPredecessor != nil ||
		!validSHA256(absentDigest) {
		t.Fatalf("absence authority = %#v, %q, %v", absent, absentDigest, err)
	}
	predecessor := taskMaterializationAppliedPredecessor{
		Present: true, KeyRevision: 21, RevisionID: ids.NewAt(ids.KindTask, at, 16), RenderGeneration: 1,
	}
	artifact := withTestEnvironmentComposeArtifact(
		EnvironmentComposeProjection{EnvironmentID: task.Owner.EnvironmentID},
	).ComposeArtifact
	present, presentDigest, err := buildBlueprintAbsenceAuthorityForTest(task, predecessor, manifest, artifact)
	if err != nil || present.Candidates[0].Target != ReleaseRestorationCandidateAbsence ||
		present.AppliedPredecessor == nil ||
		present.AppliedPredecessor.KeyRevision != predecessor.KeyRevision ||
		!bytes.Equal(present.AppliedPredecessor.ComposeArtifact, artifact) ||
		presentDigest == absentDigest {
		t.Fatalf("present authority = %#v, %q, %v", present, presentDigest, err)
	}
	if _, _, err := buildBlueprintAbsenceAuthorityForTest(task, predecessor, manifest, nil); err == nil {
		t.Fatal("present predecessor accepted without its exact artifact")
	}
	drifted := predecessor
	drifted.KeyRevision++
	_, driftedDigest, err := buildBlueprintAbsenceAuthorityForTest(task, drifted, manifest, artifact)
	if err != nil || driftedDigest == presentDigest {
		t.Fatalf("predecessor drift digest = %q, %v, want changed", driftedDigest, err)
	}
}
