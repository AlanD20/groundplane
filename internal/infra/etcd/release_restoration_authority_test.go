package etcd

import (
	bytes "bytes"
	context "context"
	sha256 "crypto/sha256"
	json "encoding/json"
	errors "errors"
	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	core "github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
	reflect "reflect"
	slices "slices"
	strings "strings"
	testing "testing"
	time "time"
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
	read, err := published.store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testreleases.ReleasePublicationKey(publicationID),
				testreleases.ReleaseManifestStagingKey(publicationID),
				testhierarchy.EnvironmentMutationEpochKey(published.environmentID),
			},
		},
	)
	if err != nil || read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		t.Fatalf("read Blueprint Release authority = %#v, %v", read, err)
	}
	marker, err := testreleases.DecodeReleaseRecord[testreleases.ReleasePublicationMarker](
		read.Values[0].Value,
		"release-publication",
	)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := testreleases.DecodeReleaseRecord[testreleases.ReleaseStagedManifest](
		read.Values[1].Value,
		"release-staged-manifest",
	)
	if err != nil || len(manifest.Members) != 1 {
		t.Fatalf("decode staged manifest = %#v, %v", manifest, err)
	}
	member := manifest.Members[0]
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: published.environmentID, RevisionID: task.ID, RenderGeneration: uint64(task.RenderGeneration),
		DesiredServices: []testservices.EnvironmentServiceProjection{{
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
	render := testreleaserender.ReleaseRenderInput{
		ReleaseID: member.ReleaseID, PlanID: task.PlanID, ArtifactID: task.Params[testtaskjournal.TaskComposeArtifactParam],
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
	rawRender, err := testreleaserender.EncodeReleaseRenderInput(render)
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
		stored, readErr := published.store.Get(ctx, testtaskjournal.TaskStorageKey(task.ID))
		if readErr != nil || stored.Entry == nil {
			t.Fatalf("read published Task: %v", readErr)
		}
		task, err = DecodeTaskRecord(stored.Entry.Value)
		if err != nil {
			t.Fatal(err)
		}
		root, readErr := published.store.Get(ctx, testscriptsourceevidence.ScriptSourceRootKey(task.OperationID))
		if readErr != nil || root.Entry == nil {
			t.Fatalf("read Script source root: %v", readErr)
		}
		values[testscriptsourceevidence.ScriptSourceRootKey(task.OperationID)] = root.Entry.Value
		task.ComponentActionStepIDs = []string{componentStepID}
		task.Steps = append(
			task.Steps,
			testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: componentStepID},
		)
		marker.CandidateReleaseDescriptor.ComponentActionStepIDs = []string{componentStepID}
		values[testtaskjournal.TaskStorageKey(task.ID)], err = EncodeTaskRecord(task)
		if err != nil {
			t.Fatal(err)
		}
	}
	for key, record := range map[string]struct {
		typeName string
		value    any
	}{testreleases.ReleasePublicationKey(publicationID): {"release-publication", marker}, testreleases.ReleaseManifestStagingKey(publicationID): {"release-staged-manifest", manifest}, testreleases.ReleaseIntentStagingKey(publicationID, member.ReleaseID): {"release-intent", intent}, testreleases.ReleaseRenderInputStagingKey(publicationID, member.ReleaseID): {"release-render-input", json.RawMessage(rawRender)}, testreleases.ReleaseCheckpointStagingKey(publicationID, member.ReleaseID): {"release-checkpoint", checkpoint}} {
		values[key], err = testreleases.EncodeReleaseRecord(record.typeName, record.value)
		if err != nil {
			t.Fatal(err)
		}
	}
	mutations := make([]testkeyvalue.Mutation, 0, len(values))
	for key, value := range values {
		mutations = append(mutations, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: value})
	}
	mutations = append(mutations, testkeyvalue.Mutation{
		Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentMutationEpochKey(published.environmentID), Value: read.Values[2].Value,
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
		persisted, err := published.store.Get(ctx, testtaskjournal.TaskStorageKey(task.ID))
		if err != nil {
			t.Fatal(err)
		}
		actualTask, err := DecodeTaskRecord(persisted.Entry.Value)
		if err != nil {
			t.Fatal(err)
		}
		persisted, err = published.store.Get(ctx, testreleases.ReleasePublicationKey(publicationID))
		if err != nil {
			t.Fatal(err)
		}
		actualMarker, err := testreleases.DecodeReleaseRecord[testreleases.ReleasePublicationMarker](
			persisted.Entry.Value,
			"release-publication",
		)
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
		claim.Assignment.Record.RestorationAuthority.Candidates[0].Target != testtaskassignments.ReleaseRestorationCandidateAbsence {
		t.Fatalf("claim recovery fixture = %#v, %t, %v", claim, found, err)
	}
	procedure, err := executionplan.OpenCandidateReleaseDescriptor(marker.CandidateReleaseDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	primary := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultCompose, ExitCode: 17, FailedStepID: procedure.GetMembers()[0].GetForwardStepIds()[0],
		Diagnostic: testtaskjournal.TaskResultDiagnosticComposeFailed, ReconciliationRequired: true, ExecutionEpoch: 1,
	}
	if componentRunning {
		_, err = repository.AppendTaskEvent(
			ctx, testtaskjournal.TaskEventInput{
				Identity: testtaskjournal.TaskEventIdentity{
					AssignmentID:    claim.Assignment.Record.AssignmentID,
					AgentID:         agentID,
					AgentGeneration: 1,
					TaskID:          task.ID,
					StepID:          componentStepID,
					Attempt:         1,
					Ordinal:         1,
				},
				State:   testtaskjournal.TaskEventStateRunning,
				Payload: json.RawMessage(`{"message":"Component activation started"}`),
			}, task.CreatedAt.Add(150*time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	transitioned, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusFailed,
		primary,
		task.CreatedAt.Add(3*time.Minute),
	)
	if err != nil || transitioned.Record.Status != testtaskjournal.TaskStatusRunning ||
		transitioned.Record.Result != nil {
		t.Fatalf("transition recovery fixture = %#v, %v", transitioned, err)
	}
	recovery, err := repository.GetTaskAssignment(ctx, task.ID)
	if err != nil || recovery.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeRecoveryOnly ||
		recovery.Assignment.Record.ExecutionEpoch != 2 || recovery.ReleaseRecovery == nil {
		t.Fatalf("recovery assignment = %#v, %v", recovery, err)
	}
	ordinal := uint64(1)
	for _, stepID := range recovery.ReleaseRecovery.StepIDs {
		for _, state := range []testtaskjournal.TaskEventState{testtaskjournal.TaskEventStateRunning, testtaskjournal.TaskEventStateCompleted} {
			_, err = repository.AppendTaskEvent(ctx, testtaskjournal.TaskEventInput{
				Identity: testtaskjournal.TaskEventIdentity{
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
	final := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone, ExecutionEpoch: 2,
		ReleaseRecoveryRecordSHA256: recovery.Assignment.Record.ReleaseRecoveryRecordSHA256,
		CandidateAbsenceEvidence: &testtaskjournal.TaskCandidateAbsenceEvidence{
			AssignmentID: recovery.Assignment.Record.AssignmentID, PlanHash: authority.PlanHash,
			AuthoritySHA256:     recovery.Assignment.Record.RestorationAuthoritySHA256,
			ComposeProjectName:  procedure.GetMembers()[0].GetCandidateAbsence().GetComposeProjectName(),
			CandidateArtifactID: authority.CandidateArtifactID, AbsenceProven: true,
			Candidates: []testtaskjournal.TaskCandidateAbsenceCandidate{
				{ServiceID: member.ServiceID, ReleaseID: member.ReleaseID},
			},
		},
	}
	terminal, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		recovery.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		final,
		task.CreatedAt.Add(10*time.Minute),
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
		if readErr != nil ||
			retained.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeRecoveryOnly {
			t.Fatalf("recovery ownership lost: %#v %v", retained, readErr)
		}
		return
	}
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusFailed || terminal.Record.Result == nil ||
		terminal.Record.Result.ExitCode != primary.ExitCode || terminal.Record.Result.FailedStepID != primary.FailedStepID ||
		terminal.Record.Result.ReconciliationRequired {
		t.Fatalf("final recovery acknowledgement = %#v, %v", terminal, err)
	}
	cleanup, err := published.store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testtaskjournal.TaskAssignmentKey(agentID, task.ID),
				testtaskjournal.TaskAssignmentIndexKey(task.ID),
				testtaskjournal.TaskActiveOperationKey(
					task.OperationID,
				),
				testtaskjournal.TaskTimeoutIndexKey(task.ID, recovery.Assignment.Record.RecoveryDeadline),
				testtaskjournal.TaskMaterializationWriterKey(published.environmentID),
				testtaskassignments.ReleaseRecoveryKey(task.ID),
				testscriptsourceevidence.ScriptSourceRootKey(task.OperationID),
				testenvironmentprojection.EnvironmentComposeProjectionStorageKey(published.environmentID),
			},
		},
	)
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
		if selected := task.Params[testreleaserender.ReleaseHookStepExecutionParam(step.ID)]; selected != "" {
			executionID = selected
		}
	}
	executionRead, err := published.store.Get(ctx, testscriptexecutions.ScriptExecutionKey(executionID))
	if err != nil || executionRead.Entry == nil {
		t.Fatalf("released Script execution = %#v, %v", executionRead, err)
	}
	execution, err := testrecordcodec.Decode[testscriptexecutions.ScriptExecutionRecord](
		executionRead.Entry.Value,
		"script-execution",
	)
	if err != nil || !releaseRecoveryParentFailureExecutionMatches(execution) {
		t.Fatalf("parent-failure Script execution = %#v, %v", execution, err)
	}
	replay, err := repository.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		recovery.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		final,
		task.CreatedAt.Add(11*time.Minute),
	)
	if err != nil || replay.Revision != terminal.Revision || replay.Record.Status != terminal.Record.Status {
		t.Fatalf("exact final recovery replay = %#v, %v", replay, err)
	}
	changed := final
	changed.CandidateAbsenceEvidence = testtaskjournal.CloneTaskCandidateAbsenceEvidence(final.CandidateAbsenceEvidence)
	changed.CandidateAbsenceEvidence.AuthoritySHA256 = strings.Repeat("f", 64)
	if _, err := repository.AcknowledgeTask(
		ctx, agentID, 1, task.ID, recovery.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, changed, task.CreatedAt.Add(11*time.Minute),
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("mismatched final recovery replay error = %v", err)
	}
}

// Rationale: recovery replay must reuse Task-result equality without weakening optional authority identity.
func TestReleaseRecoveryAbsenceEvidenceEqualityUsesCanonicalTaskResultSemantics(t *testing.T) {
	left := &testtaskjournal.TaskCandidateAbsenceEvidence{
		AssignmentID: "assignment", PlanHash: strings.Repeat("a", 64), AuthoritySHA256: strings.Repeat("b", 64),
		ComposeProjectName: "project", CandidateArtifactID: "artifact", AbsenceProven: true,
	}
	right := testtaskjournal.CloneTaskCandidateAbsenceEvidence(left)
	right.Candidates = []testtaskjournal.TaskCandidateAbsenceCandidate{}
	if !testtaskjournal.TaskCandidateAbsenceEvidenceEqual(left, right) {
		t.Fatal("canonical absence evidence equality distinguished nil and empty candidates")
	}
	if testtaskjournal.TaskCandidateAbsenceEvidenceEqual(nil, right) {
		t.Fatal("canonical absence evidence equality collapsed optional evidence presence")
	}
	right.AuthoritySHA256 = strings.Repeat("c", 64)
	if testtaskjournal.TaskCandidateAbsenceEvidenceEqual(left, right) {
		t.Fatal("canonical absence evidence equality accepted changed authority")
	}
}

// Rationale: completed Blueprint replay rejects every terminal or retention identity drift, including expiry.
func TestBlueprintTerminalReplayTypedEqualityPreservesExactLedgerIdentity(t *testing.T) {
	if testattachments.SameBlueprintAttachStrings(nil, []string{}) ||
		!testattachments.SameBlueprintAttachStrings(
			[]string{"first", "second"},
			[]string{"first", "second"},
		) || testattachments.SameBlueprintAttachStrings([]string{"first", "second"}, []string{"second", "first"}) {
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
		authority.Candidates[0].Target != testtaskassignments.ReleaseRestorationCandidateAbsence ||
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
	digest, err := testtaskassignments.ReleaseRestorationAuthoritySHA256(*authority)
	if err != nil || digest != claim.Assignment.Record.RestorationAuthoritySHA256 {
		t.Fatalf("restoration authority digest mismatch: %v", err)
	}
	if claim.Task.Record.Target != fixture.serviceID ||
		claim.Task.Record.Params[testtaskjournal.TaskMaterializationEnvironmentParam] != "" {
		t.Fatal("fixture does not represent a Service-target release without a writer")
	}
	read, err := fixture.repository.store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testtaskjournal.TaskAssignmentKey(fixture.agentID, claim.Task.Record.ID),
				testtaskjournal.TaskAssignmentIndexKey(claim.Task.Record.ID),
				testtaskjournal.TaskTimeoutIndexKey(claim.Task.Record.ID, claim.Assignment.Record.Deadline),
			},
		},
	)
	if err != nil || len(read.Values) != 3 {
		t.Fatalf("read assignment copies: %v", err)
	}
	defer testkeyvalue.ClearValues(read.Values)
	encoded, err := testtaskassignments.EncodeTaskAssignment(claim.Assignment.Record)
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

// Rationale: forward and recovery reconnects must advance the epoch without separating the Task from its assignment.
func TestReleaseReconnectPreservesListableAssignmentCoRevision(t *testing.T) {
	fixture := newOrdinaryReleaseClaimFixture(t)
	ctx := context.Background()
	forward, err := fixture.repository.ReconnectAgentAssignment(ctx, fixture.claim)
	if err != nil || forward.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeForward ||
		forward.Assignment.Record.ExecutionEpoch != 2 || forward.Task.Revision != forward.Assignment.Revision {
		t.Fatalf("forward reconnect = %#v, %v", forward, err)
	}
	listed, err := fixture.repository.ListAgentAssignments(ctx, fixture.agentID, 1, 1)
	if err != nil || len(listed) != 1 || listed[0].Assignment.Record.ExecutionEpoch != 2 ||
		listed[0].Task.Revision != listed[0].Assignment.Revision {
		t.Fatalf("list forward reconnect assignment = %#v, %v", listed, err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticComposeFailed,
		ReconciliationRequired: true, ExecutionEpoch: forward.Assignment.Record.ExecutionEpoch,
	}
	if _, err := fixture.repository.AcknowledgeTask(
		ctx, fixture.agentID, 1, fixture.claim.Task.Record.ID, fixture.claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, result, fixture.now.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	recovery, err := fixture.repository.GetTaskAssignment(ctx, fixture.claim.Task.Record.ID)
	if err != nil || recovery.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeRecoveryOnly {
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
	result := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultCompose, ExitCode: 17, FailedStepID: fixture.forwardStepID,
		Diagnostic: testtaskjournal.TaskResultDiagnosticComposeFailed, ReconciliationRequired: true, ExecutionEpoch: 1,
	}
	transitioned, err := fixture.repository.AcknowledgeTask(
		context.Background(),
		fixture.agentID,
		1,
		fixture.claim.Task.Record.ID,
		fixture.claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusFailed,
		result,
		fixture.now.Add(2*time.Second),
	)
	if err != nil || transitioned.Record.Status != testtaskjournal.TaskStatusRunning ||
		transitioned.Record.Result != nil {
		t.Fatalf("primary recovery transition = %#v, %v", transitioned, err)
	}
	listed, err := fixture.repository.ListAgentAssignments(context.Background(), fixture.agentID, 1, 1)
	if err != nil || len(listed) != 1 || listed[0].Task.Revision != listed[0].Assignment.Revision ||
		transitioned.Revision != listed[0].Task.Revision {
		t.Fatalf("list assignment after failed acknowledgement = %#v, %v; transition %#v", listed, err, transitioned)
	}
	replay, err := fixture.repository.AcknowledgeTask(
		context.Background(),
		fixture.agentID,
		1,
		fixture.claim.Task.Record.ID,
		fixture.claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusFailed,
		result,
		fixture.now.Add(3*time.Second),
	)
	if err != nil || replay.Record.Status != testtaskjournal.TaskStatusRunning || replay.Record.Result != nil {
		t.Fatalf("old primary replay = %#v, %v", replay, err)
	}
	changed := result
	changed.ExitCode++
	if _, err := fixture.repository.AcknowledgeTask(
		context.Background(), fixture.agentID, 1, fixture.claim.Task.Record.ID,
		fixture.claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusFailed, changed, fixture.now.Add(3*time.Second),
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
	manifest := testreleases.ReleaseStagedManifest{
		PublicationID: publicationID, OperationID: operationID, CreatedAt: now,
		Members: []testreleases.ReleaseStagedMemberRef{{
			ServiceID: serviceID, ReleaseID: releaseID, IntentDigest: strings.Repeat("b", 64),
			RenderDigest: strings.Repeat("c", 64), CheckpointDigest: strings.Repeat("d", 64),
		}},
	}
	manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := testreleases.EncodeReleaseRecord("release-publication", testreleases.ReleasePublicationMarker{
		PublicationID: publicationID, OperationID: operationID, ManifestDigest: manifest.Digest, PublishedAt: now,
		CandidateReleaseDescriptor: executionplan.CandidateReleaseDescriptor{
			PlanID: planID, PlanHash: bytes.Repeat([]byte{0xaa}, sha256.Size),
			Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, ProcedureBytes: procedureBytes,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestValue, err := testreleases.EncodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		t.Fatal(err)
	}
	epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(
		testbackupruntime.EnvironmentMutationEpochRecord{EnvironmentID: environmentID},
	)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testreleases.ReleasePublicationKey(publicationID), Value: markerValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseManifestStagingKey(publicationID),
			Value: manifestValue,
		},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testhierarchy.EnvironmentMutationEpochKey(environmentID),
			Value: epochValue,
		},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed ordinary Release authority = %#v, %v", seeded, err)
	}
	task := newTaskRecord(
		taskID, operationID, testtaskjournal.TaskOwner{
			WorkspaceType: testtaskjournal.TaskWorkspaceTenant, TenantID: ids.NewAt(ids.KindTenant, now, 110),
			ProjectID: ids.NewAt(ids.KindProject, now, 111), EnvironmentID: environmentID,
		}, testtaskjournal.TaskActorOperator, testtaskjournal.TaskDeploy, environmentID, 120, now,
	)
	task.Executor = testtaskjournal.TaskExecutorAgent
	task.IdempotencyKey = "ordinary-release-claim"
	task.PlanID, task.PlanHash = planID, strings.Repeat("a", 64)
	task.RenderGeneration = 1
	task.Params = map[string]string{
		testreleaserender.TaskReleasePublicationParam:       publicationID,
		testtaskjournal.TaskComposeArtifactParam:            artifactID,
		testtaskjournal.TaskMaterializationEnvironmentParam: environmentID,
	}
	if serviceTarget {
		task.Target = serviceID
		delete(task.Params, testtaskjournal.TaskMaterializationEnvironmentParam)
	}
	task.Steps = []testtaskjournal.TaskStepRecord{
		{Kind: testtaskjournal.TaskStepOperation, ID: forwardStepID},
		{Kind: testtaskjournal.TaskStepOperation, ID: probeStepID},
		{Kind: testtaskjournal.TaskStepOperation, ID: compensateStepID},
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
	status := testtaskjournal.TaskStatusFailed
	result := testtaskjournal.TaskResultRecord{
		Kind:                   testtaskjournal.TaskResultCompose,
		ExitCode:               1,
		Diagnostic:             testtaskjournal.TaskResultDiagnosticComposeFailed,
		ReconciliationRequired: true,
	}
	primaryDigest, err := testtaskassignments.CanonicalPrimaryReportSHA256(status, result)
	if err != nil {
		t.Fatal(err)
	}
	record := testtaskassignments.ReleaseRecoveryRecord{
		Schema: 1, TaskID: taskID, AssignmentID: ids.NewAt(ids.KindAssignment, at, 2),
		OperationID: ids.NewAt(ids.KindOperation, at, 3), PlanHash: strings.Repeat("1", 64),
		RestorationAuthoritySHA256: strings.Repeat("2", 64), PrimaryReportSHA256: primaryDigest,
		PrimaryStatus: status, PrimaryResult: result,
		RecoveryDeadline: at.Add(time.Hour),
		RecoveryStepIDs:  []string{ids.NewAt(ids.KindStep, at, 4), ids.NewAt(ids.KindStep, at, 5)},
		Phase:            testtaskassignments.ReleaseRecoveryPhaseProbe, EvidenceRevision: 7,
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
	if _, err := testtaskassignments.EncodeReleaseRecoveryRecord(corrupt); err == nil {
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
	candidates := make([]testtaskassignments.ReleaseRestorationCandidate, len(procedure.Members))
	for index, member := range procedure.Members {
		member.ServiceId = ids.NewAt(ids.KindService, at, int64(20+index))
		member.CandidateReleaseId = ids.NewAt(ids.KindDeployment, at, int64(30+index))
		candidates[index] = testtaskassignments.ReleaseRestorationCandidate{
			ServiceID: member.ServiceId,
			ReleaseID: member.CandidateReleaseId,
			Target:    testtaskassignments.ReleaseRestorationCandidateAbsence,
		}
	}
	steps, err := releaseRestorationStepIDs(procedure, candidates)
	if err != nil || !reflect.DeepEqual(steps, []string{probeA, probeB, compensateB, compensateA}) {
		t.Fatalf("canonical recovery steps = %v, %v", steps, err)
	}
	result := testtaskjournal.TaskResultRecord{
		Kind:                   testtaskjournal.TaskResultCompose,
		ExitCode:               1,
		Diagnostic:             testtaskjournal.TaskResultDiagnosticComposeFailed,
		ReconciliationRequired: true,
	}
	report, err := testtaskassignments.CanonicalPrimaryReportSHA256(testtaskjournal.TaskStatusFailed, result)
	if err != nil {
		t.Fatal(err)
	}
	record := testtaskassignments.ReleaseRecoveryRecord{
		Schema: 1, TaskID: ids.NewAt(ids.KindTask, at, 5), AssignmentID: ids.NewAt(ids.KindAssignment, at, 6),
		OperationID: ids.NewAt(ids.KindOperation, at, 7), PlanHash: strings.Repeat("1", 64),
		RestorationAuthoritySHA256: strings.Repeat("2", 64), PrimaryReportSHA256: report,
		PrimaryStatus: testtaskjournal.TaskStatusFailed, PrimaryResult: result, RecoveryDeadline: at.Add(time.Hour),
		MutationEvidence: []testtaskassignments.ReleaseRecoveryMutationEvidence{
			{StepID: forwardB, Running: true, Completed: true},
		}, RecoveryStepIDs: steps,
		Phase: testtaskassignments.ReleaseRecoveryPhaseProbe, EvidenceRevision: 9,
	}
	digest, err := testtaskassignments.ReleaseRecoveryRecordSHA256(record)
	if err != nil {
		t.Fatal(err)
	}
	applicable, err := releaseApplicableCompensationStepIDs(procedure, candidates, record.MutationEvidence)
	if err != nil || !reflect.DeepEqual(applicable, []string{compensateB}) {
		t.Fatalf("recorded mutation compensation = %v, %v", applicable, err)
	}
	changedEvidence := record
	changedEvidence.MutationEvidence = []testtaskassignments.ReleaseRecoveryMutationEvidence{
		{StepID: forwardB, Running: true},
	}
	changedEvidenceDigest, err := testtaskassignments.ReleaseRecoveryRecordSHA256(changedEvidence)
	if err != nil || changedEvidenceDigest == digest {
		t.Fatalf("changed mutation evidence digest = %q, %v", changedEvidenceDigest, err)
	}
	changedDeadline := record
	changedDeadline.RecoveryDeadline = changedDeadline.RecoveryDeadline.Add(time.Nanosecond)
	changedDigest, err := testtaskassignments.ReleaseRecoveryRecordSHA256(changedDeadline)
	if err != nil || changedDigest == digest {
		t.Fatalf("changed recovery deadline digest = %q, %v", changedDigest, err)
	}
	for index, stepID := range steps {
		next, changed, advanceErr := testtaskassignments.AdvanceReleaseRecoveryRecord(
			record,
			testtaskjournal.TaskEventInput{
				Identity: testtaskjournal.TaskEventIdentity{
					StepID: stepID,
				}, State: testtaskjournal.TaskEventStateCompleted,
			},
			int64(10+index),
		)
		if advanceErr != nil || !changed {
			t.Fatalf("advance %d = %#v, %v", index, next, advanceErr)
		}
		nextDigest, digestErr := testtaskassignments.ReleaseRecoveryRecordSHA256(next)
		if digestErr != nil || nextDigest != digest {
			t.Fatalf("progress digest = %q, %v, want %q", nextDigest, digestErr, digest)
		}
		record = next
	}
	if record.Phase != testtaskassignments.ReleaseRecoveryPhaseProven || int(record.Cursor) != len(steps) {
		t.Fatalf("terminal recovery progress = %#v", record)
	}
}

func TestTaskAssignmentRequiresEpochModeDeadlinesAndRecoveryDigest(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	record := testtaskassignments.TaskAssignmentRecord{
		AssignmentID: ids.NewAt(ids.KindAssignment, at, 1), TaskID: ids.NewAt(ids.KindTask, at, 2),
		Executor: testtaskjournal.TaskExecutorAgent, AgentID: ids.NewAt(ids.KindAgent, at, 3), AgentGeneration: 1,
		ClaimedTaskRevision: 4, AssignedAt: at, Deadline: at.Add(time.Minute), RecoveryDeadline: at.Add(2 * time.Minute),
		ExecutionMode: testtaskassignments.TaskExecutionModeForward, ExecutionEpoch: 1,
	}
	if _, err := testtaskassignments.EncodeTaskAssignment(record); err != nil {
		t.Fatalf("forward assignment rejected: %v", err)
	}
	record.ExecutionMode = testtaskassignments.TaskExecutionModeRecoveryOnly
	if _, err := testtaskassignments.EncodeTaskAssignment(record); err == nil {
		t.Fatal("recovery assignment accepted without recovery record digest")
	}
	record.ReleaseRecoveryRecordSHA256 = strings.Repeat("3", 64)
	if _, err := testtaskassignments.EncodeTaskAssignment(record); err == nil {
		t.Fatal("recovery assignment accepted without restoration authority")
	}
}

func TestBlueprintRestorationAuthorityPreservesConfiguredAppliedWitness(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 4, 13, 0, 0, 0, time.UTC)
	task := TaskRecord{
		ID: ids.NewAt(ids.KindTask, at, 10), OperationID: ids.NewAt(ids.KindOperation, at, 11),
		Owner: testtaskjournal.TaskOwner{
			EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 12),
		}, Type: testtaskjournal.TaskUpdate,
		PlanHash: strings.Repeat("4", 64), RenderGeneration: 2,
		Params: map[string]string{
			testreleaserender.TaskReleasePublicationParam: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
			testtaskjournal.TaskComposeArtifactParam:      ids.NewAt(ids.KindConfig, at, 13),
		},
	}
	manifest := testreleases.ReleaseStagedManifest{
		PublicationID: task.Params[testreleaserender.TaskReleasePublicationParam], OperationID: task.OperationID,
		Members: []testreleases.ReleaseStagedMemberRef{
			{ServiceID: ids.NewAt(ids.KindService, at, 14), ReleaseID: ids.NewAt(ids.KindDeployment, at, 15)},
		},
	}
	absent, absentDigest, err := buildBlueprintAbsenceAuthorityForTest(
		task,
		taskMaterializationAppliedPredecessor{},
		manifest,
		nil,
	)
	if err != nil || absent.Candidates[0].Target != testtaskassignments.ReleaseRestorationCandidateAbsence ||
		absent.AppliedPredecessor != nil ||
		!testrecordcodec.ValidSHA256(absentDigest) {
		t.Fatalf("absence authority = %#v, %q, %v", absent, absentDigest, err)
	}
	predecessor := taskMaterializationAppliedPredecessor{
		Present: true, KeyRevision: 21, RevisionID: ids.NewAt(ids.KindTask, at, 16), RenderGeneration: 1,
	}
	artifact := withTestEnvironmentComposeArtifact(
		testenvironmentprojection.EnvironmentComposeProjection{EnvironmentID: task.Owner.EnvironmentID},
	).ComposeArtifact
	present, presentDigest, err := buildBlueprintAbsenceAuthorityForTest(task, predecessor, manifest, artifact)
	if err != nil || present.Candidates[0].Target != testtaskassignments.ReleaseRestorationCandidateAbsence ||
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
