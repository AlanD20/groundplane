package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	testcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type recoveryFileState struct {
	present bool
	content []byte
	uid     uint32
	gid     uint32
	mode    entrymaterialization.Mode
}

type recoveryMaterializationCall struct {
	stepID     string
	verifyOnly bool
}

type recoveryMaterializationHelper struct {
	mu            sync.Mutex
	state         recoveryFileState
	calls         []recoveryMaterializationCall
	order         *[]string
	failNextWrite bool
	failVerifyAt  int
	verifyCalls   int
}

func (helper *recoveryMaterializationHelper) Run(
	ctx context.Context,
	request materializerrunner.Request,
) error {
	defer request.Stream.Close()
	var content []byte
	header, err := entrymaterialization.Decode(
		ctx,
		request.Stream,
		entrymaterialization.Limits{
			MaxContentBytes:     entrymaterialization.MaximumContentBytes,
			MaxDestinationBytes: entrymaterialization.MaximumDestinationBytes,
		},
		func(_ context.Context, _ entrymaterialization.Header, source io.Reader) error {
			var readErr error
			content, readErr = io.ReadAll(source)
			return readErr
		},
	)
	if err != nil {
		return err
	}
	helper.mu.Lock()
	defer helper.mu.Unlock()
	helper.calls = append(helper.calls, recoveryMaterializationCall{
		stepID: header.StepID(), verifyOnly: request.VerifyOnly,
	})
	if helper.order != nil {
		*helper.order = append(
			*helper.order,
			"file:"+header.StepID()+":"+map[bool]string{false: "write", true: "verify"}[request.VerifyOnly],
		)
	}
	if request.VerifyOnly {
		helper.verifyCalls++
		if helper.failVerifyAt == helper.verifyCalls {
			return errs.New(errs.KindInternal, "test verifier failure")
		}
		wantPresent := header.OutputKind() != entrymaterialization.OutputRemoveGeneratedEnv &&
			header.OutputKind() != entrymaterialization.OutputRemovePlainFile &&
			header.OutputKind() != entrymaterialization.OutputRemoveSecretFile
		if helper.state.present != wantPresent || wantPresent &&
			(!bytes.Equal(helper.state.content, content) || helper.state.uid != header.UID() ||
				helper.state.gid != header.GID() || helper.state.mode != header.Mode()) {
			return errs.New(errs.KindStateConflict, "test pinned file drift")
		}
		return nil
	}
	helper.state = recoveryFileState{
		present: true, content: bytes.Clone(content), uid: header.UID(), gid: header.GID(), mode: header.Mode(),
	}
	if header.OutputKind() == entrymaterialization.OutputRemoveGeneratedEnv ||
		header.OutputKind() == entrymaterialization.OutputRemovePlainFile ||
		header.OutputKind() == entrymaterialization.OutputRemoveSecretFile {
		helper.state = recoveryFileState{}
	}
	if helper.failNextWrite {
		helper.failNextWrite = false
		return errs.New(errs.KindInternal, "test partial write failure")
	}
	return nil
}

type configurationRecoveryFixture struct {
	assignment testtaskassignment.Assignment
	forward    *agentpb.ExecutionStep
	probe      *agentpb.ExecutionStep
	compensate *agentpb.ExecutionStep
	prior      []byte
	next       []byte
}

func newConfigurationRecoveryFixture(t *testing.T) configurationRecoveryFixture {
	t.Helper()
	assignment, _ := sealedRecreateProbeAssignment(t, 1, "singleton")
	plan := proto.CloneOf(assignment.Plan)
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	plan.CandidateReleaseProcedure.Members = nil
	at := time.Date(2026, time.September, 14, 12, 0, 0, 0, time.UTC)
	prior, next := []byte("prior pinned configuration\n"), []byte("candidate configuration\n")
	forward := recoveryMaterializationStep(
		plan.Artifacts[0], ids.NewAt(ids.KindStep, at, 21), ids.NewAt(ids.KindConfig, at, 22), next,
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
	)
	probe := recoveryMaterializationStep(
		plan.Artifacts[0], ids.NewAt(ids.KindStep, at, 23), ids.NewAt(ids.KindConfig, at, 24), prior,
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
	)
	compensate := recoveryMaterializationStep(
		plan.Artifacts[0], ids.NewAt(ids.KindStep, at, 25), ids.NewAt(ids.KindConfig, at, 26), prior,
		agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
	)
	compensate.PrerequisiteStepId = forward.StepId
	plan.CandidateReleaseProcedure.ConfigurationRestoration = &agentpb.ConfigurationRestoration{
		PriorSnapshotId: ids.NewAt(ids.KindConfig, at, 27), PriorSnapshotSha256: bytes.Repeat([]byte{0x41}, 32),
		Files: []*agentpb.ConfigurationFileRestoration{{
			ForwardStepId: forward.StepId, ProbeStepId: probe.StepId, CompensateStepId: compensate.StepId,
		}},
	}
	plan.Steps = append([]*agentpb.ExecutionStep{forward}, plan.Steps...)
	plan.Steps = append(plan.Steps, probe, compensate)
	assignment.Plan = plan
	assignment.RestorationAuthority.PlanHash = bytes.Clone(plan.PlanHash)
	assignment.AssignmentID = ids.NewAt(ids.KindAssignment, at, 28)
	assignment.ExecutionEpoch = 7
	assignment.ForwardDeadline = time.Now().Add(time.Minute)
	assignment.RecoveryDeadline = assignment.ForwardDeadline.Add(time.Minute)
	assignment.Deadline = assignment.ForwardDeadline
	return configurationRecoveryFixture{
		assignment: assignment, forward: plan.Steps[0],
		probe: plan.Steps[len(plan.Steps)-2], compensate: plan.Steps[len(plan.Steps)-1],
		prior: prior, next: next,
	}
}

func recoveryMaterializationStep(
	artifact *agentpb.ComposeArtifact,
	stepID string,
	materializationID string,
	content []byte,
	policy agentpb.ExecutionStepPolicy,
) *agentpb.ExecutionStep {
	digest := sha256.Sum256(content)
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 30, Policy: policy,
		Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: &agentpb.MaterializeFile{
			ArtifactId: artifact.ArtifactId, MaterializationId: materializationID,
			EnvironmentId: artifact.OwnerId, Destination: "config/application.yaml",
			OutputKind: agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
			Uid:        1000, Gid: 1000, Mode: 0o444, Length: uint64(len(content)), Sha256: digest[:],
		}},
	}
}

// SVC-15/JOURNEY-02: acknowledgement of Running is the durable boundary that
// makes even a failed, partially applied file write eligible for exact recovery.
func TestReleaseConfigurationForwardFailureWaitsForRunningAndDefersNativeCompensation(t *testing.T) {
	fixture := newConfigurationRecoveryFixture(t)
	helper := &recoveryMaterializationHelper{failNextWrite: true}
	pool := configurationRecoveryPool(t, fixture, helper)
	if err := pool.materializations.Register(fixture.assignment); err != nil {
		t.Fatal(err)
	}
	acceptRecoveryContent(t, pool, fixture.assignment, fixture.forward.StepId, fixture.next)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reservation := &taskReservation{
		assignment: fixture.assignment, ctx: ctx, cancel: cancel, eventsDurable: true,
	}
	go pool.executeRelease(context.Background(), reservation)
	running := <-pool.Outputs()
	if running.Progress == nil || running.Progress.StepID != fixture.forward.StepId ||
		running.Progress.State != TaskProgressRunning {
		t.Fatalf("first output = %#v, want file Running", running)
	}
	if len(helper.calls) != 0 {
		t.Fatal("file helper ran before durable Running acknowledgement")
	}
	ack := &agentpb.TaskEventAck{
		TaskId: running.Progress.TaskID, AssignmentId: running.Progress.AssignmentID,
		PlanHash: running.Progress.PlanHash[:], StepId: running.Progress.StepID,
		ExecutionEpoch: running.Progress.ExecutionEpoch, Ordinal: 1,
		State: agentpb.TaskState_TASK_STATE_RUNNING,
	}
	if err := pool.AcceptTaskEventAck(context.Background(), ack); err != nil {
		t.Fatal(err)
	}
	result := nextReleaseResult(t, pool)
	if result.Terminal != TaskTerminalFailed || !result.Compose.GetReconciliationRequired() {
		t.Fatalf("partial file result = %#v", result)
	}
	if !bytes.Equal(helper.state.content, fixture.next) || len(helper.calls) != 1 {
		t.Fatalf("partial write/state = %#v/%q", helper.calls, helper.state.content)
	}
	recovery := recoveryAssignment(fixture, agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE)
	if err := pool.materializations.Register(recovery); err != nil {
		t.Fatalf("register forward-to-recovery transition: %v", err)
	}
	acceptRecoveryContent(t, pool, recovery, fixture.probe.StepId, fixture.prior)
	acceptRecoveryContent(t, pool, recovery, fixture.compensate.StepId, fixture.prior)
	result = executeConfigurationRecovery(t, pool, recovery)
	if result.Terminal != TaskTerminalCompleted || !bytes.Equal(helper.state.content, fixture.prior) {
		t.Fatalf("partial write recovery result/state = %#v/%#v", result, helper.state)
	}
}

// SVC-15/JOURNEY-02: recovery restores the sealed prior bytes and metadata,
// verifies them independently, and can re-probe after an assignment reconnect.
func TestReleaseConfigurationRecoveryRestoresAndReprobesReusableSource(t *testing.T) {
	fixture := newConfigurationRecoveryFixture(t)
	helper := &recoveryMaterializationHelper{state: recoveryFileState{
		present: true, content: bytes.Clone(fixture.next), uid: 1000, gid: 1000, mode: 0o444,
	}}
	pool := configurationRecoveryPool(t, fixture, helper)
	recovery := recoveryAssignment(fixture, agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE)
	if err := pool.materializations.Register(recovery); err != nil {
		t.Fatal(err)
	}
	acceptRecoveryContent(t, pool, recovery, fixture.probe.StepId, fixture.prior)
	acceptRecoveryContent(t, pool, recovery, fixture.compensate.StepId, fixture.prior)
	result := executeConfigurationRecovery(t, pool, recovery)
	if result.Terminal != TaskTerminalCompleted || result.Compose.GetReconciliationRequired() ||
		!bytes.Equal(helper.state.content, fixture.prior) || helper.state.uid != 1000 ||
		helper.state.gid != 1000 || helper.state.mode != 0o444 {
		t.Fatalf("recovery result/state = %#v/%#v", result, helper.state)
	}
	wantCalls := []recoveryMaterializationCall{
		{stepID: fixture.probe.StepId, verifyOnly: true},
		{stepID: fixture.compensate.StepId},
		{stepID: fixture.compensate.StepId, verifyOnly: true},
	}
	if !slices.Equal(helper.calls, wantCalls) {
		t.Fatalf("materialization calls = %#v, want %#v", helper.calls, wantCalls)
	}

	proof := recoveryAssignment(fixture, agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN)
	if err := pool.materializations.Register(proof); err != nil {
		t.Fatalf("register proof reconnect: %v", err)
	}
	acceptRecoveryContent(t, pool, proof, fixture.probe.StepId, fixture.prior)
	result = executeConfigurationRecovery(t, pool, proof)
	if result.Terminal != TaskTerminalCompleted || result.Compose.GetReconciliationRequired() {
		t.Fatalf("proven re-probe result = %#v", result)
	}
}

// SVC-15/JOURNEY-02: reconnect preserves the fixed attempt deadline. Only the
// next recovery epoch may use the recovery deadline captured by the original
// assignment; a later timestamp cannot extend stale file-write authority.
func TestReleaseConfigurationReplayRejectsDeadlineAndEpochDrift(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"same attempt extended", "recovery extended", "recovery source changed", "epoch skipped", "valid recovery"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newConfigurationRecoveryFixture(t)
			inbox := filematerialization.NewInbox()
			defer inbox.Release(fixture.assignment.TaskID)
			if err := inbox.Register(fixture.assignment); err != nil {
				t.Fatal(err)
			}
			inbox.Retire(fixture.assignment.TaskID)
			next := recoveryAssignment(fixture, agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE)
			switch scenario {
			case "same attempt extended":
				next = fixture.assignment
				next.Deadline = next.Deadline.Add(time.Minute)
			case "recovery extended":
				next.Deadline = next.Deadline.Add(time.Minute)
			case "recovery source changed":
				next.RecoveryDeadline = next.RecoveryDeadline.Add(time.Minute)
			case "epoch skipped":
				next.ExecutionEpoch++
			}
			err := inbox.Register(next)
			if (err == nil) != (scenario == "valid recovery") {
				t.Fatalf("replay admission: %v", err)
			}
		})
	}
}

type sequencedReleaseHelper struct {
	responses map[string][]*agentpb.ComposeHelperResponse
	order     *[]string
}

func (helper *sequencedReleaseHelper) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	if helper.order != nil {
		*helper.order = append(*helper.order, "native:"+request.GetStepId())
	}
	responses := helper.responses[request.GetStepId()]
	if len(responses) == 0 {
		return nil, errs.New(errs.KindInternal, "unexpected native recovery call")
	}
	helper.responses[request.GetStepId()] = responses[1:]
	return proto.CloneOf(responses[0]), nil
}

// SVC-15/JOURNEY-02: native health observed before file restoration is
// transient evidence. The Agent re-probes after restoring files and does not
// execute an otherwise-applicable native compensation when health is restored.
func TestReleaseConfigurationRecoveryReprobesNativeAfterFileRestore(t *testing.T) {
	fixture := newConfigurationRecoveryFixture(t)
	order := []string{}
	fileHelper := &recoveryMaterializationHelper{state: recoveryFileState{
		present: true, content: bytes.Clone(fixture.next), uid: 1000, gid: 1000, mode: 0o444,
	}, order: &order}
	materializer, err := filematerialization.New(fileHelper, nil)
	if err != nil {
		t.Fatal(err)
	}
	nativeProbe := releaseProbe(ids.New(ids.KindStep))
	nativeCompensate := releaseCompensate(ids.New(ids.KindStep), ids.New(ids.KindStep))
	fixture.assignment.Plan.Artifacts = append(
		fixture.assignment.Plan.Artifacts,
		releaseTestArtifact("candidate-artifact"),
		releaseTestArtifact("prior-artifact"),
	)
	nativeCompensate.GetServiceProxyCompensate().PriorTarget = nativeProbe.GetServiceProxyProbe().ExpectedTarget
	nativeCompensate.GetServiceProxyCompensate().PriorReleaseId = nativeProbe.GetServiceProxyProbe().ReleaseId
	procedure := fixture.assignment.Plan.CandidateReleaseProcedure
	procedure.Members = []*agentpb.CandidateReleaseMember{{
		ServiceId: "api", CandidateReleaseId: "release-api", CandidateArtifactId: "candidate-artifact",
		ServingPredecessor: &agentpb.ServingPredecessorRestoration{
			ProbeStepId: nativeProbe.StepId, CompensateStepId: nativeCompensate.StepId,
		},
		CandidateAbsence: &agentpb.CandidateAbsenceRestoration{
			ProbeStepId: nativeProbe.StepId, CompensateStepId: nativeCompensate.StepId,
		},
	}}
	fixture.assignment.Plan.Steps = append(
		fixture.assignment.Plan.Steps,
		nativeProbe,
		nativeCompensate,
	)
	candidate := releaseExecutionSuccess("api", false)
	prior := releaseExecutionSuccess("api", false)
	prior.ProxyEvidence.Target = nativeProbe.GetServiceProxyProbe().ExpectedTarget
	prior.ProxyEvidence.ReleaseId = nativeProbe.GetServiceProxyProbe().ReleaseId
	nativeHelper := &sequencedReleaseHelper{responses: map[string][]*agentpb.ComposeHelperResponse{
		nativeProbe.StepId: {candidate, prior},
	}, order: &order}
	compose, err := testcomposeruntime.New(nativeHelper, releaseExecutionObserver{})
	if err != nil {
		t.Fatal(err)
	}
	pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
	pool.materializer, pool.compose = materializer, compose
	assignment := recoveryAssignment(fixture, agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE)
	assignment.ReleaseRecoveryDirective.ApplicableCompensationStepIds = []string{
		fixture.compensate.StepId,
		nativeCompensate.StepId,
	}
	if err := pool.materializations.Register(assignment); err != nil {
		t.Fatal(err)
	}
	acceptRecoveryContent(t, pool, assignment, fixture.probe.StepId, fixture.prior)
	acceptRecoveryContent(t, pool, assignment, fixture.compensate.StepId, fixture.prior)
	result := executeConfigurationRecovery(t, pool, assignment)
	if result.Terminal != TaskTerminalCompleted || result.Compose.GetReconciliationRequired() {
		t.Fatalf("native re-probe result = %#v", result)
	}
	want := []string{
		"file:" + fixture.probe.StepId + ":verify",
		"native:" + nativeProbe.StepId,
		"file:" + fixture.compensate.StepId + ":write",
		"file:" + fixture.compensate.StepId + ":verify",
		"native:" + nativeProbe.StepId,
	}
	if !slices.Equal(order, want) {
		t.Fatalf("recovery order = %v, want %v", order, want)
	}
}

// SVC-15/JOURNEY-02: host drift only reports that a recorded compensation is
// required. It cannot create applicability, and helper failure cannot prove recovery.
func TestReleaseConfigurationRecoveryFailsClosedWithoutApplicabilityOrProof(t *testing.T) {
	for _, test := range []struct {
		name       string
		applicable bool
		failVerify bool
	}{
		{name: "drift without durable applicability"},
		{name: "verification helper failure", applicable: true, failVerify: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newConfigurationRecoveryFixture(t)
			helper := &recoveryMaterializationHelper{state: recoveryFileState{
				present: true, content: bytes.Clone(fixture.next), uid: 1000, gid: 1000, mode: 0o444,
			}, failVerifyAt: map[bool]int{false: 0, true: 2}[test.failVerify]}
			pool := configurationRecoveryPool(t, fixture, helper)
			assignment := recoveryAssignment(fixture, agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE)
			if !test.applicable {
				assignment.ReleaseRecoveryDirective.ApplicableCompensationStepIds = nil
			}
			if err := pool.materializations.Register(assignment); err != nil {
				t.Fatal(err)
			}
			acceptRecoveryContent(t, pool, assignment, fixture.probe.StepId, fixture.prior)
			if test.applicable {
				acceptRecoveryContent(t, pool, assignment, fixture.compensate.StepId, fixture.prior)
			}
			result := executeConfigurationRecovery(t, pool, assignment)
			want := fixture.next
			if test.applicable {
				want = fixture.prior
			}
			if result.Terminal != TaskTerminalFailed || !result.Compose.GetReconciliationRequired() ||
				!bytes.Equal(helper.state.content, want) {
				t.Fatalf("failed-closed result/state = %#v/%#v", result, helper.state)
			}
		})
	}
}

func TestReleaseConfigurationRecoveryRejectsUnsafePath(t *testing.T) {
	ordinary := materializationAssignment(t, []byte("content\n"))
	changed := proto.CloneOf(ordinary.Plan)
	changed.Steps[0].GetMaterializeFile().Destination = "../outside"
	changed.PlanHash = nil
	if _, err := executionplan.Seal(changed); err == nil {
		t.Fatal("unsafe recovery destination was sealed")
	}
}

func configurationRecoveryPool(
	t *testing.T,
	_ configurationRecoveryFixture,
	helper *recoveryMaterializationHelper,
) *WorkerPool {
	t.Helper()
	materializer, err := filematerialization.New(helper, nil)
	if err != nil {
		t.Fatal(err)
	}
	compose, err := testcomposeruntime.New(completedComposeHelper(), releaseExecutionObserver{})
	if err != nil {
		t.Fatal(err)
	}
	pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
	pool.materializer, pool.compose = materializer, compose
	return pool
}

func recoveryAssignment(
	fixture configurationRecoveryFixture,
	phase agentpb.ReleaseRecoveryPhase,
) testtaskassignment.Assignment {
	assignment := fixture.assignment
	assignment.ExecutionMode = agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY
	assignment.ExecutionEpoch++
	assignment.Deadline = assignment.RecoveryDeadline
	stepIDs := executionplan.RecoveryStepIDs(assignment.Plan.CandidateReleaseProcedure)
	cursor := uint32(0)
	if phase == agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN {
		cursor = uint32(len(stepIDs))
	}
	assignment.ReleaseRecoveryDirective = &agentpb.ReleaseRecoveryDirective{
		StepIds: stepIDs, Cursor: cursor, Phase: phase,
		ApplicableCompensationStepIds: []string{fixture.compensate.StepId},
	}
	return assignment
}

func acceptRecoveryContent(
	t *testing.T,
	pool *WorkerPool,
	assignment testtaskassignment.Assignment,
	stepID string,
	content []byte,
) {
	t.Helper()
	stepIndex := -1
	for index, step := range assignment.Plan.Steps {
		if step.StepId == stepID {
			stepIndex = index
			break
		}
	}
	if stepIndex < 0 {
		t.Fatalf("materialization step %s not found", stepID)
	}
	for _, transfer := range materializationTransfersForStep(assignment, stepIndex, content, 7) {
		if err := pool.AcceptMaterializationTransfer(context.Background(), transfer); err != nil {
			t.Fatalf("accept %s: %v", stepID, err)
		}
	}
}

func executeConfigurationRecovery(t *testing.T, pool *WorkerPool, assignment testtaskassignment.Assignment) TaskResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	reservation := &taskReservation{assignment: assignment, ctx: ctx, cancel: cancel}
	pool.executeRelease(context.Background(), reservation)
	return nextReleaseResult(t, pool)
}

func nextReleaseResult(t *testing.T, pool *WorkerPool) TaskResult {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case output := <-pool.Outputs():
			if output.Result != nil {
				return *output.Result
			}
		case <-deadline:
			t.Fatal("timed out waiting for release result")
			return TaskResult{}
		}
	}
}

var _ filematerialization.Helper = (*recoveryMaterializationHelper)(nil)
