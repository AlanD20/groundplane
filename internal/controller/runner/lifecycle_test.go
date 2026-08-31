package runner

import (
	"context"
	"errors"
	"slices"
	"testing"

	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: recovery from an issued mutation must observe its result and must
// never redispatch the host effect or reuse registration-token bytes.
func TestCreateResolvesIssuedEffectWithoutRedispatch(t *testing.T) {
	t.Parallel()
	journal := newMemoryJournal()
	runtime := &recordingRuntime{}
	lifecycle, err := NewLifecycle(journal, runtime)
	if err != nil {
		t.Fatalf("NewLifecycle() error = %v", err)
	}
	tokenBytes := []byte("github-one-use-token")
	token, err := NewRegistrationToken(tokenBytes)
	if err != nil {
		t.Fatalf("NewRegistrationToken() error = %v", err)
	}
	attempt := Attempt{TaskID: "task_create", Executor: "controller", Plan: testPlan()}
	issued := corerunner.StepStartDaemon
	journal.progress = Progress{
		RunnerID: attempt.Plan.RunnerID, TaskID: attempt.TaskID, Executor: attempt.Executor,
		PlanDigest: attempt.Plan.Digest(), IdentityDigest: attempt.Plan.IdentityDigest(), RuntimeEpoch: 1,
		Operation: OperationCreate, NextStep: 4, ActiveStep: &issued, Status: StatusRunning, Revision: 1,
	}
	if err := lifecycle.Create(context.Background(), attempt, token); err != nil {
		t.Fatalf("Create(recovery) error = %v", err)
	}
	if !allZero(tokenBytes) {
		t.Fatal("registration token source was not cleared")
	}
	if !slices.Equal(runtime.steps, []corerunner.Step{corerunner.StepStartRunner}) {
		t.Fatalf("dispatched steps = %v", runtime.steps)
	}
	if !slices.Equal(runtime.resolved, []corerunner.Step{corerunner.StepStartDaemon}) {
		t.Fatalf("resolved steps = %v", runtime.resolved)
	}
	if journal.progress.Status != StatusReady ||
		journal.progress.NextStep != len(corerunner.CreationSteps()) ||
		journal.progress.Evidence == nil {
		t.Fatalf("progress = %#v", journal.progress)
	}
}

// Rationale: Controller restart or broker corruption must expose the stable
// retry reason before a journal entry or host effect can occur.
func TestCreateWithoutUsableTokenRequiresFreshRegistrationToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		token *RegistrationToken
	}{
		{name: "missing", token: nil},
		{name: "non-nil empty", token: &RegistrationToken{}},
		{name: "invalid byte", token: &RegistrationToken{value: []byte("invalid token")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			journal := newMemoryJournal()
			runtime := &recordingRuntime{}
			lifecycle, err := NewLifecycle(journal, runtime)
			if err != nil {
				t.Fatalf("NewLifecycle() error = %v", err)
			}
			err = lifecycle.Create(
				context.Background(),
				Attempt{TaskID: "task_create", Executor: "controller", Plan: testPlan()},
				test.token,
			)
			var domainError *errs.Error
			if !errors.As(err, &domainError) || domainError.Detail != "registration_token_required" {
				t.Fatalf("Create(without usable token) error = %v", err)
			}
			if len(runtime.steps) != 0 || journal.progress.Status != "" {
				t.Fatalf("host steps = %v, progress = %#v", runtime.steps, journal.progress)
			}
			if test.token != nil && len(test.token.value) != 0 {
				t.Fatal("rejected registration token bytes were not cleared")
			}
		})
	}
}

// Rationale: cleanup must checkpoint exact absence for every effect, retain its
// fence until completion, and remove the nftables egress policy last.
func TestRemoveRecordsOrderedAbsenceReceipts(t *testing.T) {
	t.Parallel()
	journal := newMemoryJournal()
	journal.progress = Progress{
		RunnerID: testPlan().RunnerID, TaskID: "task_create", Executor: "controller",
		PlanDigest: testPlan().Digest(), IdentityDigest: testPlan().IdentityDigest(), RuntimeEpoch: 1,
		Operation: OperationCreate, NextStep: len(corerunner.CreationSteps()), Status: StatusReady, Revision: 1,
	}
	runtime := &recordingRuntime{}
	lifecycle, err := NewLifecycle(journal, runtime)
	if err != nil {
		t.Fatalf("NewLifecycle() error = %v", err)
	}
	attempt := Attempt{TaskID: "task_remove", Executor: "controller", Plan: testPlan()}
	if err := lifecycle.Remove(context.Background(), attempt); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if !slices.Equal(runtime.steps, []corerunner.Step{
		corerunner.StepStopRunner, corerunner.StepStopDaemon, corerunner.StepStopProxy,
		corerunner.StepRemoveNetwork, corerunner.StepRemoveIdentity, corerunner.StepRemoveEgress,
	}) {
		t.Fatalf("cleanup steps = %v", runtime.steps)
	}
	if !journal.released || journal.progress.Status != StatusRemoved ||
		len(journal.progress.CleanupReceipts) != len(corerunner.RemovalSteps()) {
		t.Fatalf("progress = %#v, released = %t", journal.progress, journal.released)
	}
}

// Rationale: losing the durable failure write is a separate correctness fault
// and must remain visible alongside the host-side failure.
func TestLifecycleReturnsJournalFailure(t *testing.T) {
	t.Parallel()
	hostFailure := errors.New("host result unknown")
	journalFailure := errors.New("journal unavailable")
	journal := newMemoryJournal()
	journal.failErr = journalFailure
	runtime := &recordingRuntime{applyErr: hostFailure, resolveErr: hostFailure}
	lifecycle, err := NewLifecycle(journal, runtime)
	if err != nil {
		t.Fatal(err)
	}
	token, err := NewRegistrationToken([]byte("github-one-use-token"))
	if err != nil {
		t.Fatal(err)
	}
	err = lifecycle.Create(
		context.Background(),
		Attempt{TaskID: "task_create", Executor: "controller", Plan: testPlan()},
		token,
	)
	if !errors.Is(err, hostFailure) || !errors.Is(err, journalFailure) {
		t.Fatalf("Create() error = %v", err)
	}
}

type memoryJournal struct {
	progress Progress
	released bool
	failErr  error
}

func newMemoryJournal() *memoryJournal { return &memoryJournal{} }

func (journal *memoryJournal) Begin(_ context.Context, attempt Attempt, operation Operation) (Progress, error) {
	digest := attempt.Plan.Digest()
	if journal.progress.Status == "" {
		journal.progress = Progress{
			RunnerID: attempt.Plan.RunnerID, TaskID: attempt.TaskID, Executor: attempt.Executor,
			PlanDigest: digest, IdentityDigest: attempt.Plan.IdentityDigest(), RuntimeEpoch: attempt.Plan.RuntimeEpoch,
			Operation: operation, Status: StatusRunning, Revision: 1,
		}
		return journal.progress, nil
	}
	if journal.progress.PlanDigest != digest {
		return Progress{}, errors.New("plan changed")
	}
	if journal.progress.TaskID == attempt.TaskID && journal.progress.Operation == operation {
		return journal.progress, nil
	}
	if operation == OperationRemove &&
		(journal.progress.Status == StatusReady || journal.progress.Status == StatusFailed) {
		journal.progress = Progress{
			RunnerID: attempt.Plan.RunnerID, TaskID: attempt.TaskID, Executor: attempt.Executor,
			PlanDigest: digest, IdentityDigest: attempt.Plan.IdentityDigest(), RuntimeEpoch: attempt.Plan.RuntimeEpoch,
			Operation: operation, Status: StatusRunning, Revision: journal.progress.Revision + 1,
		}
		return journal.progress, nil
	}
	return Progress{}, errors.New("operation fenced")
}

func (journal *memoryJournal) Issue(_ context.Context, current Progress, step corerunner.Step) (Progress, error) {
	current.ActiveStep = &step
	current.Revision++
	journal.progress = current
	return current, nil
}

func (journal *memoryJournal) Checkpoint(
	_ context.Context,
	current Progress,
	evidence corerunner.StepEvidence,
) (Progress, error) {
	current.NextStep++
	current.ActiveStep = nil
	if evidence.Ownership != nil {
		copy := *evidence.Ownership
		current.Evidence = &copy
	}
	if current.Operation == OperationRemove {
		current.CleanupReceipts = append(current.CleanupReceipts, evidence)
	}
	current.Revision++
	journal.progress = current
	return current, nil
}

func (journal *memoryJournal) Fail(_ context.Context, current Progress) error {
	if journal.failErr != nil {
		return journal.failErr
	}
	current.Status = StatusFailed
	current.Revision++
	journal.progress = current
	return nil
}

func (journal *memoryJournal) Ready(_ context.Context, current Progress, _ RuntimeEvidence) error {
	current.Status = StatusReady
	current.Revision++
	journal.progress = current
	return nil
}

func (journal *memoryJournal) Removed(_ context.Context, current Progress) error {
	current.Status = StatusRemoved
	current.Revision++
	journal.progress = current
	journal.released = true
	return nil
}

type recordingRuntime struct {
	steps      []corerunner.Step
	resolved   []corerunner.Step
	applyErr   error
	resolveErr error
}

func (runtime *recordingRuntime) apply(
	_ context.Context,
	_ corerunner.Plan,
	step corerunner.Step,
	token []byte,
) (corerunner.StepEvidence, error) {
	runtime.steps = append(runtime.steps, step)
	if runtime.applyErr != nil {
		return corerunner.StepEvidence{}, runtime.applyErr
	}
	if step != corerunner.StepStartRunner && len(token) != 0 {
		return corerunner.StepEvidence{}, errors.New("token escaped registration step")
	}
	if step == corerunner.StepStartRunner {
		ownership := RuntimeEvidence{
			ContainerID: repeat("a", 64), DaemonNonce: repeat("b", 64), SocketDevice: 1, SocketInode: 2,
		}
		return corerunner.StepEvidence{Step: step, State: corerunner.EffectApplied, Ownership: &ownership}, nil
	}
	if isRemovalStep(step) {
		return corerunner.StepEvidence{
			Step: step, State: corerunner.EffectAbsent, ReceiptSHA256: "sha256:" + repeat("c", 64),
		}, nil
	}
	return corerunner.StepEvidence{Step: step, State: corerunner.EffectApplied}, nil
}

func (runtime *recordingRuntime) resolve(
	_ context.Context,
	_ corerunner.Plan,
	step corerunner.Step,
) (corerunner.StepEvidence, error) {
	runtime.resolved = append(runtime.resolved, step)
	if runtime.resolveErr != nil {
		return corerunner.StepEvidence{}, runtime.resolveErr
	}
	return corerunner.StepEvidence{Step: step, State: corerunner.EffectApplied}, nil
}

func (runtime *recordingRuntime) EnsureIdentity(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepEnsureIdentity, nil)
}
func (runtime *recordingRuntime) ObserveIdentity(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.resolve(ctx, plan, corerunner.StepEnsureIdentity)
}
func (runtime *recordingRuntime) EnsureNetwork(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepEnsureNetwork, nil)
}
func (runtime *recordingRuntime) ObserveNetwork(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.resolve(ctx, plan, corerunner.StepEnsureNetwork)
}
func (runtime *recordingRuntime) EnsureEgress(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepEnsureEgress, nil)
}
func (runtime *recordingRuntime) ObserveEgress(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.resolve(ctx, plan, corerunner.StepEnsureEgress)
}
func (runtime *recordingRuntime) StartProxy(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepStartProxy, nil)
}
func (runtime *recordingRuntime) ObserveProxy(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.resolve(ctx, plan, corerunner.StepStartProxy)
}
func (runtime *recordingRuntime) StartDaemon(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepStartDaemon, nil)
}
func (runtime *recordingRuntime) ObserveDaemon(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.resolve(ctx, plan, corerunner.StepStartDaemon)
}
func (runtime *recordingRuntime) StartRunner(ctx context.Context, plan corerunner.Plan, token []byte) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepStartRunner, token)
}
func (runtime *recordingRuntime) ObserveRunner(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.resolve(ctx, plan, corerunner.StepStartRunner)
}
func (runtime *recordingRuntime) StopRunner(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepStopRunner, nil)
}
func (runtime *recordingRuntime) ObserveRunnerAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.removalObservation(ctx, plan, corerunner.StepStopRunner)
}
func (runtime *recordingRuntime) StopDaemon(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepStopDaemon, nil)
}
func (runtime *recordingRuntime) ObserveDaemonAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.removalObservation(ctx, plan, corerunner.StepStopDaemon)
}
func (runtime *recordingRuntime) StopProxy(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepStopProxy, nil)
}
func (runtime *recordingRuntime) ObserveProxyAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.removalObservation(ctx, plan, corerunner.StepStopProxy)
}
func (runtime *recordingRuntime) RemoveNetwork(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepRemoveNetwork, nil)
}
func (runtime *recordingRuntime) ObserveNetworkAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.removalObservation(ctx, plan, corerunner.StepRemoveNetwork)
}
func (runtime *recordingRuntime) RemoveIdentity(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepRemoveIdentity, nil)
}
func (runtime *recordingRuntime) ObserveIdentityAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.removalObservation(ctx, plan, corerunner.StepRemoveIdentity)
}
func (runtime *recordingRuntime) RemoveEgress(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.apply(ctx, plan, corerunner.StepRemoveEgress, nil)
}
func (runtime *recordingRuntime) ObserveEgressAbsent(ctx context.Context, plan corerunner.Plan) (corerunner.StepEvidence, error) {
	return runtime.removalObservation(ctx, plan, corerunner.StepRemoveEgress)
}
func (runtime *recordingRuntime) removalObservation(
	ctx context.Context,
	plan corerunner.Plan,
	step corerunner.Step,
) (corerunner.StepEvidence, error) {
	evidence, err := runtime.resolve(ctx, plan, step)
	if err != nil {
		return corerunner.StepEvidence{}, err
	}
	evidence.State = corerunner.EffectAbsent
	evidence.ReceiptSHA256 = "sha256:" + repeat("c", 64)
	return evidence, nil
}

func isRemovalStep(step corerunner.Step) bool {
	for _, candidate := range corerunner.RemovalSteps() {
		if candidate == step {
			return true
		}
	}
	return false
}

func testPlan() corerunner.Plan { return corerunner.Plan{RunnerID: "run_test", RuntimeEpoch: 1} }

func allZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}

func repeat(value string, count int) string {
	result := ""
	for len(result) < count {
		result += value
	}
	return result
}
