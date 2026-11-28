package gitdelivery

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/gitselector"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GitHub reads and dispatches with the exact connection and generation in
// Binding. Providers must never dispatch using credentials for another binding.
type GitHub interface {
	ResolveSource(ctx context.Context, binding Binding) (string, error)
	VerifyWorkflow(ctx context.Context, binding Binding) error
	Dispatch(ctx context.Context, run Run) (WorkflowRun, error)
	FindDispatch(ctx context.Context, run Run) (WorkflowRun, bool, error)
	Inspect(ctx context.Context, run Run) (WorkflowRun, error)
	BuildResult(ctx context.Context, run Run) (BuildEvidence, error)
}

// Access captures the repository-visible and scope-authorized Runner identities.
// The implementation resolves hierarchy, connection, binding and Runner authority
// server-side. Recheck must also reject a locally revoked connection generation.
type Access interface {
	Capture(ctx context.Context, binding Binding) ([]int64, error)
	Recheck(ctx context.Context, binding Binding, runnerID int64) ([]keyvalue.Condition, error)
}

// RunnerDefaultResolver resolves an inherited preference and its saved revision
// before acceptance. Already accepted runs retain their captured selector.
type RunnerDefaultResolver interface {
	ResolveRunnerDefault(context.Context, Binding) (gitselector.Selector, []keyvalue.Condition, error)
}

func (service *Service) resolveRunnerSelector(
	ctx context.Context,
	binding Binding,
) (Binding, []keyvalue.Condition, error) {
	if !binding.InheritRunnerDefault {
		return binding, nil, nil
	}
	resolver, ok := service.access.(RunnerDefaultResolver)
	if !ok {
		return Binding{}, nil, errs.New(errs.KindInternal, "repository Runner default resolver is unavailable")
	}
	selector, guards, err := resolver.ResolveRunnerDefault(ctx, binding)
	if err != nil {
		return Binding{}, nil, err
	}
	if !selector.Valid() || len(guards) == 0 {
		return Binding{}, nil, errs.New(errs.KindInternal, "repository Runner default resolution is incomplete")
	}
	binding.RunnerLabel, binding.RunnerGroup, binding.RunnerGroupID = selector.Label, selector.Group, selector.GroupID
	return binding, guards, nil
}

type WorkflowRun struct {
	ID         int64
	WorkflowID int64
	Attempt    int64
	HeadSHA    string
	Event      string
	Title      string
	URL        string
	State      WorkflowState
}

type WorkflowState string

const (
	WorkflowPending   WorkflowState = "pending"
	WorkflowSucceeded WorkflowState = "succeeded"
	WorkflowFailed    WorkflowState = "failed"
)

type BuildEvidence struct {
	RunID        int64
	RunAttempt   int64
	RunnerID     int64
	RunnerGroup  int64
	RequestID    string
	SourceCommit string
	ImageDigest  string
}

type FetchInput struct {
	Target          Target
	ImageRepository string
	Digest          string
	RequestID       string
	Authority       []keyvalue.Condition
}

type DeployInput struct {
	Target          Target
	ImageRepository string
	Digest          string
	FetchTaskID     string
	RequestID       string
	Fence           keyvalue.Condition
	Authority       []keyvalue.Condition
	Policy          RolloutPolicy
}

type TaskKind string

const (
	TaskFetch  TaskKind = "fetch"
	TaskDeploy TaskKind = "deploy"
)

type TaskState string

const (
	TaskPending   TaskState = "pending"
	TaskSucceeded TaskState = "succeeded"
	TaskFailed    TaskState = "failed"
	TaskAttention TaskState = "attention_required"
)

type TaskInput struct {
	Kind            TaskKind
	Target          Target
	TaskID          string
	RequestID       string
	ImageRepository string
	Digest          string
	FetchTaskID     string
}

type TaskResult struct {
	State     TaskState
	ReleaseID string
}

// Releases publishes ordinary native Tasks. Implementations must protect
// replay using RequestID and include the supplied Fence in Deploy admission.
// TaskSucceeded and TaskFailed mean a fully settled Task. Pending recovery
// retains the deployment hold until the original Task settles.
type Releases interface {
	Policy(context.Context, Target) (RolloutPolicy, []keyvalue.Condition, error)
	Fetch(context.Context, FetchInput) (string, error)
	Deploy(context.Context, DeployInput) (string, error)
	// DeployReceipt gets an accepted Task via exact native replay. A missing
	// receipt returns an exact absence condition for safely retiring the claim.
	DeployReceipt(context.Context, DeployInput) (string, keyvalue.Condition, error)
	Task(context.Context, TaskInput) (TaskResult, error)
}

type Service struct {
	repository *Repository
	github     GitHub
	access     Access
	releases   Releases
	now        func() time.Time
}

type BindingEffects = func(Binding) ([]keyvalue.Condition, []keyvalue.Mutation, error)
type RunEffects = func(Run) ([]keyvalue.Condition, []keyvalue.Mutation, error)

func NewService(repository *Repository, github GitHub, access Access, releases Releases) (*Service, error) {
	if repository == nil || github == nil || access == nil || releases == nil {
		return nil, errs.New(errs.KindInternal, "GitHub delivery dependencies are incomplete")
	}
	return &Service{repository: repository, github: github, access: access, releases: releases, now: time.Now}, nil
}

func (service *Service) Bind(
	ctx context.Context,
	binding Binding,
	expectedRevision int64,
	effects ...BindingEffects,
) (BindingSnapshot, error) {
	if err := binding.Validate(); err != nil {
		return BindingSnapshot{}, err
	}
	if expectedRevision < 0 {
		return BindingSnapshot{}, errs.New(errs.KindValidationFailed, "GitHub binding revision is invalid")
	}
	if expectedRevision != 0 {
		current, err := service.GetBinding(ctx, binding.ServiceID)
		if err != nil {
			return BindingSnapshot{}, err
		}
		if current.Revision != expectedRevision || current.Binding.Target != binding.Target {
			return BindingSnapshot{}, errs.New(errs.KindStateConflict, "GitHub binding revision or owner changed")
		}
	}
	if binding.InheritRunnerDefault && binding.RunnerSelector() != (gitselector.Selector{}) {
		return BindingSnapshot{}, errs.New(
			errs.KindValidationFailed,
			"choose inheritance or an explicit Runner selector",
		)
	}
	effective, defaultGuards, err := service.resolveRunnerSelector(ctx, binding)
	if err != nil {
		return BindingSnapshot{}, err
	}
	if _, err := service.access.Capture(ctx, effective); err != nil {
		return BindingSnapshot{}, err
	}
	if err := service.github.VerifyWorkflow(ctx, binding); err != nil {
		return BindingSnapshot{}, err
	}
	guards, err := service.accessConditions(ctx, binding, 0)
	if err != nil {
		return BindingSnapshot{}, err
	}
	return service.repository.bind(ctx, binding, expectedRevision, append(guards, defaultGuards...), effects)
}

func (service *Service) GetBinding(ctx context.Context, serviceID string) (BindingSnapshot, error) {
	if err := recordcodecServiceID(serviceID); err != nil {
		return BindingSnapshot{}, err
	}
	return service.repository.binding(ctx, serviceID)
}

// Begin accepts a durable request. Automatic callers pass the observed commit
// and share the current durable head across webhook delivery and polling.
func (service *Service) Begin(
	ctx context.Context,
	serviceID string,
	input StartInput,
	effects ...RunEffects,
) (Run, error) {
	return service.beginRequest(ctx, serviceID, input, 0, effects)
}

func (service *Service) beginRequest(
	ctx context.Context,
	serviceID string,
	input StartInput,
	observedRevision int64,
	effects []RunEffects,
) (Run, error) {
	if ids.Validate(ids.KindService, serviceID) != nil ||
		(!input.Automatic && !keyPattern.MatchString(input.IdempotencyKey)) ||
		(input.ExpectedCommit != "" && !commitPattern.MatchString(input.ExpectedCommit)) {
		return Run{}, errs.New(errs.KindValidationFailed, "invalid GitHub deployment request")
	}
	captured, err := service.repository.admission(ctx, serviceID, input)
	if err != nil {
		return Run{}, err
	}
	if captured.replay != nil {
		return service.Get(ctx, serviceID, captured.replay.ID)
	}
	if observedRevision > 0 && captured.binding.Revision != observedRevision {
		return Run{}, errs.New(errs.KindStateConflict, "GitHub binding changed after observing the branch")
	}
	if input.Automatic {
		if !captured.binding.Binding.Automatic {
			return Run{}, errs.New(errs.KindRequestForbidden, "automatic GitHub deployment is disabled")
		}
		latest, err := service.repository.latestIntent(ctx, captured)
		if err != nil {
			return Run{}, err
		}
		commit, err := service.resolveObservedSource(ctx, captured.binding.Binding, input.ExpectedCommit)
		if err != nil {
			return Run{}, err
		}
		if latest.BindingRevision == captured.binding.Revision && latest.Commit == commit {
			return latest, nil
		}
		input.ExpectedCommit = commit
		input.IdempotencyKey = automaticTransitionKey(captured, commit)
	}
	if captured.lock != 0 {
		return Run{}, errs.New(errs.KindResourceInUse, "the previous GitHub deployment is still activating")
	}
	return service.begin(ctx, captured, input, effects)
}

func (service *Service) begin(
	ctx context.Context,
	captured admission,
	input StartInput,
	effects []RunEffects,
) (Run, error) {
	binding := captured.binding.Binding
	binding, defaultGuards, err := service.resolveRunnerSelector(ctx, binding)
	if err != nil {
		return Run{}, err
	}
	authorized, err := service.access.Capture(ctx, binding)
	if err != nil {
		return Run{}, err
	}
	authorized = slices.Clone(authorized)
	slices.Sort(authorized)
	if !validRunners(authorized) {
		return Run{}, errs.New(errs.KindStateConflict, "no compatible registered Runner matches the GitHub deployment")
	}
	commit := input.ExpectedCommit
	if !input.Automatic {
		commit, err = service.resolveObservedSource(ctx, binding, input.ExpectedCommit)
		if err != nil {
			return Run{}, err
		}
	}
	guards, err := service.accessConditions(ctx, binding, 0)
	if err != nil {
		return Run{}, err
	}
	policy, policyGuards, err := service.releases.Policy(ctx, binding.Target)
	if err != nil {
		return Run{}, err
	}
	if err := policy.validate(); err != nil {
		return Run{}, err
	}
	guards = append(guards, policyGuards...)
	guards = append(guards, defaultGuards...)
	at := service.now().UTC()
	run := Run{
		ID: ids.New(ids.KindOperation), Binding: binding, BindingRevision: captured.binding.Revision,
		Commit: commit, EligibleRunners: authorized, Policy: policy, Phase: Queued, CreatedAt: at, UpdatedAt: at,
	}
	if err := service.repository.createRun(ctx, captured, run, input, guards, effects); err != nil {
		return Run{}, err
	}
	return run, nil
}

func (service *Service) Get(ctx context.Context, serviceID, id string) (Run, error) {
	if ids.Validate(ids.KindService, serviceID) != nil || ids.Validate(ids.KindOperation, id) != nil {
		return Run{}, errs.New(errs.KindValidationFailed, "invalid GitHub deployment identity")
	}
	run, err := service.repository.get(ctx, id)
	if err != nil {
		return Run{}, err
	}
	if run.Binding.ServiceID != serviceID {
		return Run{}, errs.New(errs.KindRequestNotFound, "GitHub deployment not found")
	}
	return run, nil
}

func (service *Service) List(
	ctx context.Context,
	serviceID, after string,
	limit int64,
	revision ...int64,
) (RunPage, error) {
	if ids.Validate(ids.KindService, serviceID) != nil ||
		(after != "" && ids.Validate(ids.KindOperation, after) != nil) ||
		limit <= 0 ||
		limit > 64 ||
		len(revision) > 1 ||
		(len(revision) == 1 && revision[0] < 0) {
		return RunPage{}, errs.New(errs.KindValidationFailed, "invalid GitHub deployment history request")
	}
	var snapshot int64
	if len(revision) == 1 {
		snapshot = revision[0]
	}
	return service.repository.list(ctx, serviceID, after, limit, snapshot)
}

func (service *Service) Approve(ctx context.Context, serviceID, id string, effects ...RunEffects) (Run, error) {
	if ids.Validate(ids.KindService, serviceID) != nil || ids.Validate(ids.KindOperation, id) != nil {
		return Run{}, errs.New(errs.KindValidationFailed, "invalid GitHub deployment identity")
	}
	view, err := service.repository.run(ctx, id)
	if err != nil {
		return Run{}, err
	}
	if view.run.Binding.ServiceID != serviceID {
		return Run{}, errs.New(errs.KindRequestNotFound, "GitHub deployment not found")
	}
	if view.run.Phase != AwaitingApproval || !view.isLatest {
		return Run{}, errs.New(errs.KindStateConflict, "GitHub deployment no longer awaits approval")
	}
	guards, err := service.accessConditions(ctx, view.run.Binding, view.run.GitHubRunnerID)
	if err != nil {
		return Run{}, err
	}
	run := view.run
	run.Phase = FetchPending
	run.ErrorCode = ""
	run.UpdatedAt = maxTime(run.UpdatedAt, service.now().UTC())
	return service.repository.updateRun(ctx, view, run, guards, effects...)
}

// RunAll advances a bounded page before polling again. It carries no run
// state across restarts. The scheduler can replay successful admissions, and
// ambiguous provider effects stay with their original request.
func (service *Service) RunAll(ctx context.Context, interval time.Duration, logger *slog.Logger) error {
	if interval <= 0 || logger == nil {
		return errs.New(errs.KindInternal, "GitHub delivery poller requires an interval and logger")
	}
	timer := time.NewTicker(interval)
	defer timer.Stop()
	automaticContext, stopAutomatic := context.WithCancel(ctx)
	automaticDone := make(chan struct{})
	webhookDone := make(chan struct{})
	go func() {
		defer close(automaticDone)
		service.pollAutomatic(automaticContext, max(interval, minimumAutomaticInterval), logger)
	}()
	go func() {
		defer close(webhookDone)
		service.pollPushes(automaticContext, logger)
	}()
	defer func() {
		stopAutomatic()
		<-automaticDone
		<-webhookDone
	}()
	var after string
	for {
		ids, next, err := service.repository.active(ctx, after)
		if err == nil {
			after = next
			for _, id := range ids {
				if _, err := service.Advance(ctx, id); err != nil && ctx.Err() == nil {
					kind, _ := errs.KindOf(err)
					logger.DebugContext(ctx, "GitHub deployment advancement deferred", "deployment", id, "kind", kind)
				}
			}
		} else if ctx.Err() == nil {
			kind, _ := errs.KindOf(err)
			logger.WarnContext(ctx, "GitHub deployment work could not be read", "kind", kind)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func recordcodecServiceID(value string) error {
	if err := ids.Validate(ids.KindService, value); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return nil
}
