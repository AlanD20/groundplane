package gitdelivery

import (
	"context"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const dispatchDiscoveryTimeout = 24 * time.Hour

// Advance performs the next recoverable step in a captured deployment.
func (service *Service) Advance(ctx context.Context, id string) (Run, error) {
	if ids.Validate(ids.KindOperation, id) != nil {
		return Run{}, errs.New(errs.KindValidationFailed, "invalid GitHub deployment identity")
	}
	view, err := service.repository.run(ctx, id)
	if err != nil {
		return Run{}, err
	}
	run := view.run
	if run.Phase.terminal() {
		return run, nil
	}
	if run.Phase.activationOwned() && (!view.isLatest || view.lockID != id || view.lock <= 0) {
		return Run{}, errs.New(errs.KindInternal, "GitHub deployment lost its activation claim")
	}
	if !view.isLatest {
		return service.change(ctx, view, Superseded, "intent_changed")
	}
	switch run.Phase {
	case Queued:
		return service.dispatch(ctx, view)
	case Dispatching:
		return service.findDispatch(ctx, view)
	case Building:
		return service.inspectBuild(ctx, view)
	case FetchPending:
		return service.fetch(ctx, view)
	case Fetching:
		return service.observeTask(ctx, view, TaskFetch)
	case DeployPending:
		return service.deploy(ctx, view)
	case Deploying:
		return service.observeTask(ctx, view, TaskDeploy)
	}
	return run, nil
}

func (service *Service) dispatch(ctx context.Context, view runView) (Run, error) {
	run := view.run
	if err := service.github.VerifyWorkflow(ctx, run.Binding); err != nil {
		return service.rejectOrRetry(ctx, view, "workflow_changed", err)
	}
	guards, err := service.accessConditions(ctx, run.Binding, 0)
	if err != nil {
		return service.rejectOrRetry(ctx, view, "authorization_changed", err)
	}
	// Only the winner of this transition can issue the HTTP mutation. Discovery
	// recovers a lost response or restart without sending a second dispatch.
	run.Phase = Dispatching
	run.UpdatedAt = maxTime(run.UpdatedAt, service.now().UTC())
	if _, err := service.repository.updateRun(ctx, view, run, guards); err != nil {
		return Run{}, err
	}
	current, err := service.repository.run(ctx, run.ID)
	if err != nil {
		return Run{}, err
	}
	if !current.isLatest || current.run.Phase != Dispatching {
		return current.run, nil
	}
	observed, err := service.github.Dispatch(ctx, current.run)
	if err != nil {
		if errs.IsRetryable(err) {
			return current.run, err
		}
		// Unless the provider supplied a documented definitive rejection, the
		// outcome is ambiguous even for a client-side error.
		return service.change(ctx, current, Dispatching, "dispatch_uncertain")
	}
	if observed.ID <= 0 {
		return service.change(ctx, current, Dispatching, "dispatch_uncertain")
	}
	if observed.Attempt == 0 {
		observed.Attempt = 1
	}
	return service.captureDispatch(ctx, current, observed)
}

func (service *Service) findDispatch(ctx context.Context, view runView) (Run, error) {
	observed, found, err := service.github.FindDispatch(ctx, view.run)
	if err != nil {
		return service.rejectOrRetry(ctx, view, "dispatch_unverifiable", err)
	}
	if !found {
		if service.now().Sub(view.run.UpdatedAt) >= dispatchDiscoveryTimeout {
			return service.change(ctx, view, Attention, "dispatch_unconfirmed")
		}
		return view.run, nil
	}
	if !matchesWorkflow(view.run, observed) {
		return service.change(ctx, view, Failed, "workflow_identity_mismatch")
	}
	return service.captureDispatch(ctx, view, observed)
}

func (service *Service) captureDispatch(ctx context.Context, view runView, observed WorkflowRun) (Run, error) {
	if observed.ID <= 0 || observed.Attempt != 1 {
		return service.change(ctx, view, Failed, "workflow_identity_mismatch")
	}
	run := view.run
	run.GitHubRunID = observed.ID
	run.GitHubRunAttempt = observed.Attempt
	run.GitHubURL = workflowURL(observed.URL, observed.ID)
	return service.save(ctx, view, run, Building, "")
}

func (service *Service) inspectBuild(ctx context.Context, view runView) (Run, error) {
	run := view.run
	observed, err := service.github.Inspect(ctx, run)
	if err != nil {
		return service.rejectOrRetry(ctx, view, "workflow_unverifiable", err)
	}
	if !matchesWorkflow(run, observed) || observed.ID != run.GitHubRunID || observed.Attempt != run.GitHubRunAttempt {
		return service.change(ctx, view, Failed, "workflow_identity_mismatch")
	}
	switch observed.State {
	case WorkflowPending:
		return run, nil
	case WorkflowFailed:
		return service.change(ctx, view, Failed, "build_failed")
	case WorkflowSucceeded:
		return service.acceptBuild(ctx, view, observed)
	default:
		return Run{}, errs.New(errs.KindInternal, "invalid GitHub workflow state")
	}
}

func (service *Service) acceptBuild(ctx context.Context, view runView, observed WorkflowRun) (Run, error) {
	run := view.run
	evidence, err := service.github.BuildResult(ctx, run)
	if err != nil {
		return service.rejectOrRetry(ctx, view, "build_result_unverifiable", err)
	}
	if evidence.RunID != run.GitHubRunID || evidence.RunAttempt != run.GitHubRunAttempt ||
		evidence.RequestID != run.ID || evidence.SourceCommit != run.Commit ||
		!validDigest(evidence.ImageDigest) || !slices.Contains(run.EligibleRunners, evidence.RunnerID) ||
		(run.Binding.RunnerGroupID != 0 && run.Binding.RunnerGroupID != evidence.RunnerGroup) {
		return service.change(ctx, view, Failed, "build_result_mismatch")
	}
	if _, err := service.access.Recheck(ctx, run.Binding, evidence.RunnerID); err != nil {
		return service.rejectOrRetry(ctx, view, "authorization_changed", err)
	}
	run.ImageDigest = evidence.ImageDigest
	run.GitHubRunnerID = evidence.RunnerID
	run.GitHubURL = workflowURL(observed.URL, observed.ID)
	phase := FetchPending
	if run.Binding.RequireApproval {
		phase = AwaitingApproval
	}
	return service.save(ctx, view, run, phase, "")
}

func (service *Service) fetch(ctx context.Context, view runView) (Run, error) {
	run := view.run
	authority, err := service.nativeAuthority(ctx, view)
	if err != nil {
		return service.rejectOrRetry(ctx, view, "authorization_changed", err)
	}
	taskID, err := service.releases.Fetch(ctx, FetchInput{
		Target: run.Binding.Target, ImageRepository: run.ImageRepository(), Digest: run.ImageDigest,
		RequestID: run.ID, Authority: authority,
	})
	if err != nil {
		return service.pendingNativeAdmission(ctx, view, "fetch_admission_blocked", err)
	}
	run.FetchTaskID = taskID
	return service.save(ctx, view, run, Fetching, "")
}

func (service *Service) deploy(ctx context.Context, view runView) (Run, error) {
	run := view.run
	input := DeployInput{
		Target: run.Binding.Target, ImageRepository: run.ImageRepository(), Digest: run.ImageDigest,
		FetchTaskID: run.FetchTaskID, RequestID: run.ID, Policy: run.Policy,
		Fence: condition(lockKey(run.Binding.ServiceID), view.lock),
	}
	authority, err := service.nativeAuthority(ctx, view)
	if err != nil {
		return service.reconcileDeploy(ctx, view, input, "authorization_changed", authorityRejected(err), err)
	}
	input.Authority = authority
	taskID, err := service.releases.Deploy(ctx, input)
	if err != nil {
		return service.reconcileDeploy(ctx, view, input, "deploy_admission_blocked", deployRejected(err), err)
	}
	return service.captureDeployTask(ctx, view, taskID)
}

func (service *Service) captureDeployTask(ctx context.Context, view runView, taskID string) (Run, error) {
	run := view.run
	run.DeployTaskID = taskID
	return service.save(ctx, view, run, Deploying, "")
}

func (service *Service) reconcileDeploy(
	ctx context.Context,
	view runView,
	input DeployInput,
	code string,
	rejected bool,
	cause error,
) (Run, error) {
	if ctx.Err() != nil {
		return view.run, cause
	}
	taskID, absent, err := service.releases.DeployReceipt(ctx, input)
	if err != nil {
		return service.pendingNativeAdmission(ctx, view, "deploy_admission_unverifiable", err)
	}
	if taskID != "" {
		return service.captureDeployTask(ctx, view, taskID)
	}
	if !rejected {
		return service.pendingNativeAdmission(ctx, view, code, cause)
	}
	if absent.Key == "" || absent.Prefix || absent.ModRevision != 0 {
		return view.run, errs.New(errs.KindInternal, "native deployment receipt lacks an exact absence condition")
	}
	run := view.run
	run.Phase = Failed
	run.ErrorCode = code
	run.UpdatedAt = maxTime(run.UpdatedAt, service.now().UTC())
	// Native admission compares the activation claim. Releasing the claim
	// with the receipt's absence CAS ensures at most one side can commit.
	return service.repository.updateRun(ctx, view, run, []keyvalue.Condition{absent})
}

func (service *Service) nativeAuthority(ctx context.Context, view runView) ([]keyvalue.Condition, error) {
	run := view.run
	guards, err := service.accessConditions(ctx, run.Binding, run.GitHubRunnerID)
	if err != nil {
		return nil, err
	}
	return append(slices.Clone(guards),
		condition(headKey(run.Binding.ServiceID), view.head),
		condition(bindingKey(run.Binding.ServiceID), run.BindingRevision),
	), nil
}

func (service *Service) accessConditions(
	ctx context.Context,
	binding Binding,
	runnerID int64,
) ([]keyvalue.Condition, error) {
	guards, err := service.access.Recheck(ctx, binding, runnerID)
	if err != nil {
		return nil, err
	}
	if len(guards) == 0 {
		return nil, errs.New(errs.KindInternal, "GitHub deployment authorization lacks durable conditions")
	}
	return guards, nil
}

func (service *Service) observeTask(ctx context.Context, view runView, kind TaskKind) (Run, error) {
	run := view.run
	input := TaskInput{
		Kind: kind, Target: run.Binding.Target, TaskID: run.FetchTaskID,
		RequestID: run.ID, ImageRepository: run.ImageRepository(), Digest: run.ImageDigest,
	}
	if kind == TaskDeploy {
		input.TaskID = run.DeployTaskID
		input.FetchTaskID = run.FetchTaskID
	}
	result, err := service.releases.Task(ctx, input)
	if err != nil {
		return service.pendingNativeAdmission(ctx, view, string(kind)+"_task_unverifiable", err)
	}
	switch result.State {
	case TaskPending:
		return service.change(ctx, view, run.Phase, "")
	case TaskAttention:
		return service.change(ctx, view, run.Phase, string(kind)+"_task_needs_attention")
	case TaskFailed:
		return service.change(ctx, view, Failed, string(kind)+"_failed")
	case TaskSucceeded:
		if kind == TaskFetch {
			return service.change(ctx, view, DeployPending, "")
		}
		run.ReleaseID = result.ReleaseID
		return service.save(ctx, view, run, Succeeded, "")
	default:
		return Run{}, errs.New(errs.KindInternal, "invalid native release Task state")
	}
}

func (service *Service) pendingNativeAdmission(ctx context.Context, view runView, code string, err error) (Run, error) {
	if ctx.Err() != nil || errs.IsRetryable(err) {
		return view.run, err
	}
	// The RequestID may already have been accepted. Keep its existing claim
	// until the native Task can be identified and its outcome verified.
	return service.change(ctx, view, view.run.Phase, code)
}

func (service *Service) rejectOrRetry(ctx context.Context, view runView, code string, err error) (Run, error) {
	if ctx.Err() == nil && authorityRejected(err) {
		return service.change(ctx, view, Failed, code)
	}
	return view.run, err
}

func authorityRejected(err error) bool {
	kind, _ := errs.KindOf(err)
	switch kind {
	case errs.KindRequestForbidden, errs.KindScopeUnauthorized, errs.KindStateConflict,
		errs.KindValidationFailed, errs.KindRequestNotFound, errs.KindRunnerNotFound,
		errs.KindTenantNotFound, errs.KindProjectNotFound, errs.KindEnvironmentNotFound, errs.KindServiceNotFound:
		return true
	default:
		return false
	}
}

func deployRejected(err error) bool {
	kind, _ := errs.KindOf(err)
	switch kind {
	case errs.KindRequestForbidden, errs.KindScopeUnauthorized, errs.KindValidationFailed, errs.KindTaskNotFound,
		errs.KindRequestNotFound, errs.KindAgentNotFound, errs.KindTenantNotFound, errs.KindProjectNotFound,
		errs.KindEnvironmentNotFound, errs.KindServiceNotFound, errs.KindStrategyNotImplemented:
		return true
	default:
		return false
	}
}

func (service *Service) change(ctx context.Context, view runView, phase Phase, code string) (Run, error) {
	if view.run.Phase == phase && view.run.ErrorCode == code {
		return view.run, nil
	}
	return service.save(ctx, view, view.run, phase, code)
}

func (service *Service) save(ctx context.Context, view runView, run Run, phase Phase, code string) (Run, error) {
	run.Phase = phase
	run.ErrorCode = code
	run.UpdatedAt = maxTime(run.UpdatedAt, service.now().UTC())
	return service.repository.updateRun(ctx, view, run, nil)
}

func maxTime(existing, current time.Time) time.Time {
	if current.Before(existing) {
		return existing
	}
	return current
}

func matchesWorkflow(run Run, observed WorkflowRun) bool {
	return observed.WorkflowID == run.Binding.WorkflowID && observed.HeadSHA == run.Binding.WorkflowCommit &&
		observed.Event == "workflow_dispatch" && observed.Title == BuildJobName+" "+run.ID
}

func workflowURL(value string, runID int64) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" {
		return ""
	}
	pieces := strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/")
	if len(pieces) != 5 || pieces[0] == "" || pieces[1] == "" || pieces[2] != "actions" ||
		pieces[3] != "runs" || pieces[4] != strconv.FormatInt(runID, 10) {
		return ""
	}
	return parsed.String()
}
