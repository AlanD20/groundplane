// Package gitdelivery coordinates a captured GitHub build with native Fetch and Deploy.
package gitdelivery

import (
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/gitselector"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	BuildJobName      = "Groundplane build"
	BuildArtifactName = "groundplane-build-result"
	maximumRunners    = 100
)

type Target struct {
	TenantID      string `json:"tenant_id"`
	ProjectID     string `json:"project_id"`
	EnvironmentID string `json:"environment_id"`
	ServiceID     string `json:"service_id"`
}

func (target Target) validate() error {
	for _, item := range []struct {
		id   string
		kind ids.Kind
	}{
		{target.TenantID, ids.KindTenant},
		{target.ProjectID, ids.KindProject},
		{target.EnvironmentID, ids.KindEnvironment},
		{target.ServiceID, ids.KindService},
	} {
		if err := recordcodec.ValidateID(item.kind, item.id); err != nil {
			return err
		}
	}
	return nil
}

type Binding struct {
	Target
	ConnectionID         string   `json:"connection_id"`
	ConnectionScopeKind  ids.Kind `json:"connection_scope_kind"`
	ConnectionScopeID    string   `json:"connection_scope_id"`
	ConnectionGeneration int64    `json:"connection_generation"`
	InstallationID       int64    `json:"installation_id"`
	RepositoryID         int64    `json:"repository_id"`
	WorkflowID           int64    `json:"workflow_id"`
	WorkflowRef          string   `json:"workflow_ref"`
	WorkflowCommit       string   `json:"workflow_commit"`
	Branch               string   `json:"branch"`
	Dockerfile           string   `json:"dockerfile"`
	BuildContext         string   `json:"build_context"`
	InheritRunnerDefault bool     `json:"inherit_runner_default,omitempty"`
	RunnerLabel          string   `json:"runner_label"`
	RunnerGroup          string   `json:"runner_group,omitempty"`
	RunnerGroupID        int64    `json:"runner_group_id,omitempty"`
	Automatic            bool     `json:"automatic"`
	RequireApproval      bool     `json:"require_approval"`
}

func (binding Binding) Validate() error {
	if err := binding.Target.validate(); err != nil {
		return err
	}
	if !namePattern.MatchString(binding.ConnectionID) || !binding.connectionScopeAllowed() ||
		binding.ConnectionGeneration <= 0 ||
		binding.InstallationID <= 0 ||
		binding.RepositoryID <= 0 ||
		binding.WorkflowID <= 0 ||
		!validBranch(binding.WorkflowRef) ||
		!commitPattern.MatchString(binding.WorkflowCommit) ||
		!validBranch(binding.Branch) ||
		!validRepoPath(binding.Dockerfile, false) ||
		!validRepoPath(binding.BuildContext, true) ||
		(!binding.RunnerSelector().Valid() && !(binding.InheritRunnerDefault && binding.RunnerSelector() == (gitselector.Selector{}))) {
		return errs.New(errs.KindValidationFailed, "invalid GitHub deployment binding")
	}
	return nil
}

func (binding Binding) RunnerSelector() gitselector.Selector {
	return gitselector.Selector{Label: binding.RunnerLabel, Group: binding.RunnerGroup, GroupID: binding.RunnerGroupID}
}

func (binding Binding) connectionScopeAllowed() bool {
	switch binding.ConnectionScopeKind {
	case ids.KindTenant:
		return binding.ConnectionScopeID == binding.TenantID
	case ids.KindProject:
		return binding.ConnectionScopeID == binding.ProjectID
	case ids.KindEnvironment:
		return binding.ConnectionScopeID == binding.EnvironmentID
	default:
		return false
	}
}

type StartInput struct {
	IdempotencyKey string
	ExpectedCommit string
	Automatic      bool
}

type RolloutPolicy struct {
	Strategy  release.Strategy  `json:"strategy"`
	OnFailure release.OnFailure `json:"on_failure"`
}

func (policy RolloutPolicy) validate() error {
	strategy, err := release.DeploymentStrategy(string(policy.Strategy), "")
	if err != nil || strategy != policy.Strategy ||
		(policy.OnFailure != release.OnFailureSwitchBack && policy.OnFailure != release.OnFailureLeaveActive) {
		return errs.New(errs.KindInternal, "invalid captured release policy")
	}
	return nil
}

type Phase string

const (
	Queued           Phase = "queued"
	Dispatching      Phase = "dispatching"
	Building         Phase = "building"
	AwaitingApproval Phase = "awaiting_approval"
	FetchPending     Phase = "fetch_pending"
	Fetching         Phase = "fetching"
	DeployPending    Phase = "deploy_pending"
	Deploying        Phase = "deploying"
	Succeeded        Phase = "succeeded"
	Failed           Phase = "failed"
	Superseded       Phase = "superseded"
	Attention        Phase = "attention_required"
)

func (phase Phase) terminal() bool {
	return phase == Succeeded || phase == Failed || phase == Superseded || phase == Attention
}

func (phase Phase) activationOwned() bool {
	return phase == DeployPending || phase == Deploying
}

type Run struct {
	ID               string        `json:"id"`
	Binding          Binding       `json:"binding"`
	BindingRevision  int64         `json:"binding_revision"`
	Commit           string        `json:"commit"`
	EligibleRunners  []int64       `json:"eligible_runners"`
	Policy           RolloutPolicy `json:"policy"`
	Phase            Phase         `json:"phase"`
	GitHubRunID      int64         `json:"github_run_id,omitempty"`
	GitHubURL        string        `json:"github_url,omitempty"`
	GitHubRunAttempt int64         `json:"github_run_attempt,omitempty"`
	GitHubRunnerID   int64         `json:"github_runner_id,omitempty"`
	ImageDigest      string        `json:"image_digest,omitempty"`
	FetchTaskID      string        `json:"fetch_task_id,omitempty"`
	DeployTaskID     string        `json:"deploy_task_id,omitempty"`
	ReleaseID        string        `json:"release_id,omitempty"`
	ErrorCode        string        `json:"error_code,omitempty"`
	CreatedAt        time.Time     `json:"created_at"`
	UpdatedAt        time.Time     `json:"updated_at"`
}

// ImageRepository belongs to exactly one immutable deployment request.
func (run Run) ImageRepository() string {
	return ImageRepository(run.Binding.Target, run.ID)
}

// ImageRepository returns the private namespace reserved for one deployment.
func ImageRepository(target Target, requestID string) string {
	return imagefetch.RegistryAuthority + "/gitdelivery/" + strings.ToLower(
		target.ServiceID,
	) + "/" + strings.ToLower(
		requestID,
	)
}

func (run Run) validate() error {
	if ids.Validate(ids.KindOperation, run.ID) != nil || run.Binding.Validate() != nil ||
		!run.Binding.RunnerSelector().Valid() ||
		run.BindingRevision <= 0 ||
		!commitPattern.MatchString(run.Commit) ||
		run.Policy.validate() != nil ||
		!validRunners(run.EligibleRunners) ||
		!recordcodec.IsCanonicalUTC(run.CreatedAt) ||
		!recordcodec.IsCanonicalUTC(run.UpdatedAt) ||
		run.UpdatedAt.Before(run.CreatedAt) ||
		(run.ImageDigest != "" && !validDigest(run.ImageDigest)) ||
		(run.FetchTaskID != "" && ids.Validate(ids.KindTask, run.FetchTaskID) != nil) ||
		(run.DeployTaskID != "" && ids.Validate(ids.KindTask, run.DeployTaskID) != nil) ||
		(run.ReleaseID != "" && ids.Validate(ids.KindDeployment, run.ReleaseID) != nil) {
		return errs.New(errs.KindInternal, "invalid stored GitHub deployment")
	}
	switch run.Phase {
	case Queued, Dispatching, Failed, Superseded, Attention:
	case Building:
		if run.GitHubRunID <= 0 || run.GitHubRunAttempt <= 0 {
			return errs.New(errs.KindInternal, "GitHub deployment is missing build identity")
		}
	case AwaitingApproval, FetchPending, Fetching, DeployPending, Deploying, Succeeded:
		if run.GitHubRunID <= 0 || run.GitHubRunAttempt <= 0 || run.ImageDigest == "" ||
			!slices.Contains(run.EligibleRunners, run.GitHubRunnerID) {
			return errs.New(errs.KindInternal, "GitHub deployment is missing verified build evidence")
		}
	default:
		return errs.New(errs.KindInternal, "invalid stored GitHub deployment phase")
	}
	if (run.Phase == Fetching || run.Phase.activationOwned() || run.Phase == Succeeded) && run.FetchTaskID == "" ||
		(run.Phase == Deploying || run.Phase == Succeeded) && run.DeployTaskID == "" ||
		run.Phase == Succeeded && run.ReleaseID == "" {
		return errs.New(errs.KindInternal, "GitHub deployment is missing a native Task or Release")
	}
	return nil
}

var (
	commitPattern   = regexp.MustCompile(`\A[0-9a-f]{40}\z`)
	namePattern     = regexp.MustCompile(`\A[A-Za-z0-9][A-Za-z0-9._-]{0,99}\z`)
	branchPattern   = regexp.MustCompile(`\A[A-Za-z0-9._/-]{1,200}\z`)
	repoPathPattern = regexp.MustCompile(`\A[A-Za-z0-9._/-]{1,250}\z`)
	keyPattern      = regexp.MustCompile(`\A[A-Za-z0-9._-]{1,128}\z`)
)

func validBranch(value string) bool {
	if !branchPattern.MatchString(value) || strings.HasPrefix(value, "-") ||
		strings.HasSuffix(value, ".") || strings.Contains(value, "..") {
		return false
	}
	for member := range strings.SplitSeq(value, "/") {
		if member == "" || strings.HasPrefix(member, ".") || strings.HasSuffix(member, ".lock") {
			return false
		}
	}
	return !strings.HasPrefix(value, "refs/")
}

func validRepoPath(value string, allowDot bool) bool {
	if value == "." {
		return allowDot
	}
	return repoPathPattern.MatchString(value) && !strings.HasPrefix(value, "/") && path.Clean(value) == value &&
		value != ".." && !strings.HasPrefix(value, "../")
}

func validRunners(runners []int64) bool {
	if len(runners) == 0 || len(runners) > maximumRunners {
		return false
	}
	for i, value := range runners {
		if value <= 0 || i > 0 && runners[i-1] >= value {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	return strings.HasPrefix(value, "sha256:") && recordcodec.ValidNonZeroSHA256(strings.TrimPrefix(value, "sha256:"))
}
