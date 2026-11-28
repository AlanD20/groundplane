package gitdeliveryoperator

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	"github.com/AlanD20/groundplane/internal/infra/githubapp"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func bindingScope(binding gitdelivery.Binding) githubconnection.Scope {
	return githubconnection.Scope{Kind: binding.ConnectionScopeKind, ID: binding.ConnectionScopeID}
}

func (operator *Operator) withRepository(
	ctx context.Context,
	binding gitdelivery.Binding,
	use func(*githubapp.Client, githubapp.Scope) error,
) error {
	return operator.connections.WithCredentials(
		ctx,
		bindingScope(binding),
		binding.ConnectionID,
		uint64(binding.ConnectionGeneration),
		func(connection githubconnection.Connection, credentials githubconnection.Credentials) error {
			if connection.InstallationID != binding.InstallationID {
				return errs.New(errs.KindScopeUnauthorized, "GitHub installation is outside the captured binding")
			}
			client, err := githubapp.New(connection.AppID, credentials.PrivateKeyPEM)
			if err != nil {
				return err
			}
			return use(
				client,
				githubapp.Scope{InstallationID: connection.InstallationID, RepositoryID: binding.RepositoryID},
			)
		},
	)
}

func (operator *Operator) ResolveSource(ctx context.Context, binding gitdelivery.Binding) (string, error) {
	return operator.resolveBranch(ctx, binding, binding.Branch)
}

func (operator *Operator) resolveBranch(
	ctx context.Context,
	binding gitdelivery.Binding,
	branch string,
) (string, error) {
	var commit string
	err := operator.withRepository(ctx, binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		var err error
		commit, err = client.ResolveBranch(ctx, scope, branch)
		return err
	})
	return commit, err
}

func (operator *Operator) VerifyWorkflow(ctx context.Context, binding gitdelivery.Binding) error {
	return operator.withRepository(ctx, binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		return verifyWorkflow(ctx, client, scope, binding)
	})
}

func verifyWorkflow(
	ctx context.Context,
	client *githubapp.Client,
	scope githubapp.Scope,
	binding gitdelivery.Binding,
) error {
	commit, err := client.ResolveBranch(ctx, scope, binding.WorkflowRef)
	if err != nil {
		return err
	}
	if commit != binding.WorkflowCommit {
		return errs.New(errs.KindStateConflict, "the approved GitHub workflow ref has moved")
	}
	workflow, err := client.InspectWorkflow(ctx, scope, binding.WorkflowID, commit)
	if err != nil {
		return err
	}
	if workflow.ID != binding.WorkflowID || workflow.State != "active" ||
		!strings.HasPrefix(workflow.Path, ".github/workflows/") {
		return errs.New(errs.KindStateConflict, "the approved GitHub workflow is unavailable")
	}
	return nil
}

func (operator *Operator) Dispatch(ctx context.Context, run gitdelivery.Run) (gitdelivery.WorkflowRun, error) {
	binding := run.Binding
	ready, err := operator.Capture(ctx, binding)
	if err != nil {
		return gitdelivery.WorkflowRun{}, err
	}
	if !slices.ContainsFunc(ready, func(id int64) bool { return slices.Contains(run.EligibleRunners, id) }) {
		return gitdelivery.WorkflowRun{}, errs.New(
			errs.KindStateConflict,
			"no captured Groundplane Runner is ready for this build",
		)
	}
	var providerRun githubapp.Run
	err = operator.withRepository(ctx, binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		if err := verifyWorkflow(ctx, client, scope, binding); err != nil {
			return err
		}
		var err error
		providerRun, err = client.Dispatch(ctx, scope, githubapp.DispatchInput{
			WorkflowID: binding.WorkflowID, WorkflowRef: binding.WorkflowRef, RequestID: run.ID, Commit: run.Commit,
			RunnerLabel: binding.RunnerLabel, RunnerGroup: binding.RunnerGroup,
			Dockerfile: binding.Dockerfile, BuildContext: binding.BuildContext, ImageRepository: run.ImageRepository(),
		})
		return err
	})
	return workflowRun(providerRun), err
}

func (operator *Operator) FindDispatch(
	ctx context.Context,
	run gitdelivery.Run,
) (gitdelivery.WorkflowRun, bool, error) {
	var providerRun githubapp.Run
	var found bool
	err := operator.withRepository(ctx, run.Binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		var err error
		providerRun, found, err = client.FindDispatch(ctx, scope, run.Binding.WorkflowID, run.ID, run.CreatedAt)
		return err
	})
	return workflowRun(providerRun), found, err
}

func (operator *Operator) Inspect(ctx context.Context, run gitdelivery.Run) (gitdelivery.WorkflowRun, error) {
	var providerRun githubapp.Run
	err := operator.withRepository(ctx, run.Binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		var err error
		providerRun, err = client.Run(ctx, scope, run.GitHubRunID)
		return err
	})
	return workflowRun(providerRun), err
}

func (operator *Operator) BuildResult(ctx context.Context, run gitdelivery.Run) (gitdelivery.BuildEvidence, error) {
	var evidence gitdelivery.BuildEvidence
	err := operator.withRepository(ctx, run.Binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		jobs, err := client.Jobs(ctx, scope, run.GitHubRunID, run.GitHubRunAttempt)
		if err != nil {
			return err
		}
		var build githubapp.Job
		for _, job := range jobs {
			if job.Name == gitdelivery.BuildJobName {
				if build.ID != 0 {
					return errs.New(errs.KindStateConflict, "GitHub reported multiple Groundplane build jobs")
				}
				build = job
			}
		}
		if build.ID == 0 || build.Status != "completed" || build.Conclusion != "success" ||
			build.RunID != run.GitHubRunID || build.Attempt != run.GitHubRunAttempt ||
			build.HeadSHA != run.Binding.WorkflowCommit || !slices.Contains(run.EligibleRunners, build.RunnerID) ||
			!containsLabel(build.Labels, run.Binding.RunnerLabel) ||
			(run.Binding.RunnerGroup != "" && !strings.EqualFold(build.RunnerGroupName, run.Binding.RunnerGroup)) {
			return errs.New(errs.KindScopeUnauthorized, "GitHub build job did not complete on an eligible Runner")
		}
		result, err := client.BuildResult(
			ctx,
			scope,
			run.GitHubRunID,
			run.GitHubRunAttempt,
			gitdelivery.BuildArtifactName,
		)
		if err != nil {
			return err
		}
		evidence = gitdelivery.BuildEvidence{
			RunID: result.RunID, RunAttempt: result.RunAttempt, RunnerID: build.RunnerID, RunnerGroup: build.RunnerGroupID,
			RequestID: result.RequestID, SourceCommit: result.SourceCommit, ImageDigest: result.ImageDigest,
		}
		return nil
	})
	return evidence, err
}

func workflowRun(run githubapp.Run) gitdelivery.WorkflowRun {
	state := gitdelivery.WorkflowPending
	if run.Status == "completed" {
		state = gitdelivery.WorkflowFailed
		if run.Conclusion == "success" {
			state = gitdelivery.WorkflowSucceeded
		}
	}
	return gitdelivery.WorkflowRun{
		ID: run.ID, WorkflowID: run.WorkflowID, Attempt: run.Attempt, HeadSHA: run.HeadSHA,
		Event: run.Event, Title: run.DisplayTitle, URL: run.URL, State: state,
	}
}

func containsLabel(labels []string, label string) bool {
	return slices.ContainsFunc(labels, func(candidate string) bool { return strings.EqualFold(candidate, label) })
}
