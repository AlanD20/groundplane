package gitdeliveryoperator

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/githubapp"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (operator *Operator) CaptureRegistryJob(ctx context.Context, id string) (gitdelivery.RegistryJobSnapshot, error) {
	return operator.delivery.CaptureRegistryJob(ctx, id)
}

// VerifyRegistryJob checks the live provider assignment against the approved
// workflow and exact scoped Runner. A signed proof is required at issuance.
// Registry token exchanges repeat the assignment check using their opaque grant.
func (operator *Operator) VerifyRegistryJob(
	ctx context.Context,
	run gitdelivery.Run,
	runnerID string,
	proof *githubapp.OIDCProof,
) ([]keyvalue.Condition, error) {
	binding := run.Binding
	if proof != nil && (proof.RepositoryID != binding.RepositoryID || proof.RunID != run.GitHubRunID ||
		proof.RunAttempt != run.GitHubRunAttempt || proof.SHA != binding.WorkflowCommit ||
		proof.WorkflowSHA != binding.WorkflowCommit || proof.EventName != "workflow_dispatch" ||
		proof.RunnerEnvironment != "self-hosted" || proof.JobID <= 0) {
		return nil, deniedRegistryJob()
	}
	conditions, err := operator.Recheck(ctx, binding, 0)
	if err != nil {
		return nil, err
	}
	registration, err := operator.captureRegistryRunner(ctx, run, runnerID)
	if err != nil {
		return nil, err
	}
	if err := operator.verifyProviderRegistryJob(ctx, run, registration, proof); err != nil {
		return nil, err
	}
	return distinctConditions(append(conditions, registration.conditions...))
}

type registryRunner struct {
	githubID   int64
	githubName string
	conditions []keyvalue.Condition
}

func (operator *Operator) captureRegistryRunner(
	ctx context.Context,
	run gitdelivery.Run,
	id string,
) (registryRunner, error) {
	if operator.authority == nil {
		return registryRunner{}, deniedRegistryJob()
	}
	runner, err := operator.runners.GetRunner(ctx, id)
	if err != nil {
		return registryRunner{}, err
	}
	record := runner.Record
	if record.ProvisioningState != runners.RunnerProvisioningReady ||
		!runnerOwnsTarget(record.Desired, run.Binding.Target) ||
		!containsLabel(record.Desired.Labels, run.Binding.RunnerLabel) {
		return registryRunner{}, deniedRegistryJob()
	}
	name, err := registrationName(record.Desired)
	if err != nil {
		return registryRunner{}, err
	}
	authority, err := operator.authority.GitHubDeliveryAuthority(ctx, id)
	if err != nil {
		return registryRunner{}, err
	}
	if authority.GitHubID <= 0 || !slices.Contains(run.EligibleRunners, authority.GitHubID) ||
		len(authority.Conditions) == 0 {
		return registryRunner{}, deniedRegistryJob()
	}
	return registryRunner{githubID: authority.GitHubID, githubName: name, conditions: append([]keyvalue.Condition{
		{Key: runners.RunnerKey(id), ModRevision: runner.Revision},
		{Key: runners.RunnerLifecycleKey(id), ModRevision: record.LifecycleRevision},
		absentTombstone(deletions.DeletionTargetRunner, id),
	}, authority.Conditions...)}, nil
}

func (operator *Operator) verifyProviderRegistryJob(
	ctx context.Context,
	run gitdelivery.Run,
	runner registryRunner,
	proof *githubapp.OIDCProof,
) error {
	binding := run.Binding
	return operator.withRepository(ctx, binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		observed, err := client.Run(ctx, scope, run.GitHubRunID)
		if err != nil {
			return err
		}
		if observed.ID != run.GitHubRunID || observed.WorkflowID != binding.WorkflowID ||
			observed.Attempt != run.GitHubRunAttempt || observed.HeadSHA != binding.WorkflowCommit ||
			observed.Event != "workflow_dispatch" || observed.DisplayTitle != gitdelivery.BuildJobName+" "+run.ID {
			return deniedRegistryJob()
		}
		if observed.Status == "queued" || observed.Status == "waiting" || observed.Status == "pending" {
			return errs.New(errs.KindStateConflict, "GitHub has not confirmed the active build job")
		}
		if observed.Status != "in_progress" {
			return deniedRegistryJob()
		}
		if proof != nil {
			if err := checkWorkflowProof(ctx, client, scope, binding, *proof); err != nil {
				return err
			}
		}
		jobs, err := client.Jobs(ctx, scope, run.GitHubRunID, run.GitHubRunAttempt)
		if err != nil {
			return err
		}
		var build githubapp.Job
		for _, job := range jobs {
			if job.Name != gitdelivery.BuildJobName {
				continue
			}
			if build.ID != 0 {
				return deniedRegistryJob()
			}
			build = job
		}
		if build.ID == 0 || build.Status == "queued" {
			return errs.New(errs.KindStateConflict, "GitHub has not confirmed the active build job")
		}
		if build.Status != "in_progress" || build.RunnerID != runner.githubID ||
			!strings.EqualFold(build.RunnerName, runner.githubName) ||
			build.RunID != run.GitHubRunID || build.Attempt != run.GitHubRunAttempt ||
			build.HeadSHA != binding.WorkflowCommit || (proof != nil && proof.JobID != build.ID) ||
			!containsLabel(build.Labels, binding.RunnerLabel) ||
			(binding.RunnerGroupID != 0 &&
				(binding.RunnerGroupID != build.RunnerGroupID || !strings.EqualFold(binding.RunnerGroup, build.RunnerGroupName))) {
			return deniedRegistryJob()
		}
		return nil
	})
}

func checkWorkflowProof(
	ctx context.Context,
	client *githubapp.Client,
	scope githubapp.Scope,
	binding gitdelivery.Binding,
	proof githubapp.OIDCProof,
) error {
	repository, err := client.Repository(ctx, scope)
	if err != nil {
		return err
	}
	workflow, err := client.InspectWorkflow(ctx, scope, binding.WorkflowID, binding.WorkflowCommit)
	if err != nil {
		return err
	}
	ref := "refs/heads/" + binding.WorkflowRef
	name := repository.Owner + "/" + repository.Name
	expectedWorkflow := name + "/" + workflow.Path + "@" + ref
	workflowMatches := proof.WorkflowRef == expectedWorkflow && proof.Ref == ref
	reusableMatches := (proof.JobWorkflowRef == "" && proof.JobWorkflowSHA == "") ||
		(proof.JobWorkflowRef == expectedWorkflow && proof.JobWorkflowSHA == binding.WorkflowCommit)
	if repository.ID != binding.RepositoryID || !strings.EqualFold(proof.Repository, name) ||
		workflow.ID != binding.WorkflowID || workflow.State != "active" || !workflowMatches || !reusableMatches {
		return deniedRegistryJob()
	}
	return nil
}

func deniedRegistryJob() *errs.Error {
	return errs.New(errs.KindRequestForbidden, "build job is not authorized")
}
