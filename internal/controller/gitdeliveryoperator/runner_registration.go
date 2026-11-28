package gitdeliveryoperator

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/githubapp"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VerifyRunnerRegistration captures provider-side identity for a supervised
// scoped Runner cutover. Activation must compare GitHubID to the local
// registration before treating it as proven ready for deployment.
func (operator *Operator) VerifyRunnerRegistration(
	ctx context.Context,
	runnerID, connectionID string,
	repositoryID int64,
) (RunnerAuthorityEvidence, error) {
	current, err := operator.runners.GetRunner(ctx, runnerID)
	if err != nil {
		return RunnerAuthorityEvidence{}, err
	}
	record := current.Record
	if record.ProvisioningState != runners.RunnerProvisioningReady || record.ContainerID == "" {
		return RunnerAuthorityEvidence{}, errs.New(
			errs.KindStateConflict,
			"Runner must be provisioned before verifying GitHub registration",
		)
	}
	_, deleting, err := operator.runners.GetRunnerDeletionTombstone(ctx, runnerID)
	if err != nil {
		return RunnerAuthorityEvidence{}, err
	}
	if deleting {
		return RunnerAuthorityEvidence{}, errs.New(errs.KindResourceInUse, "Runner removal is in progress")
	}
	var kind ids.Kind
	switch record.Desired.OwnerKind {
	case runners.RunnerOwnerEnvironment:
		kind = ids.KindEnvironment
	case runners.RunnerOwnerProject:
		kind = ids.KindProject
	case runners.RunnerOwnerTenant:
		kind = ids.KindTenant
	default:
		return RunnerAuthorityEvidence{}, errs.New(errs.KindInternal, "Runner has an invalid owner")
	}
	scope := publicScope(githubconnection.Scope{Kind: kind, ID: record.Desired.OwnerID})
	binding, _, guards, err := operator.repositoryTarget(ctx, scope, connectionID, repositoryID)
	if err != nil {
		return RunnerAuthorityEvidence{}, err
	}
	if !runnerOwnsTarget(record.Desired, binding.Target) {
		return RunnerAuthorityEvidence{}, errs.New(
			errs.KindScopeUnauthorized,
			"Runner owner is outside the selected repository connection",
		)
	}
	proof, err := operator.connections.ActiveFence(
		ctx,
		bindingScope(binding),
		binding.ConnectionID,
		uint64(binding.ConnectionGeneration),
	)
	if err != nil {
		return RunnerAuthorityEvidence{}, err
	}
	name, err := registrationName(record.Desired)
	if err != nil {
		return RunnerAuthorityEvidence{}, err
	}
	var verified githubapp.Runner
	err = operator.withRepository(ctx, binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		repository, err := client.Repository(ctx, scope)
		if err != nil {
			return err
		}
		orgURL := "https://github.com/" + repository.Owner
		repoURL := orgURL + "/" + repository.Name
		if !strings.EqualFold(record.Desired.GitHubURL, orgURL) &&
			!strings.EqualFold(record.Desired.GitHubURL, repoURL) {
			return errs.New(errs.KindScopeUnauthorized, "Runner registration is outside the selected GitHub repository")
		}
		verified, err = lookupProviderRunner(ctx, client, scope, record.Desired, name, orgURL)
		if err != nil {
			return err
		}
		if verified.ID == 0 {
			return errs.New(
				errs.KindStateConflict,
				"the selected repository cannot use this GitHub Runner registration",
			)
		}
		for _, label := range record.Desired.Labels {
			if !containsLabel(verified.Labels, label) {
				return errs.New(
					errs.KindStateConflict,
					"GitHub Runner registration does not have its configured labels",
				)
			}
		}
		return nil
	})
	if err != nil {
		return RunnerAuthorityEvidence{}, err
	}
	guards = append(guards, proof...)
	guards = append(guards,
		keyvalue.Condition{Key: runners.RunnerKey(runnerID), ModRevision: current.Revision},
		keyvalue.Condition{Key: runners.RunnerLifecycleKey(runnerID), ModRevision: record.LifecycleRevision},
		absentTombstone(deletions.DeletionTargetRunner, runnerID),
	)
	guards, err = distinctConditions(guards)
	if err != nil {
		return RunnerAuthorityEvidence{}, err
	}
	return RunnerAuthorityEvidence{GitHubID: verified.ID, Conditions: guards}, nil
}

func lookupProviderRunner(ctx context.Context, client *githubapp.Client, scope githubapp.Scope,
	record runners.RunnerDesiredRecord, name, orgURL string,
) (githubapp.Runner, error) {
	var inventory []githubapp.Runner
	var err error
	if strings.EqualFold(record.GitHubURL, orgURL) {
		inventory, err = client.OrganizationRunners(ctx, scope, record.RunnerGroup, name)
	} else {
		inventory, err = client.RepositoryRunners(ctx, scope, name)
	}
	if err != nil {
		return githubapp.Runner{}, err
	}
	var exact githubapp.Runner
	for _, candidate := range inventory {
		if strings.EqualFold(candidate.Name, name) {
			if exact.ID != 0 {
				return githubapp.Runner{}, errs.New(errs.KindStateConflict, "GitHub Runner name is ambiguous")
			}
			exact = candidate
		}
	}
	return exact, nil
}
