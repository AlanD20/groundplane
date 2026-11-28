package gitdeliveryoperator

import (
	"context"
	"slices"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/controller/gitdelivery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/githubapp"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type registeredRunner struct {
	provider   githubapp.Runner
	local      runners.RunnerDesiredRecord
	conditions []keyvalue.Condition
}

func (operator *Operator) Capture(ctx context.Context, binding gitdelivery.Binding) ([]int64, error) {
	if _, err := operator.Recheck(ctx, binding, 0); err != nil {
		return nil, err
	}
	matches, err := operator.registeredRunners(ctx, binding)
	if err != nil {
		return nil, err
	}
	eligible := make([]int64, 0, len(matches))
	for _, runner := range matches {
		if runner.provider.Status == "online" && runner.selectedBy(binding) {
			eligible = append(eligible, runner.provider.ID)
		}
	}
	slices.Sort(eligible)
	if len(eligible) == 0 {
		return nil, errs.New(
			errs.KindStateConflict,
			"no online scope-authorized Groundplane Runner matches the selected label and group",
		)
	}
	return eligible, nil
}

func (operator *Operator) Recheck(
	ctx context.Context,
	binding gitdelivery.Binding,
	runnerID int64,
) ([]keyvalue.Condition, error) {
	if err := binding.Validate(); err != nil {
		return nil, err
	}
	target, conditions, err := operator.resolveTarget(ctx, binding.ServiceID)
	if err != nil {
		return nil, err
	}
	if target != binding.Target {
		return nil, errs.New(errs.KindScopeUnauthorized, "Service owner differs from the GitHub deployment target")
	}
	active, err := operator.connections.ActiveFence(
		ctx,
		bindingScope(binding),
		binding.ConnectionID,
		uint64(binding.ConnectionGeneration),
	)
	if err != nil {
		return nil, err
	}
	conditions = append(conditions, active...)
	if runnerID > 0 {
		matches, err := operator.registeredRunners(ctx, binding)
		if err != nil {
			return nil, err
		}
		for _, runner := range matches {
			if runner.provider.ID == runnerID && runner.selectedBy(binding) {
				return distinctConditions(append(conditions, runner.conditions...))
			}
		}
		return nil, errs.New(errs.KindScopeUnauthorized, "GitHub job Runner no longer has access to this Service")
	}
	return distinctConditions(conditions)
}

func distinctConditions(conditions []keyvalue.Condition) ([]keyvalue.Condition, error) {
	seen := make(map[string]int64, len(conditions))
	result := conditions[:0]
	for _, condition := range conditions {
		if revision, exists := seen[condition.Key]; exists {
			if revision != condition.ModRevision {
				return nil, errs.New(errs.KindStateConflict, "GitHub deployment authority changed during validation")
			}
			continue
		}
		seen[condition.Key] = condition.ModRevision
		result = append(result, condition)
	}
	return result, nil
}

func (runner registeredRunner) selectedBy(binding gitdelivery.Binding) bool {
	return containsLabel(runner.local.Labels, binding.RunnerLabel) &&
		containsLabel(runner.provider.Labels, binding.RunnerLabel) &&
		(binding.RunnerGroup == "" || (runner.provider.GroupID == binding.RunnerGroupID &&
			strings.EqualFold(runner.provider.GroupName, binding.RunnerGroup)))
}

func (operator *Operator) registeredRunners(
	ctx context.Context,
	binding gitdelivery.Binding,
) ([]registeredRunner, error) {
	if operator.authority == nil {
		return nil, errs.New(
			errs.KindStateConflict,
			"GitHub deployment requires a scoped Groundplane Runner with verified registration",
		)
	}
	local, err := operator.runners.ListRunners(
		ctx,
		runners.RunnerFilter{TenantID: binding.TenantID},
		keyvalue.PageRequest{Limit: keyvalue.MaximumPageLimit},
	)
	if err != nil {
		return nil, err
	}
	if local.NextCursor != "" {
		return nil, errs.New(errs.KindInternal, "Groundplane Runner inventory exceeded the tenant limit")
	}
	matches := make([]registeredRunner, 0, len(local.Items))
	registered := make(map[int64]struct{}, len(local.Items))
	err = operator.withRepository(ctx, binding, func(client *githubapp.Client, scope githubapp.Scope) error {
		repository, err := client.Repository(ctx, scope)
		if err != nil {
			return err
		}
		repoURL := "https://github.com/" + repository.Owner + "/" + repository.Name
		orgURL := "https://github.com/" + repository.Owner
		candidates := make(map[string]int, len(local.Items))
		for _, item := range local.Items {
			record := item.Record.Desired
			if strings.EqualFold(record.GitHubURL, repoURL) || strings.EqualFold(record.GitHubURL, orgURL) {
				name, err := registrationName(record)
				if err != nil {
					return err
				}
				candidates[strings.ToLower(name)]++
			}
		}
		for _, item := range local.Items {
			record := item.Record
			if record.ProvisioningState != runners.RunnerProvisioningReady ||
				!runnerOwnsTarget(record.Desired, binding.Target) ||
				(!strings.EqualFold(record.Desired.GitHubURL, repoURL) && !strings.EqualFold(record.Desired.GitHubURL, orgURL)) {
				continue
			}
			name, err := registrationName(record.Desired)
			if err != nil {
				return err
			}
			if candidates[strings.ToLower(name)] != 1 {
				continue
			}
			_, deleting, err := operator.runners.GetRunnerDeletionTombstone(ctx, record.Desired.ID)
			if err != nil {
				return err
			}
			if deleting {
				continue
			}
			authority, err := operator.authority.GitHubDeliveryAuthority(ctx, record.Desired.ID)
			if err != nil {
				return err
			}
			if authority.GitHubID <= 0 || len(authority.Conditions) == 0 {
				continue
			}
			exact, err := lookupProviderRunner(ctx, client, scope, record.Desired, name, orgURL)
			if err != nil {
				return err
			}
			if exact.ID != authority.GitHubID {
				continue
			}
			if _, exists := registered[exact.ID]; exists {
				return errs.New(
					errs.KindStateConflict,
					"GitHub Runner registration is shared by multiple Groundplane Runners",
				)
			}
			registered[exact.ID] = struct{}{}
			matches = append(matches, registeredRunner{
				provider: exact, local: record.Desired,
				conditions: append([]keyvalue.Condition{
					{Key: runners.RunnerKey(record.Desired.ID), ModRevision: item.Revision},
					{Key: runners.RunnerLifecycleKey(record.Desired.ID), ModRevision: record.LifecycleRevision},
					absentTombstone(deletions.DeletionTargetRunner, record.Desired.ID),
				}, authority.Conditions...),
			})
		}
		return nil
	})
	return matches, err
}

func registrationName(record runners.RunnerDesiredRecord) (string, error) {
	if record.GitHubName != "" {
		return record.GitHubName, nil
	}
	return runners.RunnerName(record.ID)
}

func runnerOwnsTarget(runner runners.RunnerDesiredRecord, target gitdelivery.Target) bool {
	if runner.TenantID != target.TenantID {
		return false
	}
	switch runner.OwnerKind {
	case runners.RunnerOwnerTenant:
		return runner.OwnerID == target.TenantID
	case runners.RunnerOwnerProject:
		return runner.OwnerID == target.ProjectID
	case runners.RunnerOwnerEnvironment:
		return runner.OwnerID == target.EnvironmentID && runner.ParentProjectID == target.ProjectID
	default:
		return false
	}
}

func (operator *Operator) ListGitRunnerSelectors(
	ctx context.Context,
	serviceID, connectionID string,
	repositoryID int64,
) (api.Page[api.GitRunnerSelector], error) {
	target, _, err := operator.resolveTarget(ctx, serviceID)
	if err != nil {
		return api.Page[api.GitRunnerSelector]{}, err
	}
	connection, err := operator.resolveConnection(ctx, target, connectionID)
	if err != nil {
		return api.Page[api.GitRunnerSelector]{}, err
	}
	runners, err := operator.registeredRunners(ctx, gitdelivery.Binding{
		Target: target, ConnectionID: connection.ID, ConnectionGeneration: int64(connection.Generation),
		ConnectionScopeKind: connection.Scope.Kind, ConnectionScopeID: connection.Scope.ID,
		InstallationID: connection.InstallationID, RepositoryID: repositoryID,
	})
	if err != nil {
		return api.Page[api.GitRunnerSelector]{}, err
	}
	type selectorKey struct {
		label   string
		groupID int64
	}
	selectors := make(map[selectorKey]api.GitRunnerSelector)
	for _, runner := range runners {
		for _, label := range runner.local.Labels {
			if !containsLabel(runner.provider.Labels, label) {
				continue
			}
			add := func(groupID int64, group string) {
				key := selectorKey{label: strings.ToLower(label), groupID: groupID}
				entry := selectors[key]
				entry.Label, entry.Group, entry.GroupID = label, group, groupID
				entry.Matching++
				if runner.provider.Status == "online" && !runner.provider.Busy {
					entry.Available++
				}
				selectors[key] = entry
			}
			add(0, "")
			if runner.provider.GroupID > 0 {
				add(runner.provider.GroupID, runner.provider.GroupName)
			}
		}
	}
	items := make([]api.GitRunnerSelector, 0, len(selectors))
	for _, selector := range selectors {
		items = append(items, selector)
	}
	sort.Slice(items, func(i, j int) bool {
		left, right := strings.ToLower(items[i].Label), strings.ToLower(items[j].Label)
		if left != right {
			return left < right
		}
		return items[i].GroupID < items[j].GroupID
	})
	return api.Page[api.GitRunnerSelector]{Items: items}, nil
}
