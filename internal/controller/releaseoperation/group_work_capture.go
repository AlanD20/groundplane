package releaseoperation

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) captureGroupWork(ctx context.Context, scope releasequeries.ReleasePlanningScope,
	group releasegroup.Versioned, projection environmentprojection.EnvironmentComposeProjection,
	candidates []releaseCandidateInput, addresses networkreservations.ProxyAddresses, requestSHA string,
) (capture etcd.GroupReleaseCapture, resultErr error) {
	if group.Group.EnvironmentID != scope.Environment.Record.ID || group.Revision <= 0 || group.ReadRevision != scope.ReadRevision ||
		len(candidates) < domain.MinimumGroupMembers || len(candidates) > domain.MaximumGroupMembers || len(candidates) != len(group.Group.Order) {
		return capture, errs.New(errs.KindValidationFailed, "Group capture requires its complete declared planning snapshot")
	}
	defer func() {
		if resultErr != nil {
			capture.Clear()
		}
	}()
	placement, err := service.ledger.ResolveReleaseHost(ctx, scope.ReadRevision)
	if err != nil {
		return capture, err
	}
	action, dependencyPlan := workexecution.ActionDeploy, scope.Compose.Record.DeployDependencyPlan
	if candidates[0].rollbackSource != "" {
		action, dependencyPlan = workexecution.ActionRollback, scope.Compose.Record.RollbackDependencyPlan
	}
	if err := validateReleaseDependencyOrder(candidates, dependencyPlan); err != nil {
		return capture, err
	}
	at := service.now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Scope:  workexecution.Scope{TenantID: scope.Project.Record.TenantID, ProjectID: scope.Project.Record.ID, EnvironmentID: scope.Environment.Record.ID},
		Action: action, TargetID: group.Group.ID, TargetName: group.Group.Name,
		Intent:    workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: requestSHA},
		CreatedAt: at, UpdatedAt: at, Deadline: at.Add(service.timeout), ExpectedItems: uint32(2 * len(candidates))}
	if err := task.Validate(); err != nil {
		return capture, err
	}
	operationID, networkSeed := ids.New(ids.KindOperation), ids.New(ids.KindConfig)
	policy := domain.OnFailure(group.Group.OnFailure)
	capture.Root = releasegroups.ExecutionCapture{PlanID: task.Intent.ID, Scope: task.Scope, Action: action, Placement: placement,
		GroupName: task.TargetName, GroupRevision: group.Revision, SourceReadRevision: scope.ReadRevision, CreatedAt: at, Deadline: task.Deadline,
		Policy: domain.GroupWorkManifest{TaskID: task.ID, Manifest: domain.GroupManifest{OperationID: operationID,
			ReleaseGroupID: task.TargetID, EnvironmentID: task.Scope.EnvironmentID, FailurePolicy: policy,
			ConfiguredTimeoutSeconds: int64(service.timeout / time.Second)}}}
	budget := 20*time.Minute + time.Duration(len(candidates))*10*time.Minute
	if policy == domain.OnFailureSwitchBack {
		budget += time.Duration(len(candidates)) * 10 * time.Minute
	}
	var forwards, restorations []workexecution.Item
	networks := make(map[string]etcd.DirectReleaseNetwork)
	var hookBytes uint64
	hookCount := 0
	for index, candidate := range candidates {
		if candidate.planning.Service.Record.Desired.ID != group.Group.Order[index] || candidate.onFailure != policy ||
			(action == workexecution.ActionRollback) != (candidate.rollbackSource != "") {
			return capture, errs.New(errs.KindStateConflict, "Group candidate differs from its declared member order or common policy")
		}
		item := capturedReleaseItem(task, ids.New(ids.KindWorkItem), candidate.planning.Service.Record.Desired.ID, placement, workexecution.Release)
		binding := &releaserender.GroupMemberBinding{GroupID: task.TargetID, OperationID: operationID, Ordinal: uint32(index + 1)}
		_, item, request, rollout, err := service.captureReleaseMember(ctx, scope, projection, candidate, addresses,
			task, item, ids.New(ids.KindPlan), networkSeed, binding)
		if err != nil {
			clear(rollout.CandidateArtifact)
			return capture, err
		}
		capture.Members = append(capture.Members, etcd.GroupReleaseMemberCapture{Preparation: request, Rollout: rollout})
		member := &capture.Members[index]
		hookCount += len(rollout.Render.Hooks)
		for _, hook := range rollout.Render.Hooks {
			hookBytes += uint64(hook.BodySize)
		}
		budget += time.Duration(len(rollout.Render.Hooks)) * time.Duration(executionplan.ScriptExecutionTimeoutSeconds) * time.Second
		if rolling := rollout.Render.Rolling; rolling != nil {
			budget += time.Duration(max(len(rolling.Prior), len(rolling.Candidate))+1)*time.Duration(rolling.ReplicaTimeoutSeconds)*time.Second - 10*time.Minute
		}
		if err := service.captureGroupPreflight(ctx, task, binding.Ordinal, member, &capture); err != nil {
			return capture, err
		}
		for _, network := range request.Networks {
			previous, found := networks[network.NetworkID]
			if found && (previous.WorkItemID != network.WorkItemID || previous.Input != network.Input || !bytes.Equal(previous.Data, network.Data)) {
				return capture, errs.New(errs.KindStateConflict, "Group members selected different immutable definitions for the same DNS bridge")
			}
			networks[network.NetworkID] = network
		}
		policyMember := domain.GroupWorkMember{Member: domain.GroupMember{Ordinal: binding.Ordinal, ServiceID: item.TargetID,
			ReleaseID: request.Intent.ID}, ForwardID: item.ID, RestoreID: ids.New(ids.KindWorkItem)}
		capture.Root.Policy.Members = append(capture.Root.Policy.Members, policyMember)
		capture.Root.Policy.Manifest.Members = append(capture.Root.Policy.Manifest.Members, policyMember.Member)
		forwards = append(forwards, item)
		restorations = append(restorations, capturedReleaseItem(task, policyMember.RestoreID, item.TargetID, placement, workexecution.ReleaseRestoration))
		value, reference, err := etcd.EncodeDirectReleasePreparation(member.Preparation)
		clear(value)
		if err != nil {
			return capture, err
		}
		capture.Root.Members = append(capture.Root.Members, releasegroups.MemberPreparations{Forward: reference})
	}
	if len(networks) > 256 || hookCount > 16 || hookBytes > 1<<20 {
		return capture, errs.New(errs.KindReleasePlanTooLarge, "Group hooks or DNS bridges exceed the captured operation limits")
	}
	if budget > service.timeout {
		return capture, errs.New(errs.KindReleaseDeadlineTooShort, "Group hooks, DNS bridges or rollout budget exceed the captured operation limits")
	}
	capture.Root.Policy.Manifest.ComputedBudgetSeconds = int64(budget / time.Second)
	for index := range capture.Root.Members {
		preparation, err := capture.Root.RestorationPreparation(index)
		if err != nil {
			return capture, err
		}
		reference, err := preparation.Reference()
		if err != nil {
			return capture, err
		}
		capture.Root.Members[index].Restore = reference
	}
	var ordered []etcd.DirectReleaseNetwork
	for _, network := range networks {
		ordered = append(ordered, network)
	}
	slices.SortFunc(ordered, func(left, right etcd.DirectReleaseNetwork) int {
		return strings.Compare(left.NetworkID, right.NetworkID)
	})
	previous := ""
	for _, network := range ordered {
		item := capturedReleaseItem(task, network.WorkItemID, network.NetworkID, placement, workexecution.Network)
		item.Phase, item.Wait, item.Input = workexecution.Ready, nil, &network.Input
		item.Claims = []workexecution.Claim{{ScopeID: task.Scope.EnvironmentID}, {HostID: placement.HostID, ScopeID: task.Scope.EnvironmentID},
			{HostID: placement.HostID, ScopeID: network.NetworkID}}
		if previous != "" {
			item.Dependencies = []string{previous}
		}
		capture.Root.Graph = append(capture.Root.Graph, item)
		previous = item.ID
	}
	for _, hook := range capture.Root.Preflight {
		item := capturedReleaseItem(task, hook.WorkItemID, hook.ServiceID, placement, workexecution.Script)
		item.TargetID, item.Phase, item.Wait, item.Input = hook.ScriptID, workexecution.Ready, nil, &hook.Input
		if previous != "" {
			item.Dependencies = []string{previous}
		}
		capture.Root.Graph = append(capture.Root.Graph, item)
		previous = item.ID
	}
	for index := range forwards {
		if previous != "" {
			forwards[index].Dependencies = []string{previous}
		}
		capture.Root.Graph = append(capture.Root.Graph, forwards[index], restorations[index])
	}
	_, err = capture.Validate()
	return capture, err
}
