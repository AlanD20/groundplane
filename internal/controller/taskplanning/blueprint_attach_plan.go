package taskplanning

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintAttachPlanCandidate struct {
	current        etcdstore.Versioned[attachrecord.Record]
	adapterKey     string
	authentication core.BackingAuthentication
	stepCount      int
}

func (resolver *TaskPlanResolver) blueprintAttachPlanCandidates(
	ctx context.Context,
	task etcd.TaskRecord,
) ([]blueprintAttachPlanCandidate, int, error) {
	if resolver.attaches == nil {
		return nil, 0, nil
	}
	versionedIntent, found, err := resolver.attaches.GetBlueprintAttachTaskIntent(ctx, task.ID)
	if err != nil || !found {
		return nil, 0, err
	}
	intent := versionedIntent.Record
	if intent.TaskID != task.ID || intent.EnvironmentID != task.Target ||
		intent.Status != taskjournal.TaskStatusPending {
		return nil, 0, errs.New(errs.KindStateConflict, "Blueprint Attach intent does not own the active Task")
	}
	if resolver.services == nil || resolver.attachIdentities == nil {
		return nil, 0, errs.New(errs.KindInternal, "Blueprint Attach plan dependencies are unavailable")
	}
	candidates := make([]blueprintAttachPlanCandidate, 0, len(intent.Candidates))
	totalSteps := 0
	for _, staged := range intent.Candidates {
		current, getErr := resolver.attaches.GetAttach(ctx, staged.ID)
		if getErr != nil {
			return nil, 0, getErr
		}
		expected := staged
		expected.Status = current.Record.Status
		if !blueprintAttachRecordEqual(expected, current.Record) ||
			(current.Record.Status != core.AttachPending && current.Record.Status != core.AttachProvisioning) {
			return nil, 0, errs.New(errs.KindStateConflict, "Blueprint Attach candidate changed after publication")
		}
		candidate := blueprintAttachPlanCandidate{current: current}
		if current.Record.OwnsCredential() {
			backing, serviceErr := resolver.services.GetService(ctx, current.Record.BackingServiceID)
			if serviceErr != nil {
				return nil, 0, serviceErr
			}
			if backing.Record.EnvironmentID != current.Record.BackingEnvironmentID ||
				backing.Record.BackingNetworkID != current.Record.BackingNetworkID ||
				backing.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
				return nil, 0, errs.New(errs.KindStateConflict, "Blueprint Attach backing Service is not runnable")
			}
			adapter, registered := adapters.Get(backing.Record.Desired.Adapter)
			if !registered {
				return nil, 0, errs.New(errs.KindValidationFailed, "Blueprint Attach adapter is not registered")
			}
			authentication, authErr := core.ResolveBackingAuthentication(
				adapter.SupportsAuthenticationModes(), backing.Record.Desired.Authentication,
			)
			if authErr != nil || authentication != backing.Record.Desired.Authentication {
				return nil, 0, errs.New(
					errs.KindStateConflict,
					"Blueprint Attach backing Service authentication mode is invalid",
				)
			}
			candidate.adapterKey = adapter.Key()
			candidate.authentication = authentication
			if !adapter.Custom() && authentication != core.BackingAuthenticationNone {
				candidate.stepCount = len(current.Record.GrantAttachIDs) + 1
				totalSteps += candidate.stepCount
			}
		}
		candidates = append(candidates, candidate)
	}
	return candidates, totalSteps, nil
}

func blueprintAttachRecordEqual(left attachrecord.Record, right attachrecord.Record) bool {
	if left.ID != right.ID || left.EnvironmentID != right.EnvironmentID || left.Name != right.Name ||
		left.BackingProjectID != right.BackingProjectID || left.BackingEnvironmentID != right.BackingEnvironmentID ||
		left.BackingServiceID != right.BackingServiceID || left.BackingNetworkID != right.BackingNetworkID ||
		left.ServiceID != right.ServiceID || left.CredentialAttachID != right.CredentialAttachID ||
		left.Status != right.Status || left.Operation != right.Operation || left.TaskID != right.TaskID ||
		left.CreatedAt != right.CreatedAt ||
		(left.GrantAttachIDs == nil) != (right.GrantAttachIDs == nil) ||
		!slices.Equal(left.GrantAttachIDs, right.GrantAttachIDs) ||
		left.HookBundle != right.HookBundle || (left.FactSets == nil) != (right.FactSets == nil) ||
		len(left.FactSets) != len(right.FactSets) {
		return false
	}
	for index := range left.FactSets {
		leftSet, rightSet := left.FactSets[index], right.FactSets[index]
		if leftSet.GrantAttachID != rightSet.GrantAttachID ||
			(leftSet.Facts == nil) != (rightSet.Facts == nil) || !slices.Equal(leftSet.Facts, rightSet.Facts) {
			return false
		}
	}
	return true
}

func (resolver *TaskPlanResolver) blueprintAttachProcedureSteps(
	ctx context.Context,
	task etcd.TaskRecord,
	candidates []blueprintAttachPlanCandidate,
	stepIndex int,
) ([]*agentpb.ExecutionStep, int, error) {
	steps := make([]*agentpb.ExecutionStep, 0)
	for _, candidate := range candidates {
		if candidate.stepCount == 0 {
			if candidate.authentication == core.BackingAuthenticationNone {
				procedureTask := task
				procedureTask.Type = taskjournal.TaskAttach
				procedureTask.Target = candidate.current.Record.ID
				procedureTask.Steps = nil
				err := resolver.attachIdentities.ResolveTaskIdentity(
					ctx,
					candidate.current,
					task.ID,
					func(identity AttachPlanIdentity) error {
						if identity.Authentication != candidate.authentication {
							return errs.New(
								errs.KindStateConflict,
								"Blueprint Attach encrypted authentication mode changed",
							)
						}
						_, buildErr := BuildAttachProvisionSteps(
							procedureTask, candidate.current.Record, candidate.adapterKey, identity,
						)
						return buildErr
					},
				)
				if err != nil {
					clearAdapterProcedurePasswords(steps)
					return nil, stepIndex, err
				}
			}
			continue
		}
		end := stepIndex + candidate.stepCount
		if end > len(task.Steps) {
			clearAdapterProcedurePasswords(steps)
			return nil, stepIndex, errs.New(errs.KindInternal, "Blueprint Attach procedure steps are incomplete")
		}
		procedureTask := task
		procedureTask.Type = taskjournal.TaskAttach
		procedureTask.Target = candidate.current.Record.ID
		procedureTask.Steps = append([]taskjournal.TaskStepRecord(nil), task.Steps[stepIndex:end]...)
		var procedures []*agentpb.ExecutionStep
		err := resolver.attachIdentities.ResolveTaskIdentity(
			ctx,
			candidate.current,
			task.ID,
			func(identity AttachPlanIdentity) error {
				if identity.Authentication != candidate.authentication {
					return errs.New(
						errs.KindStateConflict,
						"Blueprint Attach encrypted authentication mode changed",
					)
				}
				var buildErr error
				procedures, buildErr = BuildAttachProvisionSteps(
					procedureTask, candidate.current.Record, candidate.adapterKey, identity,
				)
				return buildErr
			},
		)
		if err != nil {
			clearAdapterProcedurePasswords(steps)
			return nil, stepIndex, err
		}
		steps = append(steps, procedures...)
		stepIndex = end
	}
	return steps, stepIndex, nil
}
