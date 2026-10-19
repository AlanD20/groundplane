package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AgentDeliveries joins bounded scheduling and immutable assignment delivery.
// Sending input grants no effects: the Agent must obtain Start and checkpoints.
type AgentDeliveries struct {
	resolvers    ResolverDeliveryOwner
	backings     BackingDeliveryOwner
	environments EnvironmentDeliveryOwner
	queue        *workqueue.Repository
	scheduler    *Scheduler
	releases     ReleaseDeliveryOwner
	restorations ReleaseRestorationDeliveryOwner
	scripts      ScriptDeliveryOwner
	hierarchy    HierarchyDeliveryOwner
	detaches     DetachDeliveryOwner
	attaches     AttachDeliveryOwner
	networks     NetworkDeliveryOwner
	components   ComponentDeliveryOwner
	entries      EntryDeliveryOwner
	volumes      VolumeDeliveryOwner
	lifecycles   LifecycleDeliveryOwner
	removals     ServiceRemovalDeliveryOwner
	backups      BackupDeliveryOwner
}

func NewAgentDeliveries(
	queue *workqueue.Repository,
	scheduler *Scheduler,
	releases ReleaseDeliveryOwner,
	scripts ScriptDeliveryOwner,
	hierarchy HierarchyDeliveryOwner,
	volumes VolumeDeliveryOwner,
	networks NetworkDeliveryOwner,
	detaches DetachDeliveryOwner,
	components ComponentDeliveryOwner,
	entries EntryDeliveryOwner,
	attaches AttachDeliveryOwner,
	lifecycles LifecycleDeliveryOwner,
	removals ServiceRemovalDeliveryOwner,
	backups BackupDeliveryOwner,
	environments EnvironmentDeliveryOwner,
	restorations ReleaseRestorationDeliveryOwner,
	backings BackingDeliveryOwner,
	resolvers ResolverDeliveryOwner,
) (*AgentDeliveries, error) {
	if resolvers == nil {
		return nil, errs.New(errs.KindInternal, "Agent delivery requires a DNS resolver owner")
	}
	if restorations == nil {
		return nil, errs.New(errs.KindInternal, "Agent delivery requires a captured Release restoration owner")
	}
	if environments == nil || backings == nil {
		return nil, errs.New(errs.KindInternal, "Agent delivery requires an Environment provisioning owner")
	}
	if backups == nil {
		return nil, errs.New(errs.KindInternal, "Agent delivery requires a Backup owner")
	}
	if lifecycles == nil || removals == nil {
		return nil, errs.New(errs.KindInternal, "Agent delivery requires a Service lifecycle owner")
	}
	if queue == nil || scheduler == nil || scheduler.queue != queue || releases == nil || scripts == nil ||
		hierarchy == nil ||
		volumes == nil ||
		networks == nil ||
		detaches == nil ||
		components == nil ||
		entries == nil ||
		attaches == nil {
		return nil, errs.New(errs.KindInternal, "Agent delivery requires one queue, scheduler and Release owner")
	}
	return &AgentDeliveries{
		resolvers:    resolvers,
		backings:     backings,
		environments: environments,
		restorations: restorations,
		backups:      backups,
		removals:     removals,
		queue:        queue,
		scheduler:    scheduler,
		releases:     releases,
		scripts:      scripts,
		hierarchy:    hierarchy,
		volumes:      volumes,
		networks:     networks,
		detaches:     detaches,
		components:   components,
		entries:      entries,
		attaches:     attaches,
		lifecycles:   lifecycles,
	}, nil
}

// Dispatch keeps delivered identity only for currently reserved slots. A new
// stream starts with an empty map and redelivers saved attempts, never creates
// replacement attempts merely because a response was lost.
func (delivery *AgentDeliveries) Dispatch(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	capacity int32,
	delivered map[string]string,
	send func(*agentpb.ControllerMessage) (bool, error),
) ([]Issue, error) {
	if delivery == nil || ctx == nil || capacity < 0 || capacity > 256 || delivered == nil || send == nil {
		return nil, errs.New(errs.KindInternal, "Agent dispatch arguments are invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	identity := authorization.Identity()
	if identity.Executor != workexecution.Agent {
		return nil, errs.New(errs.KindStateConflict, "Agent dispatch requires an Agent identity")
	}
	placement := workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID}
	registration, err := delivery.queue.ReadExecutor(ctx, placement)
	if err != nil {
		return nil, err
	}
	if registration.Revision == 0 || registration.Record.Identity != identity || !registration.Record.Ready {
		return nil, errs.New(errs.KindStateConflict, "Agent must finish startup reconciliation before dispatch")
	}
	var issues []Issue
	if capacity > 0 {
		cursor, err := delivery.queue.ReadSchedulerCursor(ctx, placement)
		if err != nil {
			return nil, err
		}
		sweep, err := delivery.scheduler.Schedule(ctx, identity, cursor.Record.After, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		issues = append(issues, sweep.Issues...)
		if sweep.Next != cursor.Record.After {
			change, err := workqueue.PrepareSchedulerCursor(cursor, registration, sweep.Next)
			if err != nil {
				return issues, err
			}
			result, err := delivery.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
			keyvalue.ClearValues(result.FailureReads)
			keyvalue.ClearMutationValues(change.Mutations)
			if err != nil {
				return issues, err
			}
			// A competing sweep may win the cursor CAS. Its durable assignments
			// are still the source of delivery; never repeat an effect here.
		}
	}
	assignments, err := delivery.queue.Deliveries(ctx, identity)
	if err != nil {
		return issues, err
	}
	active := make(map[string]string, len(assignments))
	for _, assignment := range assignments {
		active[assignment.Item.Item.ID] = assignment.Attempt.Attempt.ID
	}
	for workID, attemptID := range delivered {
		if active[workID] != attemptID {
			delete(delivered, workID)
		}
	}
	// Cancel hints use no worker capacity and must be sent even when every slot
	// is occupied. Durable Stop/revocation remains the authority; a hint cannot
	// release claims or cancel a captured recovery attempt.
	for _, assignment := range assignments {
		attempt := assignment.Attempt.Attempt
		if attempt.Direction != workexecution.Forward {
			continue
		}
		view, err := delivery.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, identity)
		if err != nil {
			return issues, err
		}
		if !view.ActiveAttempt || view.Work.Task.Task.Stop == nil && !view.Work.Item.Item.ForwardRevoked {
			continue
		}
		wire, err := workexecution.EncodeAgentAttempt(attempt)
		if err != nil {
			return issues, err
		}
		_, err = send(
			&agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_WorkCancel{WorkCancel: &agentpb.WorkCancel{Attempt: wire}},
			},
		)
		if err != nil {
			return issues, err
		}
	}
	for _, assignment := range assignments {
		if capacity == 0 {
			break
		}
		if !registration.Record.Allows(assignment.Item.Item.Operation) {
			continue
		}
		attempt := assignment.Attempt.Attempt
		if delivered[attempt.WorkItemID] == attempt.ID {
			continue
		}
		view, err := delivery.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, identity)
		if err != nil {
			return issues, err
		}
		if !view.ActiveAttempt {
			continue
		}
		message, err := delivery.message(ctx, view)
		if err != nil {
			issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
			continue
		}
		sent, err := send(message)
		workexecution.ClearScriptAssignment(message.GetScriptWorkAssignment())
		attachexecution.ClearRemoval(message.GetAttachRemovalAssignment().GetRemoval())
		attachexecution.ClearRecoveryAssignment(message.GetAttachRecoveryAssignment())
		attachexecution.ClearProvision(message.GetAttachProvisionAssignment().GetProvision())
		if err != nil {
			return issues, err
		}
		if !sent {
			break
		}
		delivered[attempt.WorkItemID] = attempt.ID
		capacity--
	}
	return issues, nil
}

func (delivery *AgentDeliveries) message(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.ControllerMessage, error) {
	switch view.Work.Item.Item.Operation {
	case workexecution.Resolver:
		assignment, err := delivery.resolvers.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		input, err := resolverexecution.DecodeAssignment(assignment, view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) {
			return nil, errs.New(errs.KindStateConflict, "DNS resolver delivery differs from its captured assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ResolverWorkAssignment{ResolverWorkAssignment: assignment},
		}, nil
	case workexecution.BackingProvision:
		assignment, err := delivery.backings.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		input, err := backingprovision.DecodeAssignment(assignment, view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) {
			return nil, errs.New(
				errs.KindStateConflict,
				"backing creation delivery differs from its captured assignment",
			)
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_BackingProvisionAssignment{BackingProvisionAssignment: assignment},
		}, nil
	case workexecution.Environment:
		assignment, err := delivery.environments.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		input, err := environmentexecution.DecodeAssignment(assignment, view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) {
			return nil, errs.New(errs.KindStateConflict, "Environment delivery differs from its captured assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_EnvironmentProvisionAssignment{
				EnvironmentProvisionAssignment: assignment,
			},
		}, nil
	case workexecution.Backup, workexecution.Restore, workexecution.BackupPrune:
		if view.Attempt.Attempt.Direction == workexecution.Recovery {
			assignment, err := delivery.backups.LoadRecoveryAssignment(ctx, view)
			if err != nil {
				return nil, err
			}
			owned, attempt, _, err := executionplan.OpenBackupRecoveryAssignment(
				assignment,
				view.Attempt.Attempt.Executor,
			)
			if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
				return nil, errs.New(
					errs.KindStateConflict,
					"Backup recovery delivery differs from its durable assignment",
				)
			}
			return &agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_BackupRecoveryAssignment{BackupRecoveryAssignment: owned},
			}, nil
		}
		assignment, err := delivery.backups.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			return nil, errs.New(errs.KindStateConflict, "Backup delivery differs from its durable assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_BackupWorkAssignment{BackupWorkAssignment: owned},
		}, nil
	case workexecution.ServiceRemoval:
		return delivery.removalMessage(ctx, view)
	case workexecution.Attach:
		if view.Attempt.Attempt.Direction == workexecution.Recovery {
			assignment, err := delivery.attaches.LoadRecoveryAssignment(ctx, view)
			if err != nil {
				return nil, err
			}
			input, err := attachexecution.DecodeProvisionRecoveryAssignment(assignment, view.Attempt.Attempt.Executor)
			if err != nil || !workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) {
				return nil, errs.New(
					errs.KindStateConflict,
					"Attach inspection delivery differs from its durable assignment",
				)
			}
			return &agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_AttachProvisionRecoveryAssignment{
					AttachProvisionRecoveryAssignment: assignment,
				},
			}, nil
		}
		assignment, err := delivery.attaches.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		attempt, err := workexecution.DecodeAgentAttempt(assignment.GetAttempt(), view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			attachexecution.ClearProvision(assignment.GetProvision())
			return nil, errs.New(errs.KindStateConflict, "Attach delivery differs from its durable assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_AttachProvisionAssignment{AttachProvisionAssignment: assignment},
		}, nil
	case workexecution.EntryConfiguration:
		assignment, err := delivery.entries.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		attempt, err := workexecution.DecodeAgentAttempt(assignment.GetAttempt(), view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			return nil, errs.New(errs.KindStateConflict, "Entry delivery differs from its durable assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_EntryConfigurationWorkAssignment{
				EntryConfigurationWorkAssignment: assignment,
			},
		}, nil
	case workexecution.Component:
		assignment, err := delivery.components.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		attempt, err := workexecution.DecodeAgentAttempt(assignment.GetAttempt(), view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			return nil, errs.New(errs.KindStateConflict, "Component delivery differs from its durable assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ComponentWorkAssignment{ComponentWorkAssignment: assignment},
		}, nil
	case workexecution.Network:
		assignment, err := delivery.networks.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		attempt, err := workexecution.DecodeAgentAttempt(assignment.GetAttempt(), view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			return nil, errs.New(errs.KindStateConflict, "Network delivery differs from its durable assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_NetworkWorkAssignment{NetworkWorkAssignment: assignment},
		}, nil
	case workexecution.ServiceLifecycle:
		assignment, err := delivery.lifecycles.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		attempt, err := workexecution.DecodeAgentAttempt(assignment.GetAttempt(), view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			return nil, errs.New(
				errs.KindStateConflict,
				"Service lifecycle delivery differs from its durable assignment",
			)
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ServiceLifecycleAssignment{ServiceLifecycleAssignment: assignment},
		}, nil
	case workexecution.Volume:
		assignment, err := delivery.volumes.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		attempt, err := workexecution.DecodeAgentAttempt(assignment.GetAttempt(), view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			return nil, errs.New(errs.KindStateConflict, "Volume delivery differs from its durable assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_VolumeWorkAssignment{VolumeWorkAssignment: assignment},
		}, nil
	case workexecution.Hierarchy, workexecution.Detach:
		var message *agentpb.ControllerMessage
		var err error
		if view.Work.Item.Item.Operation == workexecution.Detach {
			message, err = delivery.detaches.DetachAssignment(ctx, view)
		} else {
			message, err = delivery.hierarchy.HierarchyAssignment(ctx, view)
		}
		if err != nil {
			return nil, err
		}
		if message == nil {
			return nil, errs.New(errs.KindStateConflict, "removal delivery has no typed assignment")
		}
		var attempt workexecution.Attempt
		switch payload := message.Payload.(type) {
		case *agentpb.ControllerMessage_EnvironmentRemovalAssignment:
			if view.Work.Item.Item.Operation != workexecution.Hierarchy {
				return nil, errs.New(errs.KindStateConflict, "Detach cannot deliver Environment removal authority")
			}
			input, decodeErr := hierarchyexecution.DecodeEnvironmentAssignment(payload.EnvironmentRemovalAssignment, view.Attempt.Attempt.Executor)
			attempt, err = input.Attempt, decodeErr
		case *agentpb.ControllerMessage_AttachRemovalAssignment:
			input, decodeErr := attachexecution.DecodeRemovalAssignment(payload.AttachRemovalAssignment, view.Attempt.Attempt.Executor)
			attempt, err = input.Attempt, decodeErr
			attachexecution.ClearRemoval(input.Removal)
		case *agentpb.ControllerMessage_AttachRecoveryAssignment:
			input, decodeErr := attachexecution.DecodeRecoveryAssignment(payload.AttachRecoveryAssignment, view.Attempt.Attempt.Executor)
			attempt, err = input.Attempt, decodeErr
			attachexecution.ClearRecoveryInput(&input)
		default:
			err = errs.New(errs.KindStateConflict, "removal delivery uses an unsupported assignment")
		}
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			attachexecution.ClearRemoval(message.GetAttachRemovalAssignment().GetRemoval())
			attachexecution.ClearRecoveryAssignment(message.GetAttachRecoveryAssignment())
			return nil, errs.New(errs.KindStateConflict, "removal delivery differs from its durable assignment")
		}
		return message, nil
	case workexecution.Script:
		if view.Attempt.Attempt.Direction == workexecution.Recovery {
			input, err := delivery.scripts.LoadRecoveryInput(ctx, view)
			if err != nil {
				return nil, err
			}
			if !workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) {
				return nil, errs.New(
					errs.KindStateConflict,
					"Script recovery delivery differs from its durable assignment",
				)
			}
			assignment, err := scriptexecution.EncodeRecoveryAssignment(input)
			if err != nil {
				return nil, err
			}
			return &agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_ScriptRecoveryAssignment{ScriptRecoveryAssignment: assignment},
			}, nil
		}
		assignment, err := delivery.scripts.LoadWorkAssignment(ctx, view)
		if err != nil {
			workexecution.ClearScriptAssignment(assignment)
			return nil, err
		}
		attempt, err := workexecution.DecodeAgentAttempt(assignment.GetAttempt(), view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) ||
			attempt.Direction != workexecution.Forward {
			workexecution.ClearScriptAssignment(assignment)
			return nil, errs.New(errs.KindStateConflict, "Script delivery differs from its durable assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ScriptWorkAssignment{ScriptWorkAssignment: assignment},
		}, nil
	case workexecution.ReleaseRestoration:
		input, err := delivery.restorations.LoadRecoveryInput(ctx, view)
		if err != nil {
			return nil, err
		}
		if input.Group == nil || !workexecution.SameAssignment(input.Input.Attempt, view.Attempt.Attempt) {
			return nil, errs.New(
				errs.KindStateConflict,
				"Group restoration delivery differs from its durable assignment",
			)
		}
		assignment, err := releaseexecution.EncodeRecoveryAssignment(input)
		if err != nil {
			return nil, err
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ReleaseRecoveryAssignment{ReleaseRecoveryAssignment: assignment},
		}, nil
	case workexecution.Release:
		if view.Attempt.Attempt.Direction == workexecution.Recovery {
			input, err := delivery.releases.LoadRecoveryInput(ctx, view)
			if err != nil {
				return nil, err
			}
			if !workexecution.SameAssignment(input.Input.Attempt, view.Attempt.Attempt) {
				return nil, errs.New(errs.KindStateConflict, "Recovery delivery differs from its durable assignment")
			}
			assignment, err := releaseexecution.EncodeRecoveryAssignment(input)
			if err != nil {
				return nil, err
			}
			return &agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_ReleaseRecoveryAssignment{ReleaseRecoveryAssignment: assignment},
			}, nil
		}
		assignment, err := delivery.releases.LoadWorkAssignment(ctx, view)
		if err != nil {
			return nil, err
		}
		attempt, _, err := workexecution.DecodeReleaseDelivery(assignment, view.Attempt.Attempt.Executor)
		if err != nil || !workexecution.SameAssignment(attempt, view.Attempt.Attempt) {
			return nil, errs.New(errs.KindStateConflict, "Release delivery differs from its durable assignment")
		}
		return &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_ReleaseWorkAssignment{ReleaseWorkAssignment: assignment},
		}, nil
	default:
		return nil, errs.New(errs.KindStateConflict, "Agent operation has no native delivery owner")
	}
}
