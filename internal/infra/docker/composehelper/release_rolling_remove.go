package composehelper

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ExecuteReleaseReplicaRemoval removes only the replica selected by the durable
// rolling cursor. The rest of the predecessor set remains owned and untouched.
func ExecuteReleaseReplicaRemoval(
	ctx context.Context,
	taskRunner runner.Runner,
	input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
) (PreviousRemovalResult, error) {
	result := PreviousRemovalResult{}
	if ctx == nil || taskRunner == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(
			input,
		) != nil || !report.Progress.InFlight || report.Progress.Phase != releaseexecution.Rollout || report.Progress.Rolling == nil {
		return result, invalidReleaseHelper()
	}
	cursor := report.Progress.Rolling
	if cursor.Phase != rollingrelease.RemovePredecessor && cursor.Phase != rollingrelease.RemoveRetained {
		return result, invalidReleaseHelper()
	}
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID {
		return result, invalidReleaseHelper()
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(input.Attempt, input.Release)
	if err != nil {
		return result, err
	}
	if artifacts.Rolling == nil || artifacts.Prior == nil {
		return result, invalidReleaseHelper()
	}
	changes, err := artifacts.Rolling.Changes()
	if err != nil {
		return result, err
	}
	change := changes[cursor.Change-1]
	replica := change.Retire
	if cursor.Phase == rollingrelease.RemoveRetained {
		replica = change.Retained
	} else if time.Now().Before(cursor.DrainUntil) {
		return result, errs.New(errs.KindStateConflict, "rolling predecessor drain has not finished")
	}
	if replica == nil {
		return result, invalidReleaseHelper()
	}
	deadline := input.Attempt.Deadline
	if cursor.ReplicaDeadline.Before(deadline) {
		deadline = cursor.ReplicaDeadline
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	prior := proto.CloneOf(artifacts.Prior.Artifact)
	if artifacts.Prior.Retained != nil {
		prior.Services = append(prior.Services, proto.CloneOf(artifacts.Prior.Retained).Services...)
	}
	inspect := func() (servingInventory, error) {
		if err := bounded.Err(); err != nil {
			return servingInventory{}, err
		}
		inventory, err := inspectServingInventory(
			bounded,
			taskRunner,
			prior,
			artifacts.Candidate,
			input.Release.ServiceId,
			input.Release.ReleaseId,
			artifacts.Prior.Proxy,
		)
		if err != nil {
			return inventory, err
		}
		if artifacts.Proxy != nil {
			traffic, err := report.Progress.Traffic(input)
			if err != nil {
				return inventory, err
			}
			if inventory.proxyID == "" {
				return inventory, errs.New(errs.KindStateConflict, "rolling removal requires the serving proxy")
			}
			proven, err := observeRestorationProxy(
				bounded,
				taskRunner,
				inventory.proxyID,
				artifacts.Proxy.ProxyImplementation,
				traffic.AfterSHA256[:],
			)
			if err != nil {
				return inventory, err
			}
			if !proven {
				return inventory, errs.New(
					errs.KindStateConflict,
					"rolling removal requires proven live and restart routing away from the selected replica",
				)
			}
		}
		return inventory, nil
	}
	inventory, err := inspect()
	if err != nil {
		return result, err
	}
	ids, err := releaseReplicaRemovalIDs(bounded, taskRunner, inventory, *replica, prior, input.Release.ServiceId)
	if err != nil {
		return result, err
	}
	for _, id := range ids {
		for _, args := range [][]string{{"container", "stop", "--time", "30", "--", id}, {"container", "rm", "--", id}} {
			result.MutationAttempted = true
			if err := runReleaseProxyCommand(bounded, taskRunner, runner.RunCmdOpts{Name: DockerExecutable, Args: args, Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 32 * 1024}); err != nil {
				return result, err
			}
		}
	}
	after, err := inspect()
	if err != nil {
		return result, err
	}
	remaining, err := releaseReplicaRemovalIDs(bounded, taskRunner, after, *replica, prior, input.Release.ServiceId)
	if err != nil {
		return result, err
	}
	if len(remaining) != 0 {
		return result, errs.New(errs.KindStateConflict, "selected rolling replica remains after removal")
	}
	expected := make([]string, 0, len(inventory.predecessors))
	for _, id := range inventory.predecessors {
		if !slices.Contains(ids, id) {
			expected = append(expected, id)
		}
	}
	if !slices.Equal(after.predecessors, expected) || !slices.Equal(after.candidates, inventory.candidates) ||
		after.proxyID != inventory.proxyID {
		return result, errs.New(
			errs.KindStateConflict,
			"other Release containers changed during rolling replica removal",
		)
	}
	result.AbsenceObserved = true
	return result, nil
}

func releaseReplicaRemovalIDs(
	ctx context.Context,
	taskRunner runner.Runner,
	inventory servingInventory,
	replica rollingrelease.Replica,
	prior *agentpb.ComposeArtifact,
	serviceID string,
) ([]string, error) {
	if replica.ContainerID != "" {
		if !slices.Contains(inventory.predecessors, replica.ContainerID) {
			return nil, nil
		}
		service, err := executionplan.RollingReplicaService(prior, serviceID, replica)
		if err != nil {
			return nil, err
		}
		if err := verifyCapturedContainer(ctx, taskRunner, replica, service); err != nil {
			return nil, err
		}
		return []string{replica.ContainerID}, nil
	}
	var selected []string
	for _, id := range inventory.predecessors {
		name, err := restorationContainerComposeName(ctx, taskRunner, id)
		if err != nil {
			return nil, err
		}
		if name == replica.ComposeName {
			selected = append(selected, id)
		}
	}
	if len(selected) > 1 {
		return nil, errs.New(errs.KindStateConflict, "rolling singleton selection matched multiple containers")
	}
	return selected, nil
}
