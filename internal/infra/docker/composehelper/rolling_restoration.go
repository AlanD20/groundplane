package composehelper

import (
	"context"
	"crypto/sha256"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Each inverse change uses the same captured routing sets as the rollout.
// Unrouted candidates can be retired first; a routed candidate must wait for
// its predecessor's readiness, confirmed proxy activation and connection drain.
func restoreRollingTraffic(ctx context.Context, taskRunner runner.Runner,
	complete, candidate *agentpb.ComposeArtifact,
	serving executionplan.NativeServingRuntime, member *agentpb.CandidateReleaseMember,
) error {
	plan, err := executionplan.OpenRollingPlan(member)
	if err != nil {
		return err
	}
	changes, err := plan.Changes()
	if err != nil {
		return err
	}
	if len(plan.Prior) == 0 {
		return errs.New(errs.KindStateConflict, "rolling restoration has no captured serving predecessor")
	}
	proxy := serving.Proxy
	inventory, err := inspectServingInventory(
		ctx,
		taskRunner,
		complete,
		candidate,
		plan.ServiceID,
		plan.ReleaseID,
		proxy,
	)
	if err != nil {
		return err
	}
	position := -1
	for index := len(changes); index >= 0; index-- {
		digest := proxy.GetProxyConfigSha256()
		if index > 0 {
			digest = member.GetRolling().GetChanges()[index-1].GetProxyConfigSha256()
		}
		matched, err := observeRestorationProxy(
			ctx,
			taskRunner,
			inventory.proxyID,
			proxy.GetProxyImplementation(),
			digest,
		)
		if err != nil {
			return err
		}
		if matched {
			position = index
			break
		}
	}
	if position < 0 {
		position, err = repairRestorationPosition(ctx, taskRunner, inventory.proxyID, proxy, member)
		if err != nil {
			return err
		}
	}
	if position < 0 {
		// Compensation may have stopped after publishing a scaled routing
		// bridge. It remains authority only for a nonempty captured prior subset.
		if plan.Prior[0].ContainerID == "" {
			return errs.New(errs.KindStateConflict, "rolling restoration proxy does not match a captured routing state")
		}
		if err := proveScaledRestorationBridge(ctx, taskRunner, inventory.proxyID, proxy, plan); err != nil {
			return err
		}
		return restoreScaledRollingTraffic(ctx, taskRunner, complete, candidate, serving, plan)
	}
	if len(inventory.candidates) > position {
		if err := waitRestorationDrain(ctx); err != nil {
			return err
		}
	}
	for index := position; index < len(changes); index++ {
		if changes[index].Candidate != nil {
			if err := retireRestorationReplica(ctx, taskRunner, complete, candidate, plan, proxy, *changes[index].Candidate, false); err != nil {
				return err
			}
		}
	}
	// Compose scaling restores the lowest missing ordinal, not an arbitrary
	// inverse ordinal. Use a verified prior subset as the temporary routing bridge.
	if plan.Prior[0].ContainerID != "" && position > 0 {
		return restoreScaledRollingTraffic(ctx, taskRunner, complete, candidate, serving, plan)
	}
	for index := position - 1; index >= 0; index-- {
		change := changes[index]
		inventory, err := inspectServingInventory(ctx, taskRunner, complete, candidate,
			plan.ServiceID, plan.ReleaseID, proxy)
		if err != nil {
			return err
		}
		beforeConfig, beforeDigest := proxy.GetProxyConfigJson(), proxy.GetProxyConfigSha256()
		if index > 0 {
			before := member.GetRolling().GetChanges()[index-1]
			beforeConfig, beforeDigest = before.GetProxyConfigJson(), before.GetProxyConfigSha256()
		}
		before, err := observeRestorationProxy(ctx, taskRunner, inventory.proxyID,
			proxy.GetProxyImplementation(), beforeDigest)
		if err != nil {
			return err
		}
		if !before {
			after := member.GetRolling().GetChanges()[index]
			matched, err := observeRestorationProxy(ctx, taskRunner, inventory.proxyID,
				proxy.GetProxyImplementation(), after.GetProxyConfigSha256())
			if err != nil {
				return err
			}
			if !matched {
				// A later change never ran. An earlier captured state is not
				// permission to create this change's predecessor or remove work.
				continue
			}
		}
		if change.Candidate != nil && before {
			if err := retireRestorationReplica(ctx, taskRunner, complete, candidate, plan,
				proxy, *change.Candidate, false); err != nil {
				return err
			}
		}
		if change.Retire != nil {
			service, err := executionplan.RollingReplicaService(serving.Artifact, plan.ServiceID, *change.Retire)
			if err != nil {
				return err
			}
			ready, err := servingPredecessorProven(ctx, taskRunner, serving.Artifact, service)
			if err != nil {
				return err
			}
			if !ready {
				replicaCtx, cancel := context.WithTimeout(ctx, time.Duration(plan.ReplicaTimeoutSeconds)*time.Second)
				err := startRestorationService(replicaCtx, taskRunner, serving.Artifact, service)
				cancel()
				if err != nil {
					return err
				}
			}
		}
		if !before {
			failure, err := activateCapturedProxy(ctx, taskRunner, inventory.proxyID,
				proxy.GetProxyImplementation(), beforeConfig)
			if err != nil || failure != nil {
				return errs.New(errs.KindRequestFailed, "rolling restoration traffic switch failed")
			}
			confirmed, err := awaitRestorationProxy(ctx, taskRunner, inventory.proxyID,
				proxy.GetProxyImplementation(), beforeDigest)
			if err != nil || !confirmed {
				return errs.New(errs.KindStateConflict, "rolling restoration routing was not confirmed")
			}
			if change.Candidate != nil {
				if err := retireRestorationReplica(ctx, taskRunner, complete, candidate, plan,
					proxy, *change.Candidate, true); err != nil {
					return err
				}
			}
		}
	}
	inventory, err = inspectServingInventory(
		ctx,
		taskRunner,
		complete,
		candidate,
		plan.ServiceID,
		plan.ReleaseID,
		proxy,
	)
	if err != nil {
		return err
	}
	confirmed, err := restorationProxyProven(ctx, taskRunner, inventory.proxyID, proxy, false)
	if err != nil || !confirmed {
		return errs.New(errs.KindStateConflict, "rolling restoration did not reach the captured prior routing")
	}
	return nil
}

func restorationContainerComposeName(ctx context.Context, taskRunner runner.Runner, id string) (string, error) {
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable,
		Args: []string{
			"container",
			"inspect",
			"--format",
			"{{index .Config.Labels \"com.docker.compose.service\"}}",
			id,
		},
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true})
	if err != nil || result.ExitCode != 0 {
		return "", errs.New(errs.KindRequestFailed, "restoration container identity could not be read")
	}
	return strings.TrimSpace(string(result.Stdout)), nil
}

func restoreScaledRollingTraffic(ctx context.Context, taskRunner runner.Runner,
	complete, candidate *agentpb.ComposeArtifact, serving executionplan.NativeServingRuntime, plan rollingrelease.Plan,
) error {
	proxy := serving.Proxy
	inventory, err := inspectServingInventory(
		ctx,
		taskRunner,
		complete,
		candidate,
		plan.ServiceID,
		plan.ReleaseID,
		proxy,
	)
	if err != nil {
		return err
	}
	service, err := executionplan.RollingReplicaService(serving.Artifact, plan.ServiceID, plan.Prior[0])
	if err != nil {
		return err
	}
	count := uint32(len(inventory.predecessors))
	if count > uint32(len(plan.Prior)) {
		return foreignRestorationRuntime()
	}
	partial := proto.CloneOf(service)
	partial.ExpectedReplicas = max(count, 1)
	if count == 0 {
		base := []string{"compose", "--project-name", serving.Artifact.GetProjectName(), "--project-directory",
			composeProjectDirectory(serving.Artifact), "--file", "-"}
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable,
			Args: append(
				base,
				"up",
				"--detach",
				"--no-deps",
				"--no-recreate",
				"--scale",
				service.GetComposeName()+"=1",
				"--",
				service.GetComposeName(),
			),
			Dir: composeProjectDirectory(serving.Artifact), Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
			Stdin: slices.Clone(serving.Artifact.GetCanonicalYaml())})
		if err != nil || result.ExitCode != 0 {
			return errs.New(errs.KindRequestFailed, "scaled predecessor startup failed")
		}
	}
	replicaCtx, cancel := context.WithTimeout(ctx, time.Duration(plan.ReplicaTimeoutSeconds)*time.Second)
	ready, err := waitServingMember(replicaCtx, taskRunner, serving.Artifact, []*agentpb.ComposeService{partial}, nil)
	cancel()
	if err != nil || !ready {
		return errs.New(errs.KindStateConflict, "scaled predecessor routing bridge did not become ready")
	}
	inventory, err = inspectServingInventory(
		ctx,
		taskRunner,
		complete,
		candidate,
		plan.ServiceID,
		plan.ReleaseID,
		proxy,
	)
	if err != nil {
		return err
	}
	configuration, err := executionplan.OpenProxyConfiguration(proxy)
	if err != nil {
		return err
	}
	configuration.Upstreams = nil
	for _, id := range inventory.predecessors {
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable,
			Args: []string{"container", "inspect", "--format", "{{.Name}}", id}, Dir: WorkDirectory,
			Env: slices.Clone(fixedEnvironment), ReplaceEnv: true})
		if err != nil || result.ExitCode != 0 {
			return errs.New(errs.KindRequestFailed, "scaled predecessor routing identity could not be read")
		}
		name := strings.TrimPrefix(strings.TrimSpace(string(result.Stdout)), "/")
		captured := slices.ContainsFunc(
			plan.Prior,
			func(replica rollingrelease.Replica) bool { return replica.ContainerName == name },
		)
		if !captured {
			return foreignRestorationRuntime()
		}
		configuration.Upstreams = append(configuration.Upstreams, name)
	}
	slices.Sort(configuration.Upstreams)
	manifest, err := serviceproxy.Render(configuration)
	if err != nil {
		return err
	}
	failure, err := activateCapturedProxy(ctx, taskRunner, inventory.proxyID, proxy.GetProxyImplementation(), manifest)
	if err != nil || failure != nil {
		return errs.New(errs.KindRequestFailed, "scaled predecessor routing bridge failed")
	}
	digest := sha256.Sum256(manifest)
	confirmed, err := awaitRestorationProxy(
		ctx,
		taskRunner,
		inventory.proxyID,
		proxy.GetProxyImplementation(),
		digest[:],
	)
	if err != nil || !confirmed {
		return errs.New(errs.KindStateConflict, "scaled predecessor routing bridge was not confirmed")
	}
	if len(inventory.candidates) != 0 {
		if err := waitRestorationDrain(ctx); err != nil {
			return err
		}
	}
	for _, replica := range plan.Candidate {
		if err := retireRestorationReplica(ctx, taskRunner, complete, candidate, plan, proxy, replica, false); err != nil {
			return err
		}
	}
	return nil
}

func startRestorationService(ctx context.Context, taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact, service *agentpb.ComposeService,
) error {
	base := []string{"compose", "--project-name", artifact.GetProjectName(), "--project-directory",
		composeProjectDirectory(artifact), "--file", "-"}
	for _, suffix := range [][]string{{"config", "--quiet"},
		{"up", "--detach", "--no-deps", "--no-recreate", "--", service.GetComposeName()}} {
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable,
			Args: append(slices.Clone(base), suffix...), Dir: composeProjectDirectory(artifact),
			Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, Stdin: slices.Clone(artifact.GetCanonicalYaml())})
		if err != nil || result.ExitCode != 0 {
			return errs.New(errs.KindRequestFailed, "captured predecessor startup failed")
		}
	}
	ready, err := waitServingMember(ctx, taskRunner, artifact, []*agentpb.ComposeService{service}, nil)
	if err != nil || !ready {
		return errs.New(errs.KindStateConflict, "captured predecessor did not become ready")
	}
	return nil
}

func retireRestorationReplica(ctx context.Context, taskRunner runner.Runner,
	complete, candidate *agentpb.ComposeArtifact, plan rollingrelease.Plan,
	proxy *agentpb.ComposeService, replica rollingrelease.Replica, drain bool,
) error {
	inventory, err := inspectServingInventory(ctx, taskRunner, complete, candidate,
		plan.ServiceID, plan.ReleaseID, proxy)
	if err != nil {
		return err
	}
	service, err := executionplan.RollingReplicaService(candidate, plan.ServiceID, replica)
	if err != nil {
		return err
	}
	for _, id := range inventory.candidates {
		labels, err := restorationContainerComposeName(ctx, taskRunner, id)
		if err != nil {
			return err
		}
		if labels != service.GetComposeName() {
			continue
		}
		if drain {
			if err := waitRestorationDrain(ctx); err != nil {
				return err
			}
			// Re-observe exact owner and image after waiting; a name cannot
			// authorize deletion of a substituted container.
			after, err := inspectServingInventory(
				ctx,
				taskRunner,
				complete,
				candidate,
				plan.ServiceID,
				plan.ReleaseID,
				proxy,
			)
			if err != nil {
				return err
			}
			if !slices.Contains(after.candidates, id) {
				continue
			}
		}
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable,
			Args: []string{"container", "rm", "--force", "--", id}, Dir: WorkDirectory,
			Env: slices.Clone(fixedEnvironment), ReplaceEnv: true})
		if err != nil || result.ExitCode != 0 {
			return errs.New(errs.KindRequestFailed, "withdrawn rolling candidate could not be removed")
		}
	}
	return nil
}

func waitRestorationDrain(ctx context.Context) error {
	timer := time.NewTimer(time.Duration(rollingrelease.DrainSeconds) * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
