package composehelper

import (
	"context"
	"crypto/sha256"
	"time"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Probe is read-only. Compensation calls this after the existing exact runtime
// recreation and file restoration; successful Compose execution is insufficient
// until the captured prior restart files and full active Routes are proved.
func componentConfigurationRestorationResponse(
	ctx context.Context, taskRunner runner.Runner, request *agentpb.ComposeHelperRequest,
	artifact *agentpb.ComposeArtifact, catalog ComponentActionCatalog, wait bool,
) (*agentpb.ComposeHelperResponse, error) {
	procedure := request.GetPlan().GetComponentRecoveryProcedure()
	action, prior := executionplan.ComponentConfigurationAction(request.GetPlan()), procedure.GetPredecessor()
	if action == nil || prior == nil || catalog == nil || artifact == nil ||
		artifact.GetArtifactId() != prior.GetPriorArtifactId() {
		return nil, errs.New(errs.KindValidationFailed, "Component configuration recovery authority is missing")
	}
	recipe, err := catalog.ResolveContainerConfigAction(action)
	if err != nil {
		return nil, err
	}
	if recipe.activation != registeredcatalog.ContainerConfigActivationHTTPDiscovery {
		return nil, errs.New(errs.KindValidationFailed, "Component configuration recovery is not a discovery recipe")
	}
	if unchanged := procedure.GetRetainedConfiguration().GetUnchangedFile(); unchanged != nil &&
		unchanged.GetDestination() != recipe.relativePath {
		return nil, errs.New(errs.KindValidationFailed, "retained Component file path differs from the compiled recipe")
	}
	transition := action.GetContainerConfigTransition()
	priorSource, priorDigest := transition.GetPreviousSource(), transition.GetPreviousSourceSha256()
	var restoration envoyrouter.Restoration
	retainedOnly := procedure.GetRetainedConfiguration() != nil
	if retainedOnly {
		priorSource = procedure.GetRetainedConfiguration().GetSource()
		digest := sha256.Sum256(priorSource)
		priorDigest = digest[:]
	} else {
		restoration, err = envoyrouter.PrepareRestoration(priorSource, transition.GetDesiredSource())
		if err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.GetTimeoutSeconds())*time.Second)
	defer cancel()
	run := func(_ agentpb.ComposeHelperDiagnostic, arguments ...string) (runner.Result, *agentpb.ComposeHelperResponse, error) {
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: arguments, Dir: WorkDirectory,
			Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
			CaptureLimitBytes: envoyrouter.MaximumObservationBytes,
		})
		if ctx.Err() != nil {
			return runner.Result{}, nil, ctx.Err()
		}
		if err != nil || result.ExitCode != 0 {
			return runner.Result{}, nil, errs.New(
				errs.KindRequestFailed,
				"Component configuration inventory or observation failed",
			)
		}
		return result, nil, nil
	}
	observe := func() (bool, error) {
		containerID, _, err := selectComponentActionContainer(run, artifact, action, recipe)
		if err != nil || containerID == "" {
			return false, err
		}
		matches, err := containerConfigMatches(
			ctx,
			run,
			containerID,
			recipe.containerPath,
			priorDigest,
		)
		if err != nil || !matches {
			return false, err
		}
		// PID1 may still be entering its compiled startup command immediately
		// after recreation. Only this unsuccessful fixed read is a pending proof;
		// failed inventory and ownership mismatches never become absence.
		observed, err := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable,
			Args: []string{
				"container",
				"exec",
				"--user",
				"envoy",
				containerID,
				"sh",
				"-ec",
				envoyrouter.ObservationCommand,
			},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
			CaptureLimitBytes: envoyrouter.MaximumObservationBytes,
		})
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if err != nil {
			return false, errs.Wrap(errs.KindRequestFailed, err)
		}
		if observed.ExitCode != 0 {
			return false, nil
		}
		if retainedOnly {
			// Removal or an engine change never activated this predecessor's
			// alternative discovery configuration. Compensation recreates the exact
			// retained runtime; captured files and active resources prove restoration.
			proven, err := envoyrouter.VerifyObservation(observed.Stdout, priorDigest)
			if err != nil || !proven {
				return false, err
			}
			return containerConfigMatches(ctx, run, containerID, recipe.containerPath, priorDigest)
		}
		progress, err := restoration.Observe(observed.Stdout)
		if err != nil {
			return false, err
		}
		if !progress.Restored() {
			if !wait {
				return false, nil
			}
			if err := restoreHTTPDiscoveryConfiguration(ctx, taskRunner, containerID,
				priorSource, transition.GetDesiredSource()); err != nil {
				return false, err
			}
		}
		return containerConfigMatches(ctx, run, containerID, recipe.containerPath, priorDigest)
	}
	for {
		proven, err := observe()
		if err != nil {
			return nil, err
		}
		if proven || !wait {
			response := completedResponse()
			response.ComponentRestorationEvidence = &agentpb.ComponentRestorationEvidence{
				ComponentId: procedure.GetComponentId(), ArtifactId: prior.GetPriorArtifactId(), ServiceId: prior.GetServiceId(),
				ComposeProjectName: artifact.GetProjectName(), Target: agentpb.ComponentRestorationTarget_COMPONENT_RESTORATION_TARGET_PREDECESSOR,
				Restored: proven,
			}
			return response, nil
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
