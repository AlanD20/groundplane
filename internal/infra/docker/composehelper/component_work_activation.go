package composehelper

import (
	"bytes"
	"context"
	"math"
	"time"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func observeComponentActivation(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
	action *agentpb.ComponentApply,
	catalog ComponentActionCatalog,
) (bool, error) {
	if action == nil {
		return true, nil
	}
	if catalog == nil {
		return false, errs.New(errs.KindInternal, "Component activation catalog is unavailable")
	}
	recipe, err := catalog.ResolveContainerConfigAction(action)
	if err != nil {
		return false, err
	}
	run := func(_ agentpb.ComposeHelperDiagnostic, args ...string) (runner.Result, *agentpb.ComposeHelperResponse, error) {
		result, err := runComponentWork(ctx, taskRunner, args...)
		return result, nil, err
	}
	id, _, err := selectComponentActionContainer(run, artifact, action, recipe)
	if err != nil || id == "" {
		return false, err
	}
	matches, err := containerConfigMatches(ctx, run, id, recipe.containerPath, action.ArtifactDigest)
	if err != nil || !matches {
		return false, err
	}
	switch recipe.activation {
	case registeredcatalog.ContainerConfigActivationHTTPDiscovery:
		observed, err := runComponentWork(
			ctx,
			taskRunner,
			"container",
			"exec",
			"--user",
			"envoy",
			id,
			"sh",
			"-ec",
			envoyrouter.ObservationCommand,
		)
		if err != nil {
			return false, err
		}
		return envoyrouter.VerifyObservation(observed.Stdout, action.ArtifactDigest)
	case registeredcatalog.ContainerConfigActivationCommand:
		if len(recipe.activateArgs) == 0 || recipe.activateArgs[0] != "caddy" {
			return false, errs.New(
				errs.KindValidationFailed,
				"Component command activation has no registered active-configuration observation",
			)
		}
		expected, err := runComponentWork(
			ctx,
			taskRunner,
			"container",
			"exec",
			id,
			"caddy",
			"adapt",
			"--config",
			recipe.containerPath,
			"--adapter",
			"caddyfile",
		)
		if err != nil {
			return false, err
		}
		actual, err := runComponentWork(
			ctx,
			taskRunner,
			"container",
			"exec",
			id,
			"wget",
			"--quiet",
			"--output-document=-",
			"http://127.0.0.1:2019/config/",
		)
		if err != nil {
			return false, err
		}
		want, err := jcs.Canonicalize(expected.Stdout)
		if err != nil {
			return false, err
		}
		got, err := jcs.Canonicalize(actual.Stdout)
		if err != nil {
			return false, err
		}
		return bytes.Equal(want, got), nil
	default:
		return false, errs.New(errs.KindValidationFailed, "Component configuration activation is unsupported")
	}
}

func activateComponentWork(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
	action *agentpb.ComponentApply,
	catalog ComponentActionCatalog,
) error {
	if action == nil || catalog == nil {
		return errs.New(errs.KindValidationFailed, "Component activation input is missing")
	}
	recipe, err := catalog.ResolveContainerConfigAction(action)
	if err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	seconds := math.Ceil(time.Until(deadline).Seconds())
	if !ok || seconds <= 0 || seconds > math.MaxUint32 {
		return errs.New(errs.KindStateConflict, "Component activation deadline has elapsed")
	}
	response, err := executeComponentContainerConfigAction(ctx, taskRunner, uint32(seconds), artifact, action, recipe)
	if err != nil {
		return err
	}
	if response == nil || response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		return errs.New(errs.KindRequestFailed, "Component rejected its configuration or could not activate it")
	}
	return nil
}
