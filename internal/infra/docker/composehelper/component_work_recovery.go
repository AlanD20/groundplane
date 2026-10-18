package composehelper

import (
	"context"
	"time"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func executeComponentRecovery(ctx context.Context, taskRunner runner.Runner, input componentexecution.HelperInput, catalog ComponentActionCatalog) (*agentpb.ComponentWorkHelperResponse, error) {
	if err := restoreComponentRuntime(ctx, taskRunner, input); err != nil {
		return nil, err
	}
	if err := restoreComponentActivation(ctx, taskRunner, input, catalog); err != nil {
		return nil, err
	}
	if err := waitComponentRuntime(ctx, taskRunner, input, true); err != nil {
		return nil, err
	}
	expected, err := componentexecution.CaptureRestoration(input.Attempt, input.Procedure)
	if err != nil {
		return nil, err
	}
	report := componentexecution.RecoveryReport{AttemptID: input.Attempt.ID, ForwardID: input.Recovery.Forward.ID,
		Failure: input.Recovery.Evidence, ComponentID: input.Procedure.ComponentId, Input: input.Attempt.Input,
		HelperStopped: true, FilesRestored: true, RuntimeAbsent: expected.Absent, RuntimeReady: !expected.Absent,
		RuntimeSHA256: expected.RuntimeSHA256, ConfigurationSHA256: expected.ConfigurationSHA256, ObservedAt: time.Now().UTC()}
	raw, _, err := componentexecution.EncodeRecoveryReport(input.Attempt, *input.Recovery, input.Procedure, input.Progress, report)
	if err != nil {
		return nil, err
	}
	return &agentpb.ComponentWorkHelperResponse{RecoveryReport: raw}, nil
}

func restoreComponentActivation(ctx context.Context, taskRunner runner.Runner, input componentexecution.HelperInput, catalog ComponentActionCatalog) error {
	retained := input.Procedure.Predecessor.Configuration
	if retained == nil {
		return nil
	}
	action := &agentpb.ComponentApply{ComponentId: input.Procedure.ComponentId,
		DefinitionDigest: retained.DefinitionDigest, CatalogDigest: retained.CatalogDigest, ActionId: retained.ActionId,
		ArtifactId: retained.File.MaterializationId, ArtifactDigest: retained.File.Sha256, Generation: retained.Generation}
	artifact := input.Procedure.Predecessor.Runtime
	recipe, err := catalog.ResolveContainerConfigAction(action)
	if err != nil {
		return err
	}
	ready, err := observeComponentActivation(ctx, taskRunner, artifact, action, catalog)
	if err != nil || ready {
		return err
	}
	if recipe.activation == registeredcatalog.ContainerConfigActivationHTTPDiscovery {
		run := func(_ agentpb.ComposeHelperDiagnostic, args ...string) (runner.Result, *agentpb.ComposeHelperResponse, error) {
			result, err := runComponentWork(ctx, taskRunner, args...)
			return result, nil, err
		}
		id, _, err := selectComponentActionContainer(run, artifact, action, recipe)
		if err != nil {
			return err
		}
		if id == "" {
			return errs.New(errs.KindStateConflict, "Previous Component container is unavailable for configuration restoration")
		}
		failed := retained.Source
		if transition := input.Procedure.GetConfigurationAction().GetContainerConfigTransition(); transition != nil {
			failed = transition.DesiredSource
		}
		if err := restoreHTTPDiscoveryConfiguration(ctx, taskRunner, id, retained.Source, failed); err != nil {
			return err
		}
	} else if err := activateComponentWork(ctx, taskRunner, artifact, action, catalog); err != nil {
		return err
	}
	ready, err = observeComponentActivation(ctx, taskRunner, artifact, action, catalog)
	if err != nil {
		return err
	}
	if !ready {
		return errs.New(errs.KindStateConflict, "Previous Component configuration is not active after restoration")
	}
	return nil
}
