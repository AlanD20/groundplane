package releaseruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ConfigurationEffects owns the captured file set and its publication proof.
// Inspect may verify an interrupted write, but must not select current desired
// files or silently rewrite them under the old acknowledgement.
type ConfigurationEffects interface {
	Apply(context.Context, releaseexecution.Input, releaseexecution.CheckpointReport) (workexecution.Input, error)
	Inspect(context.Context, releaseexecution.Input, releaseexecution.CheckpointReport) (workexecution.Input, error)
}

// HookEffects consumes only ActiveHook's immutable input. Its Inspect method
// uses Script checkpoints and cleanup evidence; it cannot rerun a lost hook.
type HookEffects interface {
	Execute(context.Context, releaseexecution.Input, releaseexecution.CheckpointReport, *agentpb.ReleaseWorkHook) (workexecution.Input, error)
	Inspect(context.Context, releaseexecution.Input, releaseexecution.CheckpointReport, *agentpb.ReleaseWorkHook) (workexecution.Input, error)
}

// EffectEvidence persists the exact action result before publication. Resume
// reports whether a saved result exists, not whether the mutation happened.
type EffectEvidence interface {
	RecordReadiness(context.Context, releaseexecution.CheckpointReport, *agentpb.ObservedProject) (workexecution.Input, error)
	RecordDrain(context.Context, releaseexecution.CheckpointReport, time.Time) (workexecution.Input, error)
	RecordHelper(context.Context, releaseexecution.CheckpointReport, *agentpb.ReleaseHelperResponse) (workexecution.Input, error)
	Resume(context.Context, releaseexecution.CheckpointReport) (workexecution.Input, bool, error)
}

type Driver struct {
	configuration ConfigurationEffects
	hooks         HookEffects
	helper        MutationHelper
	observer      Observer
	evidence      EffectEvidence
}

func NewDriver(configuration ConfigurationEffects, hooks HookEffects, helper MutationHelper, observer Observer, evidence EffectEvidence) (*Driver, error) {
	if configuration == nil || hooks == nil || helper == nil || observer == nil || evidence == nil {
		return nil, errs.New(errs.KindInternal, "Release driver requires configuration, hooks, helper, observation and evidence owners")
	}
	return &Driver{configuration: configuration, hooks: hooks, helper: helper, observer: observer, evidence: evidence}, nil
}

func (driver *Driver) Execute(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport) (workexecution.Input, error) {
	return driver.run(ctx, input, report, false)
}

func (driver *Driver) Inspect(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport) (workexecution.Input, error) {
	return driver.run(ctx, input, report, true)
}

func (driver *Driver) run(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport, inspect bool) (workexecution.Input, error) {
	if driver == nil || ctx == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(input) != nil || !report.Progress.InFlight {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release effect requires an acknowledged active boundary")
	}
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release effect receipt names another action")
	}
	switch report.Progress.Phase {
	case releaseexecution.Configuration:
		if inspect {
			return driver.configuration.Inspect(ctx, input, report)
		}
		return driver.configuration.Apply(ctx, input, report)
	case releaseexecution.PreHooks, releaseexecution.PostHooks:
		hook, err := report.Progress.ActiveHook(input)
		if err != nil {
			return workexecution.Input{}, err
		}
		if inspect {
			return driver.hooks.Inspect(ctx, input, report, hook)
		}
		return driver.hooks.Execute(ctx, input, report, hook)
	case releaseexecution.Rollout:
		return driver.rollout(ctx, input, report, inspect)
	default:
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release cursor does not select an executable phase")
	}
}

func (driver *Driver) rollout(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport, inspect bool) (workexecution.Input, error) {
	// Reuse exact saved proof even if its upload receipt was lost. This applies
	// to readiness and drain as well as helper mutations; reconnect cannot renew
	// the original deadline or replace an already-persisted result.
	if reference, found, err := driver.evidence.Resume(ctx, report); err != nil || found {
		return reference, err
	}
	cursor := report.Progress
	if cursor.Action == releaseexecution.WaitCandidate || cursor.Rolling != nil && cursor.Rolling.Phase == rollingrelease.WaitCandidateHealthy {
		observed, err := WaitCandidate(ctx, driver.observer, input, report, nil)
		if err != nil {
			return workexecution.Input{}, err
		}
		return driver.evidence.RecordReadiness(ctx, report, observed)
	}
	if cursor.Rolling != nil && cursor.Rolling.Phase == rollingrelease.DrainPredecessor {
		if err := WaitDrain(ctx, input, report); err != nil {
			return workexecution.Input{}, err
		}
		return driver.evidence.RecordDrain(ctx, report, time.Now().UTC())
	}
	// A lost upload receipt is retried from durable bytes. Without those bytes,
	// inspection may prove completion but can never replay the host mutation.
	var response *agentpb.ReleaseHelperResponse
	var err error
	if inspect {
		response, err = InspectMutation(ctx, driver.helper, input, report)
	} else {
		response, err = Mutate(ctx, driver.helper, input, report)
	}
	if err != nil {
		return workexecution.Input{}, err
	}
	return driver.evidence.RecordHelper(ctx, report, response)
}
