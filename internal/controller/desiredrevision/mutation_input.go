package desiredrevision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type MutationDesiredInputReader interface {
	GetEnvironmentDesiredInput(
		context.Context, string,
	) (keyvalue.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error)
}

func DeriveCurrentMutationDesiredInput(
	ctx context.Context,
	reader MutationDesiredInputReader,
	environmentID string,
	expectedRevisionID string,
	candidate projectionrecord.EnvironmentComposeProjection,
	networkPool string,
	mutate func(*core.BlueprintDesiredInput) error,
) (projectionrecord.EnvironmentDesiredInput, error) {
	if ctx == nil || reader == nil || environmentID != candidate.EnvironmentID {
		return projectionrecord.EnvironmentDesiredInput{}, errs.New(
			errs.KindValidationFailed, "Environment desired mutation reader is invalid",
		)
	}
	current, found, err := reader.GetEnvironmentDesiredInput(ctx, environmentID)
	if err != nil {
		return projectionrecord.EnvironmentDesiredInput{}, err
	}
	if found != (expectedRevisionID != "") || found && current.Record.RevisionID != expectedRevisionID {
		return projectionrecord.EnvironmentDesiredInput{}, errs.New(
			errs.KindStateConflict, "Environment desired mutation head changed",
		)
	}
	if found {
		return DeriveMutationDesiredInput(&current.Record, candidate, networkPool, mutate)
	}
	return DeriveMutationDesiredInput(nil, candidate, networkPool, mutate)
}

// DeriveMutationDesiredInput advances a fixed normalized revision for one
// direct mutation. The candidate projection supplies only the Compose-backed
// fields; the owning action must change its own typed input in mutate. Other
// authored decisions are retained from the exact prior revision, never
// reconstructed from a lossy runtime projection or an audit record.
func DeriveMutationDesiredInput(
	previous *projectionrecord.EnvironmentDesiredInput,
	candidate projectionrecord.EnvironmentComposeProjection,
	networkPool string,
	mutate func(*core.BlueprintDesiredInput) error,
) (projectionrecord.EnvironmentDesiredInput, error) {
	if candidate.EnvironmentID == "" || candidate.RevisionID == "" || candidate.RenderGeneration == 0 ||
		mutate == nil {
		return projectionrecord.EnvironmentDesiredInput{}, errs.New(
			errs.KindValidationFailed, "Environment desired mutation input is invalid",
		)
	}
	if err := projectionrecord.ValidateEnvironmentComposeProjection(candidate); err != nil {
		return projectionrecord.EnvironmentDesiredInput{}, err
	}
	var input core.BlueprintDesiredInput
	if previous != nil {
		if err := projectionrecord.ValidateEnvironmentDesiredInput(*previous); err != nil ||
			previous.EnvironmentID != candidate.EnvironmentID ||
			previous.RevisionID == candidate.RevisionID ||
			previous.RenderGeneration+1 != candidate.RenderGeneration ||
			previous.RenderGeneration+1 == 0 {
			return projectionrecord.EnvironmentDesiredInput{}, errs.New(
				errs.KindStateConflict, "Environment desired mutation baseline changed",
			)
		}
		input = core.CloneBlueprintDesiredInput(previous.Input)
	}
	input.NormalizedCompose = append([]byte(nil), candidate.NormalizedCompose...)
	input.RuntimeFiles = core.CloneBlueprintDesiredInput(core.BlueprintDesiredInput{
		RuntimeFiles: candidate.RuntimeFiles,
	}).RuntimeFiles
	input.ServiceExtensions = core.CloneBlueprintDesiredInput(core.BlueprintDesiredInput{
		ServiceExtensions: candidate.ServiceExtensions,
	}).ServiceExtensions
	input.NetworkPool = networkPool
	if err := mutate(&input); err != nil {
		return projectionrecord.EnvironmentDesiredInput{}, err
	}
	return projectionrecord.NewEnvironmentDesiredInput(
		candidate.EnvironmentID, candidate.RevisionID, candidate.RenderGeneration, input,
	)
}
