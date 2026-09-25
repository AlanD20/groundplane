package blueprint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// authoredParentInput is pinned to the visible Task's immutable desired
// revision. The current head is intentionally not read here: it may have moved
// while this parent is settling a child or recording its abort.
type authoredParentInput struct {
	desired    projectionrecord.EnvironmentDesiredInput
	identities projectionrecord.EnvironmentOwnedIdentities
}

func (service *Service) loadAuthoredParentInput(
	ctx context.Context, parent etcd.TaskRecord,
) (authoredParentInput, error) {
	if err := etcd.ValidateTaskRecord(parent); err != nil {
		return authoredParentInput{}, err
	}
	if parent.Executor != taskjournal.TaskExecutorBlueprint ||
		parent.Owner.EnvironmentID == "" ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID {
		return authoredParentInput{}, errs.New(errs.KindValidationFailed, "Blueprint parent input identity is invalid")
	}
	environmentID := parent.Owner.EnvironmentID
	desired, found, err := service.repository.GetEnvironmentDesiredInputRevision(
		ctx, environmentID, parent.ID,
	)
	if err != nil {
		return authoredParentInput{}, err
	}
	if !found {
		return authoredParentInput{}, errs.New(errs.KindStateConflict, "Blueprint parent desired input is missing")
	}
	identities, found, err := service.repository.GetEnvironmentOwnedIdentitiesRevision(
		ctx, environmentID, parent.ID,
	)
	if err != nil {
		return authoredParentInput{}, err
	}
	if !found || desired.Record.EnvironmentID != environmentID ||
		desired.Record.RevisionID != parent.ID ||
		identities.Record.EnvironmentID != environmentID ||
		identities.Record.RevisionID != parent.ID ||
		identities.Record.RenderGeneration != desired.Record.RenderGeneration ||
		desired.Record.RenderGeneration > math.MaxInt32 ||
		int32(desired.Record.RenderGeneration) != parent.RenderGeneration {
		return authoredParentInput{}, errs.New(errs.KindStateConflict, "Blueprint parent desired input changed")
	}
	encoded, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(desired.Record)
	if err != nil {
		return authoredParentInput{}, err
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	if hex.EncodeToString(digest[:]) != parent.PlanHash {
		return authoredParentInput{}, errs.New(errs.KindStateConflict, "Blueprint parent sealed input changed")
	}
	if _, err := authoredOwnedIdentitySnapshot(ctx, desired.Record, identities.Record); err != nil {
		return authoredParentInput{}, err
	}
	if _, err := authoredEntryRecords(desired.Record, identities.Record); err != nil {
		return authoredParentInput{}, err
	}
	return authoredParentInput{
		desired:    projectionrecord.CloneEnvironmentDesiredInput(desired.Record),
		identities: projectionrecord.CloneEnvironmentOwnedIdentities(identities.Record),
	}, nil
}
