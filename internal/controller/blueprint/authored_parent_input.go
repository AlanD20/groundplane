package blueprint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// authoredParentInput is pinned to the visible Task's immutable desired
// revision. The current head is intentionally not read here: it may have moved
// while this parent is settling a child or recording its abort.
type authoredParentInput struct {
	desired    projectionrecord.EnvironmentDesiredInput
	identities projectionrecord.EnvironmentOwnedIdentities
	owner      workexecution.Scope
}

func (service *Service) loadAuthoredParentInput(
	ctx context.Context, parent workexecution.Task,
) (authoredParentInput, error) {
	if err := parent.Validate(); err != nil {
		return authoredParentInput{}, err
	}
	if parent.Action != workexecution.ActionApply ||
		parent.Scope.EnvironmentID == "" || parent.TargetID != parent.Scope.EnvironmentID {
		return authoredParentInput{}, errs.New(errs.KindValidationFailed, "Blueprint parent input identity is invalid")
	}
	environmentID := parent.Scope.EnvironmentID
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
		identities.Record.RenderGeneration != desired.Record.RenderGeneration {
		return authoredParentInput{}, errs.New(errs.KindStateConflict, "Blueprint parent desired input changed")
	}
	encoded, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(desired.Record)
	if err != nil {
		return authoredParentInput{}, err
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	if hex.EncodeToString(digest[:]) != parent.Intent.SHA256 {
		return authoredParentInput{}, errs.New(errs.KindStateConflict, "Blueprint parent sealed input changed")
	}
	if _, err := authoredOwnedIdentitySnapshot(ctx, desired.Record, identities.Record); err != nil {
		return authoredParentInput{}, err
	}
	if _, err := projectionrecord.AuthoredEntryRecords(desired.Record, identities.Record); err != nil {
		return authoredParentInput{}, err
	}
	return authoredParentInput{
		desired:    projectionrecord.CloneEnvironmentDesiredInput(desired.Record),
		identities: projectionrecord.CloneEnvironmentOwnedIdentities(identities.Record),
		owner:      parent.Scope,
	}, nil
}
