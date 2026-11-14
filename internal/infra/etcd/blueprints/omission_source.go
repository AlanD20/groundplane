package blueprints

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// OmissionSource is the retained declaration, not a claim that it ever produced
// a value, binding or host effect. Resource owners prove those independently.
type OmissionSource struct {
	Desired    environmentprojection.EnvironmentDesiredInput
	Identity   environmentprojection.OwnedIdentity
	Identities environmentprojection.EnvironmentOwnedIdentities
	Conditions []keyvalue.Condition
}

func ReadOmissionSource(ctx context.Context, store blueprintSnapshotReader,
	current environmentprojection.EnvironmentOwnedIdentities, kind ids.Kind, id string, revision int64,
) (OmissionSource, error) {
	if environmentprojection.ValidateEnvironmentOwnedIdentities(current) != nil ||
		ids.Validate(kind, id) != nil || revision <= 0 {
		return OmissionSource{}, errs.New(errs.KindValidationFailed, "Omission requires a fixed declaration snapshot")
	}
	sourceID := ""
	for _, omission := range current.PendingOmissions {
		if omission.Kind == kind && omission.ID == id {
			sourceID = omission.SourceRevisionID
		}
	}
	if sourceID == "" {
		return OmissionSource{}, errs.New(errs.KindStateConflict, "Removed resource has no retained declaration")
	}
	desired, found, err := ReadDesiredInputRevision(ctx, store, current.EnvironmentID, sourceID, revision)
	if err != nil {
		return OmissionSource{}, err
	}
	if !found {
		return OmissionSource{}, errs.New(errs.KindStateConflict, "Removed resource declaration is unavailable")
	}
	owned, found, err := ReadOwnedIdentitiesRevision(ctx, store, current.EnvironmentID, sourceID, revision)
	if err != nil {
		return OmissionSource{}, err
	}
	if !found || owned.Record.RenderGeneration != desired.Record.RenderGeneration {
		return OmissionSource{}, errs.New(errs.KindStateConflict, "Removed resource identities are unavailable")
	}
	var identities []environmentprojection.OwnedIdentity
	switch kind {
	case ids.KindEnvEntry:
		identities = owned.Record.Entries
	case ids.KindAttach:
		identities = owned.Record.Attaches
	default:
		return OmissionSource{}, errs.New(errs.KindValidationFailed, "Unsupported declaration removal source")
	}
	for _, identity := range identities {
		if identity.ID == id {
			return OmissionSource{Desired: desired.Record, Identity: identity, Identities: owned.Record,
				Conditions: []keyvalue.Condition{
					{Key: EnvironmentBlueprintRootKey(current.EnvironmentID, sourceID), ModRevision: desired.Revision},
					{
						Key:         EnvironmentBlueprintOwnedIdentitiesKey(current.EnvironmentID, sourceID),
						ModRevision: owned.Revision,
					},
				}}, nil
		}
	}
	return OmissionSource{}, errs.New(
		errs.KindStateConflict,
		"Removed resource is absent from its retained declaration",
	)
}
