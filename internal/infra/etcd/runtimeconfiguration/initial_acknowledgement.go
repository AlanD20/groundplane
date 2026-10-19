package runtimeconfiguration

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareInitialAcknowledgement installs a first, proven configuration head.
// Empty snapshots still establish the exact no-file predecessor for subsequent
// work. The caller joins this write to its genuine completed execution proof.
// Merely staging file sources never acknowledges their application.
func (repository *Repository) PrepareInitialAcknowledgement(ctx context.Context, reference Reference,
	revision int64,
) ([]Condition, Mutation, error) {
	if _, err := repository.Load(ctx, reference, revision); err != nil {
		return nil, Mutation{}, err
	}
	keys := []string{HeadKey(reference.EnvironmentID), publishedRootKey(reference.ID)}
	read, err := repository.read(ctx, keys, revision)
	if err != nil {
		return nil, Mutation{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 || read.Values[1] == nil {
		return nil, Mutation{}, errs.New(
			errs.KindStateConflict,
			"initial configuration acknowledgement lost its captured sources",
		)
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] != nil || read.Values[1].Key != keys[1] || read.Values[1].ModRevision <= 0 ||
		read.Values[1].ModRevision > revision {
		return nil, Mutation{}, errs.New(
			errs.KindStateConflict,
			"initial configuration acknowledgement would replace an existing head",
		)
	}
	value, err := EncodeReference(reference)
	if err != nil {
		return nil, Mutation{}, err
	}
	return []Condition{{Key: keys[0]}, {Key: keys[1], ModRevision: read.Values[1].ModRevision}},
		Mutation{Type: MutationPut, Key: keys[0], Value: value}, nil
}
