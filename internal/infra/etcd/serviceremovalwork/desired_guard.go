package serviceremovalwork

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// DesiredGuard binds the captured canonical desired bytes, independently of the
// current head. Supersession is checked through native work ownership; accepting
// a newer head must never replace the input of an already accepted removal.
func (repository *Repository) DesiredGuard(
	ctx context.Context,
	input Input,
	revision int64,
) (keyvalue.Condition, error) {
	if err := input.Validate(); err != nil {
		return keyvalue.Condition{}, err
	}
	if revision <= 0 {
		return keyvalue.Condition{}, conflict("Service removal requires a consistent desired snapshot")
	}
	key := blueprints.EnvironmentBlueprintRootKey(input.Scope.EnvironmentID, input.DesiredRevisionID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil {
		return keyvalue.Condition{}, conflict("Service removal desired revision is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > revision {
		return keyvalue.Condition{}, conflict("Service removal desired revision is unavailable")
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[0].Value)
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if seal.EnvironmentID != input.Scope.EnvironmentID || seal.RevisionID != input.DesiredRevisionID ||
		seal.RenderGeneration != input.Generation || seal.BaselineHeadRevision != input.HeadRevision ||
		hex.EncodeToString(seal.ProjectionSHA256[:]) != input.DesiredSHA256 {
		return keyvalue.Condition{}, conflict("Service removal desired revision differs from its captured input")
	}
	return keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
