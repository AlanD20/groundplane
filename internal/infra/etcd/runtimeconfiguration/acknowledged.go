package runtimeconfiguration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadAcknowledged reads the file authority at the caller's planning revision.
// A missing head means no configuration has been acknowledged, not permission
// to reconstruct it from desired state or a historical Task.
func (repository *Repository) ReadAcknowledged(ctx context.Context, environmentID string, revision int64) (Snapshot, int64, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || revision <= 0 {
		return Snapshot{}, 0, errs.New(errs.KindValidationFailed, "configuration read requires an Environment and captured revision")
	}
	keys := []string{HeadKey(environmentID), environmentprojection.EnvironmentComposeProjectionStorageKey(environmentID)}
	read, err := repository.read(ctx, keys, revision)
	if err != nil {
		return Snapshot{}, 0, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return Snapshot{}, 0, errs.New(errs.KindStateConflict, "acknowledged configuration read is incomplete")
	}
	defer clearKeyValues(read.Values)
	value := read.Values[0]
	if value == nil {
		if read.Values[1] != nil {
			return Snapshot{}, 0, errs.New(errs.KindStateConflict, "applied Environment has no acknowledged configuration")
		}
		return Snapshot{}, 0, nil
	}
	if value.Key != keys[0] || value.ModRevision <= 0 || value.ModRevision > revision {
		return Snapshot{}, 0, errs.New(errs.KindStateConflict, "acknowledged configuration revision is invalid")
	}
	reference, err := DecodeReference(value.Value)
	if err != nil {
		return Snapshot{}, 0, err
	}
	if reference.EnvironmentID != environmentID {
		return Snapshot{}, 0, errs.New(errs.KindStateConflict, "acknowledged configuration belongs to another Environment")
	}
	snapshot, err := repository.Load(ctx, reference, revision)
	return snapshot, value.ModRevision, err
}
