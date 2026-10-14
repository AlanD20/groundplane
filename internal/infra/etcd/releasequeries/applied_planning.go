package releasequeries

import (
	"bytes"
	"context"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadPlanningScopeForAppliedProjection captures lifecycle and Release evidence
// beside the exact acknowledged projection, not a pending desired Blueprint.
func (ledger *Reader) LoadPlanningScopeForAppliedProjection(
	ctx context.Context,
	captured keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection],
) (ReleasePlanningScope, error) {
	if captured.ReadRevision <= 0 || captured.Revision <= 0 {
		return ReleasePlanningScope{}, errs.New(
			errs.KindValidationFailed,
			"applied runtime capture revision is invalid",
		)
	}
	scope, err := ledger.loadPlanningHierarchy(ctx, captured.Record.EnvironmentID, captured.ReadRevision)
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys:     []string{projectionrecord.EnvironmentComposeProjectionStorageKey(captured.Record.EnvironmentID)},
		Revision: scope.ReadRevision,
	})
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	if read == nil || read.ReadRevision != scope.ReadRevision || len(read.Values) != 1 ||
		read.Values[0] == nil || read.Values[0].ModRevision != captured.Revision {
		return ReleasePlanningScope{}, errs.New(errs.KindStateConflict, "applied runtime capture changed")
	}
	defer keyvalue.ClearValues(read.Values)
	encoded, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(captured.Record)
	if err != nil {
		return ReleasePlanningScope{}, err
	}
	defer clear(encoded)
	if !bytes.Equal(encoded, read.Values[0].Value) {
		return ReleasePlanningScope{}, errs.New(errs.KindStateConflict, "applied runtime capture bytes changed")
	}
	scope.Compose = captured
	scope.Compose.Record = projectionrecord.CloneEnvironmentComposeProjection(captured.Record)
	return scope, nil
}
