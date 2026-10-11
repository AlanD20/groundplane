package hierarchydeletionplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Check publication advances the ancestor coordination epochs. Their deletion
// CAS fences this scan, including draft checks without a saved Connector.
func (repository *Planner) requireConnectorChecksAbsent(
	ctx context.Context,
	revision int64,
	environmentID string,
) error {
	read, err := repository.store.Range(ctx, keyvalue.RangeRequest{
		Prefix: connectorchecks.EnvironmentReferencePrefix(environmentID), Limit: 1, Revision: revision,
	})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) > 1 {
		return errs.New(errs.KindInternal, "connector check ownership read is incomplete")
	}
	defer keyvalue.ClearRangeValues(read.Values)
	if len(read.Values) == 0 {
		return nil
	}
	taskID := string(read.Values[0].Value)
	if ids.Validate(ids.KindTask, taskID) != nil ||
		read.Values[0].Key != connectorchecks.EnvironmentReferenceKey(environmentID, taskID) {
		return errs.New(errs.KindInternal, "connector check Environment ownership is corrupt")
	}
	return errs.New(
		errs.KindResourceInUse,
		"Environment has a connection check or pending test-object cleanup; finish it before removal",
	)
}
