package connectorchecks

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadLatest reads the selected saved check and its result at one revision.
// Draft checks never replace this selection, and expiry does not resurrect an
// older passing check as if it were the most recent attempt.
func (repository *Repository) LoadLatest(ctx context.Context, connectorID string) (keyvalue.Versioned[Record], error) {
	if ids.Validate(ids.KindConnector, connectorID) != nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindValidationFailed, "connector id is invalid")
	}
	key := LatestKey(connectorID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "latest connector check read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindRequestNotFound,
			"no retained saved connection check exists for this Connector",
		)
	}
	taskID := string(read.Values[0].Value)
	if ids.Validate(ids.KindTask, taskID) != nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "latest connector check selection is corrupt")
	}
	result, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{Key(taskID)}, Revision: read.ReadRevision},
	)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if result == nil || result.ReadRevision != read.ReadRevision || len(result.Values) != 1 || result.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "latest connector check result is missing")
	}
	defer keyvalue.ClearValues(result.Values)
	record, err := Decode(result.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if record.Owner.TaskID != taskID || record.Connector.Connector.ID != connectorID || record.Draft ||
		record.ConnectorRevision <= 0 {
		record.Clear()
		return keyvalue.Versioned[Record]{}, errs.New(
			errs.KindInternal,
			"latest connector check result differs from its selection",
		)
	}
	return keyvalue.Versioned[Record]{
		Record:       record,
		Revision:     result.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}
