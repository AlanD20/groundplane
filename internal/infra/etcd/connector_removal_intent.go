package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const connectorRemovalIntentPrefix = "/v1/records/connector-removal-intents/"

// ConnectorRemovalIntent pins the immutable Connector revision owned by one
// Controller cleanup Task. Terminal transitions delete the intent atomically.
type ConnectorRemovalIntent struct {
	TaskID            string    `json:"task_id"`
	EnvironmentID     string    `json:"environment_id"`
	ConnectorID       string    `json:"connector_id"`
	ConnectorRevision int64     `json:"connector_revision"`
	CreatedAt         time.Time `json:"created_at"`
}

func NewConnectorRemovalIntent(
	taskID string,
	environmentID string,
	connectorID string,
	connectorRevision int64,
	createdAt time.Time,
) (ConnectorRemovalIntent, error) {
	intent := ConnectorRemovalIntent{
		TaskID: taskID, EnvironmentID: environmentID, ConnectorID: connectorID,
		ConnectorRevision: connectorRevision, CreatedAt: createdAt,
	}
	if err := validateConnectorRemovalIntent(intent); err != nil {
		return ConnectorRemovalIntent{}, err
	}
	return intent, nil
}

func connectorRemovalIntentKey(taskID string) string {
	return connectorRemovalIntentPrefix + taskID
}

func (repository *ConnectorRepository) GetConnectorRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[ConnectorRemovalIntent], bool, error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[ConnectorRemovalIntent]{}, false, err
	}
	if recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[ConnectorRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"connector removal intent task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, connectorRemovalIntentKey(taskID))
	if err != nil {
		return etcdstore.Versioned[ConnectorRemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[ConnectorRemovalIntent]{}, false, errs.New(
			errs.KindInternal,
			"connector removal intent read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[ConnectorRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeConnectorRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return etcdstore.Versioned[ConnectorRemovalIntent]{}, false, corruptConnectorRemovalIntent()
	}
	return etcdstore.Versioned[ConnectorRemovalIntent]{
		Record: intent, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func encodeConnectorRemovalIntent(intent ConnectorRemovalIntent) ([]byte, error) {
	if err := validateConnectorRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("connector_removal_intent", intent)
}

func decodeConnectorRemovalIntent(value []byte) (ConnectorRemovalIntent, error) {
	intent, err := recordcodec.Decode[ConnectorRemovalIntent](value, "connector_removal_intent")
	if err != nil || validateConnectorRemovalIntent(intent) != nil {
		return ConnectorRemovalIntent{}, corruptConnectorRemovalIntent()
	}
	return intent, nil
}

func validateConnectorRemovalIntent(intent ConnectorRemovalIntent) error {
	if recordcodec.ValidateID(ids.KindTask, intent.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindConnector, intent.ConnectorID) != nil ||
		intent.ConnectorRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "connector removal intent identity is invalid")
	}
	return recordcodec.ValidateTimestamp("connector removal intent created_at", intent.CreatedAt)
}

func corruptConnectorRemovalIntent() error {
	return errs.New(errs.KindInternal, "connector removal intent is corrupt")
}
