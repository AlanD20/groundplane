package connectors

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const connectorRemovalIntentPrefix = "/v1/records/connector-removal-intents/"

// ConnectorRemovalIntent pins the immutable Connector revision owned by one
// Controller cleanup Task. Terminal transitions delete the intent atomically.
type RemovalIntent struct {
	TaskID            string    `json:"task_id"`
	EnvironmentID     string    `json:"environment_id"`
	ConnectorID       string    `json:"connector_id"`
	ConnectorRevision int64     `json:"connector_revision"`
	CreatedAt         time.Time `json:"created_at"`
}

func NewRemovalIntent(
	taskID string,
	environmentID string,
	connectorID string,
	connectorRevision int64,
	createdAt time.Time,
) (RemovalIntent, error) {
	intent := RemovalIntent{
		TaskID: taskID, EnvironmentID: environmentID, ConnectorID: connectorID,
		ConnectorRevision: connectorRevision, CreatedAt: createdAt,
	}
	if err := ValidateRemovalIntent(intent); err != nil {
		return RemovalIntent{}, err
	}
	return intent, nil
}

func RemovalIntentKey(taskID string) string {
	return connectorRemovalIntentPrefix + taskID
}

func EncodeRemovalIntent(intent RemovalIntent) ([]byte, error) {
	if err := ValidateRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("connector_removal_intent", intent)
}

func DecodeRemovalIntent(value []byte) (RemovalIntent, error) {
	intent, err := recordcodec.Decode[RemovalIntent](value, "connector_removal_intent")
	if err != nil || ValidateRemovalIntent(intent) != nil {
		return RemovalIntent{}, CorruptRemovalIntent()
	}
	return intent, nil
}

func ValidateRemovalIntent(intent RemovalIntent) error {
	if recordcodec.ValidateID(ids.KindTask, intent.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindConnector, intent.ConnectorID) != nil ||
		intent.ConnectorRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "connector removal intent identity is invalid")
	}
	return recordcodec.ValidateTimestamp("connector removal intent created_at", intent.CreatedAt)
}

func CorruptRemovalIntent() error {
	return errs.New(errs.KindInternal, "connector removal intent is corrupt")
}
