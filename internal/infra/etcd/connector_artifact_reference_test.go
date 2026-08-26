package etcd

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: both Recovery Point and orphan indexes must reject invalid raw
// ids and canonical-key/value disagreement before reporting resource use.
func TestConnectorArtifactReferenceClassificationRejectsMalformedIndexes(t *testing.T) {
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	connectorID := ids.NewAt(ids.KindConnector, now, 2706)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2707)
	indexedPointID := ids.NewAt(ids.KindRecoveryPoint, now, 2708)
	otherPointID := ids.NewAt(ids.KindRecoveryPoint, now, 2709)
	pointKey, err := backupRecoveryPointConnectorIndexKey(connectorID, indexedPointID)
	if err != nil {
		t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", err)
	}
	orphanKey, err := backupOrphanConnectorIndexKey(connectorID, indexedPointID)
	if err != nil {
		t.Fatalf("backupOrphanConnectorIndexKey() error = %v", err)
	}
	tests := []struct {
		name  string
		index int
		value KeyValue
	}{
		{name: "Recovery Point invalid raw id", index: 1, value: KeyValue{Key: pointKey, Value: []byte("not-a-point")}},
		{
			name:  "Recovery Point key value mismatch",
			index: 1,
			value: KeyValue{Key: pointKey, Value: []byte(otherPointID)},
		},
		{name: "orphan invalid raw id", index: 2, value: KeyValue{Key: orphanKey, Value: []byte("not-a-point")}},
		{name: "orphan key value mismatch", index: 2, value: KeyValue{Key: orphanKey, Value: []byte(otherPointID)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := classifyConnectorReference(test.index, test.value, connectorID, environmentID)
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("classifyConnectorReference() error = %v, want internal", err)
			}
		})
	}
}
