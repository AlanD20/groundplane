package releaseoperation

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

const publicationCurrentAttachNetwork = "gp_attach_net_01arz3ndektsv4rrffq69g5faw"

// This fixture represents the durable result of a completed replacement Attach;
// the original Attach is absent. The publisher must read these records rather
// than the earlier Entry artifact. Adapter execution is proved by Attach tests.
func seedPublicationReadyAttach(t *testing.T, store *directPublicationStore, environmentID, serviceID string) {
	t.Helper()
	attachID, taskID := ids.New(ids.KindAttach), ids.New(ids.KindTask)
	record, err := etcd.NewPendingAttachRecord(
		attachID, environmentID, "current-backing", ids.New(ids.KindProject), ids.New(ids.KindEnvironment),
		ids.New(ids.KindService), "net_01ARZ3NDEKTSV4RRFFQ69G5FAW", serviceID, attachID, nil,
		[]etcd.AttachFactSetMetadata{{Facts: []etcd.AttachFactDefinition{{Key: "HOST"}}}}, taskID, time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	record, err = etcd.MarkAttachProvisioning(record, taskID)
	if err != nil {
		t.Fatal(err)
	}
	record, err = etcd.CompleteAttachProvisioning(record, taskID, true)
	if err != nil {
		t.Fatal(err)
	}
	value, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Transact(context.Background(), nil, []etcd.Mutation{
		{Type: etcd.MutationPut, Key: "/v1/records/attaches/" + attachID, Value: value},
		{Type: etcd.MutationPut, Key: "/v1/indexes/attaches/by-owner/environment/" + environmentID + "/" + attachID,
			Value: []byte(attachID)},
	})
	if err != nil {
		t.Fatal(err)
	}
}
