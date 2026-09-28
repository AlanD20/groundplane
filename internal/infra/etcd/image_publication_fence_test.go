package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// IMG-03: the real storage adapter must add the image epoch to atomic
// publication, surface a changed epoch as a conflict and keep private failure
// reads out of ordinary callers. A separate preflight read is not sufficient.
func TestPublicationRejectsChangedImageEpoch(t *testing.T) {
	ctx := imagefence.WithScope(context.Background())
	if err := imagefence.Capture(ctx, 0); err != nil {
		t.Fatal(err)
	}
	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 8}, Succeeded: false,
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Kvs: []*mvccpb.KeyValue{{Key: []byte("/groundplane" + imagefence.Key), ModRevision: 8}},
					},
				},
			},
		},
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Transact(
		ctx,
		nil,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: "/published", Value: []byte("candidate")}},
	)
	if result.Succeeded || !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("stale publication: %#v %v", result, err)
	}
	if len(backend.transaction.conditions) != 1 ||
		string(backend.transaction.conditions[0].KeyBytes()) != "/groundplane"+imagefence.Key {
		t.Fatal("image epoch was not compared atomically")
	}
	budget, err := store.MeasureTransaction(
		ctx,
		nil,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: "/published", Value: []byte("candidate")}},
	)
	if err != nil || budget.Operations != 2 {
		t.Fatalf("image fence missing from transaction budget: %#v %v", budget, err)
	}
}
