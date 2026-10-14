package etcd

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type maintenanceMetadataClient struct {
	fakeClient
	mu      sync.Mutex
	value   []byte
	losePut bool
	puts    int
}

func (client *maintenanceMetadataClient) Get(
	_ context.Context,
	key string,
	_ ...clientv3.OpOption,
) (*clientv3.GetResponse, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	response := &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 100}}
	if key == "/groundplane"+storageMaintenanceKey {
		response.Kvs = []*mvccpb.KeyValue{{Key: []byte(key), Value: bytes.Clone(client.value), ModRevision: 100}}
	}
	return response, nil
}

func (client *maintenanceMetadataClient) Put(
	_ context.Context,
	_ string,
	value string,
	_ ...clientv3.OpOption,
) (*clientv3.PutResponse, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.puts++
	client.value = []byte(value)
	if client.losePut {
		client.losePut = false
		return nil, status.Error(codes.Unavailable, "lost reservation response")
	}
	return &clientv3.PutResponse{Header: &etcdserverpb.ResponseHeader{Revision: 100}}, nil
}

type maintenanceRPC struct {
	etcdserverpb.UnimplementedMaintenanceServer
	etcdserverpb.UnimplementedKVServer
	metadata  *maintenanceMetadataClient
	compacted atomic.Int64
	defrags   atomic.Int32
}

func (server *maintenanceRPC) Status(
	context.Context,
	*etcdserverpb.StatusRequest,
) (*etcdserverpb.StatusResponse, error) {
	size, used := int64(256<<20), int64(128<<20)
	if server.compacted.Load() > 0 {
		used = 16 << 20
	}
	if server.defrags.Load() > 0 {
		size = used
	}
	return &etcdserverpb.StatusResponse{
		Header:      &etcdserverpb.ResponseHeader{Revision: 100},
		DbSize:      size,
		DbSizeInUse: used,
		DbSizeQuota: 2 << 30,
	}, nil
}

func (server *maintenanceRPC) Compact(
	_ context.Context,
	request *etcdserverpb.CompactionRequest,
) (*etcdserverpb.CompactionResponse, error) {
	server.metadata.mu.Lock()
	value := bytes.Clone(server.metadata.value)
	server.metadata.mu.Unlock()
	record, err := decodeMaintenanceRecord(value)
	if err != nil || record.ReservedThrough < request.Revision || !request.Physical {
		return nil, status.Error(codes.Internal, "compaction preceded its durable reservation")
	}
	server.compacted.Store(request.Revision)
	return &etcdserverpb.CompactionResponse{Header: &etcdserverpb.ResponseHeader{Revision: 100}}, nil
}

func (server *maintenanceRPC) Defragment(
	context.Context,
	*etcdserverpb.DefragmentRequest,
) (*etcdserverpb.DefragmentResponse, error) {
	if server.compacted.Load() == 0 {
		return nil, status.Error(codes.Internal, "defrag preceded eligible history collection")
	}
	server.defrags.Add(1)
	return &etcdserverpb.DefragmentResponse{Header: &etcdserverpb.ResponseHeader{Revision: 100}}, nil
}

func TestMaintenanceReservationLossAndDefragWindow(t *testing.T) {
	// HOST-17/18: cutoff reservation must survive an uncertain response before
	// compaction, and SDK defrag must obey the window and report actual savings.
	for _, mode := range []string{"lost_reservation", "outside_window", "inside_window", "corrupt_record"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
			value, err := recordcodec.Encode(
				"storage-maintenance",
				maintenanceRecord{Samples: []historySample{{At: now.Add(-2 * time.Hour), Revision: 20}}},
			)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "corrupt_record" {
				value = []byte("invalid persisted metadata")
			}
			metadata := &maintenanceMetadataClient{value: value, losePut: mode == "lost_reservation"}
			rpc := &maintenanceRPC{metadata: metadata}
			listener := bufconn.Listen(1 << 20)
			grpcServer := grpc.NewServer()
			etcdserverpb.RegisterMaintenanceServer(grpcServer, rpc)
			etcdserverpb.RegisterKVServer(grpcServer, rpc)
			go func() { _ = grpcServer.Serve(listener) }()
			t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
			client, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://maintenance.test:2379"},
				DialOptions: []grpc.DialOption{
					grpc.WithContextDialer(
						func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) },
					),
				}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			store, err := newStore(metadata, "/groundplane/")
			if err != nil {
				t.Fatal(err)
			}
			store.backend = client
			policy := config.DefaultMaintenancePolicy()
			if mode == "inside_window" {
				policy.DefragStartMinuteUTC = 12 * 60
			}
			maintenance := &StorageMaintenance{store: store, policy: policy, walDirectory: t.TempDir()}
			err = maintenance.Tick(t.Context(), now)
			if mode == "corrupt_record" {
				if err == nil || metadata.puts != 0 || !bytes.Equal(metadata.value, value) {
					t.Fatal("maintenance repaired corrupt authority on read")
				}
				return
			}
			if mode == "lost_reservation" {
				if err == nil || rpc.compacted.Load() != 0 {
					t.Fatalf("compacted after unknown reservation: %v", err)
				}
				restarted, err := newStore(metadata, "/groundplane/")
				if err != nil {
					t.Fatal(err)
				}
				if err := restarted.loadMaintenanceFloor(t.Context()); err != nil ||
					restarted.compactedThrough.Load() != 20 {
					t.Fatalf("lost durable cutoff after restart: %v", err)
				}
				return
			}
			if err != nil || rpc.compacted.Load() != 20 {
				t.Fatalf("compaction: %d %v", rpc.compacted.Load(), err)
			}
			if mode == "outside_window" && rpc.defrags.Load() != 0 {
				t.Fatal("defrag ran outside window")
			}
			view := maintenance.Status()
			if view.HeadroomBytes != max(int64(0), view.QuotaBytes-view.BackendBytes) {
				t.Fatalf("reusable pages were reported as writable quota headroom: %+v", view)
			}
			if mode == "inside_window" {
				if rpc.defrags.Load() != 1 || view.ReclaimedBytes != 240<<20 || view.BackendBytes != 16<<20 ||
					view.ReusableBytes != 0 {
					t.Fatalf("defrag accounting: %+v", view)
				}
			}
		})
	}
}
