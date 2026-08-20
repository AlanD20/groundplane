package etcd

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/localdiag"
	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Rationale: local diagnostics must retain configured order and healthy
// endpoint evidence while returning a retryable aggregate for failed peers.
func TestProbeEndpointsReturnsOrderedPartialResultsAndAggregateError(t *testing.T) {
	secretCause := errors.New("password=secret")
	results, err := probeEndpoints(
		context.Background(),
		[]string{"10.0.0.2:2379", "10.0.0.1:2379"},
		func(_ context.Context, endpoint string) (endpointOutcome, error) {
			if endpoint == "10.0.0.2:2379" {
				return unavailableEndpoint(endpoint, secretCause), nil
			}
			return endpointOutcome{row: localdiag.EtcdEndpoint{Endpoint: endpoint, Healthy: true}}, nil
		},
	)
	if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || !errors.Is(err, secretCause) {
		t.Fatalf("probeEndpoints() error = %v", err)
	}
	want := []localdiag.EtcdEndpoint{
		{Endpoint: "10.0.0.2:2379", Error: endpointErrorMessage},
		{Endpoint: "10.0.0.1:2379", Healthy: true},
	}
	if !reflect.DeepEqual(results, want) {
		t.Fatalf("probeEndpoints() = %#v, want %#v", results, want)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) || strings.Contains(results[0].Error, "secret") ||
		strings.Contains(domainError.ToProblem().Detail, "secret") {
		t.Fatalf("public endpoint/aggregate error leaked transport cause: %#v / %v", results[0], err)
	}
}

// Rationale: the status RPC is the canonical endpoint observation and every
// approved field must map exactly with decimal IDs and measured latency.
func TestProbeEndpointMapsStatusAndClosesClient(t *testing.T) {
	client := &fakeEndpointStatusClient{response: &clientv3.StatusResponse{
		Header:  &etcdserverpb.ResponseHeader{MemberId: 42, Revision: 44},
		Version: "3.6.13",
		Leader:  43,
		DbSize:  8192,
	}}
	times := []time.Time{time.Unix(10, 0), time.Unix(10, 25*int64(time.Millisecond))}
	index := 0
	outcome, err := probeEndpointWith(
		context.Background(),
		"127.0.0.1:2379",
		func(context.Context, string) (endpointStatusClient, error) { return client, nil },
		func() time.Time {
			value := times[index]
			index++
			return value
		},
	)
	if err != nil {
		t.Fatalf("probeEndpointWith() error = %v", err)
	}
	want := localdiag.EtcdEndpoint{
		Endpoint: "127.0.0.1:2379", Healthy: true, Version: "3.6.13",
		MemberID: "42", LeaderID: "43", Revision: "44",
		DBSizeBytes: int64Pointer(8192), LatencyMS: int64Pointer(25),
	}
	if outcome.unavailable != nil || !reflect.DeepEqual(outcome.row, want) || !client.closed {
		t.Fatalf("probeEndpointWith() = %#v, closed=%v; want %#v", outcome, client.closed, want)
	}
}

// Rationale: a successful Status RPC may still report member alarms; those
// raw strings are private causes and must produce only the sanitized endpoint
// failure projection.
func TestProbeEndpointTreatsReportedErrorsAsUnavailable(t *testing.T) {
	client := &fakeEndpointStatusClient{response: &clientv3.StatusResponse{
		Header: &etcdserverpb.ResponseHeader{},
		Errors: []string{"alarm contains token=secret"},
	}}
	outcome, err := probeEndpointWith(
		context.Background(),
		"127.0.0.1:2379",
		func(context.Context, string) (endpointStatusClient, error) { return client, nil },
		time.Now,
	)
	if err != nil || outcome.unavailable == nil || !strings.Contains(outcome.unavailable.Error(), "token=secret") ||
		outcome.row.Error != endpointErrorMessage || strings.Contains(outcome.row.Error, "secret") {
		t.Fatalf("probeEndpointWith() = %#v, %v", outcome, err)
	}
}

// Rationale: malformed successful responses describe only their endpoint;
// they must preserve earlier rows and allow later configured peers to run.
func TestProbeEndpointsContinuesAfterMissingStatusData(t *testing.T) {
	for _, test := range []struct {
		name     string
		response *clientv3.StatusResponse
	}{
		{name: "nil response"},
		{name: "nil header", response: &clientv3.StatusResponse{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := make([]string, 0, 3)
			rows, err := probeEndpoints(
				context.Background(),
				[]string{"first:2379", "malformed:2379", "last:2379"},
				func(ctx context.Context, endpoint string) (endpointOutcome, error) {
					called = append(called, endpoint)
					if endpoint == "malformed:2379" {
						client := &fakeEndpointStatusClient{response: test.response}
						return probeEndpointWith(
							ctx,
							endpoint,
							func(context.Context, string) (endpointStatusClient, error) { return client, nil },
							time.Now,
						)
					}
					return endpointOutcome{row: localdiag.EtcdEndpoint{Endpoint: endpoint, Healthy: true}}, nil
				},
			)
			if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || len(rows) != 3 ||
				rows[0].Endpoint != "first:2379" || rows[1].Error != endpointErrorMessage ||
				rows[2].Endpoint != "last:2379" || !reflect.DeepEqual(called, []string{
				"first:2379", "malformed:2379", "last:2379",
			}) {
				t.Fatalf("probeEndpoints() = %#v, %v; called=%#v", rows, err, called)
			}
		})
	}
}

// Rationale: a failed Status call is one unavailable row, but its client still
// must close and its raw transport error must remain private.
func TestProbeEndpointSanitizesStatusFailureAndClosesClient(t *testing.T) {
	cause := errors.New("token=secret")
	client := &fakeEndpointStatusClient{statusErr: cause}
	outcome, err := probeEndpointWith(
		context.Background(),
		"127.0.0.1:2379",
		func(context.Context, string) (endpointStatusClient, error) { return client, nil },
		time.Now,
	)
	if err != nil || !errors.Is(outcome.unavailable, cause) || !client.closed ||
		outcome.row.Error != endpointErrorMessage || strings.Contains(outcome.row.Error, "secret") {
		t.Fatalf("probeEndpointWith() = %#v, %v; closed=%v", outcome, err, client.closed)
	}
}

// Rationale: the five-second child deadline describes one unavailable
// endpoint; only cancellation of the outer caller may suppress all rows and
// stop probing later configured endpoints.
func TestProbeEndpointTreatsChildDeadlineAsUnavailable(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	client := &fakeEndpointStatusClient{statusErr: context.DeadlineExceeded}
	outcome, err := probeEndpointWith(
		ctx,
		"127.0.0.1:2379",
		func(context.Context, string) (endpointStatusClient, error) { return client, nil },
		time.Now,
	)
	if err != nil || !errors.Is(outcome.unavailable, context.DeadlineExceeded) || !client.closed ||
		outcome.row.Error != endpointErrorMessage {
		t.Fatalf("probeEndpointWith() = %#v, %v; closed=%v", outcome, err, client.closed)
	}
}

// Rationale: client cleanup failure is a global adapter failure and must not
// produce a misleading partial endpoint health table.
func TestProbeEndpointTreatsCloseFailureAsGlobal(t *testing.T) {
	client := &fakeEndpointStatusClient{
		response: &clientv3.StatusResponse{Header: &etcdserverpb.ResponseHeader{}},
		closeErr: errors.New("close failed"),
	}
	_, err := probeEndpointWith(
		context.Background(),
		"127.0.0.1:2379",
		func(context.Context, string) (endpointStatusClient, error) { return client, nil },
		time.Now,
	)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) || !client.closed {
		t.Fatalf("probeEndpointWith() error/closed = %v/%v", err, client.closed)
	}
}

// Rationale: process cancellation must stop diagnostics immediately rather
// than being downgraded to an ordinary unavailable endpoint result.
func TestProbeEndpointsPreservesCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, err := probeEndpoints(ctx, []string{"127.0.0.1:2379"}, func(context.Context, string) (endpointOutcome, error) {
		called = true
		return endpointOutcome{}, nil
	})
	if !errors.Is(err, context.Canceled) || called {
		t.Fatalf("probeEndpoints() error/called = %v/%v", err, called)
	}
}

type fakeEndpointStatusClient struct {
	response  *clientv3.StatusResponse
	statusErr error
	closeErr  error
	closed    bool
}

func (client *fakeEndpointStatusClient) Status(context.Context, string) (*clientv3.StatusResponse, error) {
	return client.response, client.statusErr
}

func (client *fakeEndpointStatusClient) Close() error {
	client.closed = true
	return client.closeErr
}

func int64Pointer(value int64) *int64 {
	return &value
}
