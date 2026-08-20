package etcd

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/localdiag"
	"github.com/AlanD20/groundplane/pkg/errs"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	endpointProbeTimeout = 5 * time.Second
	endpointErrorMessage = "endpoint unavailable"
)

type endpointStatusClient interface {
	Status(context.Context, string) (*clientv3.StatusResponse, error)
	Close() error
}

type endpointClientFactory func(context.Context, string) (endpointStatusClient, error)

type endpointOutcome struct {
	row         localdiag.EtcdEndpoint
	unavailable error
}

type endpointProbe func(context.Context, string) (endpointOutcome, error)

// ProbeEndpoints directly checks every configured etcd endpoint. Results keep
// configured order and failures remain per-endpoint so one peer cannot
// suppress the rest of the diagnostic.
func ProbeEndpoints(ctx context.Context, endpoints []string) ([]localdiag.EtcdEndpoint, error) {
	return probeEndpoints(ctx, endpoints, probeEndpoint)
}

func probeEndpoints(
	ctx context.Context,
	endpoints []string,
	probe endpointProbe,
) ([]localdiag.EtcdEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(endpoints) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd diagnostics require at least one endpoint")
	}

	results := make([]localdiag.EtcdEndpoint, 0, len(endpoints))
	causes := make([]error, 0, len(endpoints))
	for _, endpoint := range endpoints {
		probeCtx, cancel := context.WithTimeout(ctx, endpointProbeTimeout)
		outcome, err := probe(probeCtx, endpoint)
		cancel()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if err != nil {
			return nil, err
		}
		outcome.row.Endpoint = endpoint
		results = append(results, outcome.row)
		if outcome.unavailable != nil {
			causes = append(causes, fmt.Errorf("%s: %w", endpoint, outcome.unavailable))
		}
	}
	if len(causes) > 0 {
		return results, errs.Wrap(errs.KindStorageUnavailable, errors.Join(causes...))
	}
	return results, nil
}

func probeEndpoint(ctx context.Context, endpoint string) (endpointOutcome, error) {
	return probeEndpointWith(ctx, endpoint, newEndpointClient, time.Now)
}

func probeEndpointWith(
	ctx context.Context,
	endpoint string,
	newClient endpointClientFactory,
	now func() time.Time,
) (endpointOutcome, error) {
	started := now()
	client, err := newClient(ctx, endpoint)
	if err != nil {
		return unavailableEndpoint(endpoint, err), nil
	}
	response, statusErr := client.Status(ctx, endpoint)
	latency := now().Sub(started).Milliseconds()
	closeErr := client.Close()
	if closeErr != nil {
		return endpointOutcome{}, errs.Wrap(errs.KindInternal, fmt.Errorf("close etcd diagnostic client: %w", closeErr))
	}
	if statusErr != nil {
		return unavailableEndpoint(endpoint, statusErr), nil
	}
	if response == nil {
		return unavailableEndpoint(endpoint, errors.New("etcd status response is nil")), nil
	}
	if response.Header == nil {
		return unavailableEndpoint(endpoint, errors.New("etcd status response header is nil")), nil
	}
	if len(response.Errors) > 0 {
		return unavailableEndpoint(endpoint, errors.New(strings.Join(response.Errors, "; "))), nil
	}
	dbSizeBytes := response.DbSize
	latencyMS := latency
	return endpointOutcome{row: localdiag.EtcdEndpoint{
		Endpoint:    endpoint,
		Healthy:     true,
		Version:     response.Version,
		MemberID:    strconv.FormatUint(response.Header.MemberId, 10),
		LeaderID:    strconv.FormatUint(response.Leader, 10),
		Revision:    strconv.FormatInt(response.Header.Revision, 10),
		DBSizeBytes: &dbSizeBytes,
		LatencyMS:   &latencyMS,
	}}, nil
}

func unavailableEndpoint(endpoint string, cause error) endpointOutcome {
	return endpointOutcome{
		row: localdiag.EtcdEndpoint{
			Endpoint: endpoint,
			Error:    endpointErrorMessage,
		},
		unavailable: cause,
	}
}

func newEndpointClient(ctx context.Context, endpoint string) (endpointStatusClient, error) {
	return clientv3.New(clientv3.Config{
		Context:     ctx,
		Endpoints:   []string{endpoint},
		DialTimeout: endpointProbeTimeout,
	})
}
