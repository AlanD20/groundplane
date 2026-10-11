// Package edgeconnectioncheck implements bounded HTTP diagnostics. CheckPrivate
// must execute inside the captured connector's network namespace; CheckPublic
// uses Controller-host outbound networking and labels that location at its
// public boundary. Neither reads provider credentials nor response bodies.
package edgeconnectioncheck

import (
	"context"
	"net/http"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func CheckPrivate(ctx context.Context, request contract.Request) (contract.PrivateResult, error) {
	if ctx == nil || request.Validate() != nil {
		return contract.PrivateResult{}, errs.New(errs.KindValidationFailed, "Tunnel diagnostic request is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, contract.PrivateTimeout)
	defer cancel()
	result := contract.PrivateResult{
		RequestID:     request.RequestID,
		RuntimeSHA256: request.RuntimeSHA256,
		StartedAt:     time.Now().UTC(),
	}
	transport := newTransport()
	defer transport.CloseIdleConnections()
	client := boundedClient(transport)
	// Independent checks: a disconnected connector does not imply that its
	// origin is down, and a reachable origin does not imply a connected Tunnel.
	result.Stages = []contract.Stage{
		checkHTTP(ctx, client, contract.Tunnel, request.ReadyURL, ""),
		checkHTTP(
			ctx,
			client,
			contract.Origin,
			"http://"+request.OriginAddress+request.Target.Path,
			request.Target.Hostname,
		),
	}
	result.FinishedAt = time.Now().UTC()
	return result, result.Validate(request)
}

func checkHTTP(
	ctx context.Context,
	client *http.Client,
	name contract.StageName,
	target, hostname string,
) contract.Stage {
	started := time.Now()
	ctx, cancel := context.WithTimeout(ctx, contract.StageTimeout)
	defer cancel()
	stage := contract.Stage{Name: name, Outcome: contract.Failed}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		stage.Failure = contract.ProtocolFailure
	} else {
		if hostname != "" {
			request.Host = hostname
		}
		request.Header.Set("User-Agent", "Groundplane-Route-Connection-Check")
		response, err := client.Do(request)
		if err != nil {
			stage.Failure = classifyFailure(err)
		} else {
			defer response.Body.Close()
			stage.HTTPStatus = response.StatusCode
			switch {
			case name == contract.Tunnel && response.StatusCode == http.StatusOK:
				stage.Outcome = contract.Passed
			case name == contract.Tunnel:
				stage.Failure = contract.NotReady
			case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
				stage.Outcome = contract.Protected
			case response.StatusCode >= 200 && response.StatusCode < 400:
				stage.Outcome = contract.Passed
			default:
				stage.Failure = contract.HTTPFailure
			}
		}
	}
	stage.Elapsed = time.Since(started)
	return stage
}
