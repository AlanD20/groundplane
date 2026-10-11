// Package runnerconnectioncheck runs read-only checks inside the existing Runner.
package runnerconnectioncheck

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"time"

	contract "github.com/AlanD20/groundplane/internal/common/runnerconnectioncheck"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func Check(ctx context.Context, request contract.Request) (contract.Result, error) {
	if ctx == nil || request.Validate() != nil {
		return contract.Result{}, errs.New(errs.KindValidationFailed, "Runner diagnostic context or input is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, contract.Timeout)
	defer cancel()
	result := contract.Result{RequestID: request.RequestID, RunnerID: request.RunnerID,
		RuntimeEpoch: request.RuntimeEpoch, PlanSHA256: request.PlanSHA256, Stages: make([]contract.Stage, 0, 4)}
	for _, name := range []string{"github", "rootless_docker", "gp_api", "registry"} {
		started := time.Now()
		stageCtx, stageCancel := context.WithTimeout(ctx, contract.StageTimeout)
		stage := contract.Stage{Name: name, Outcome: "failed"}
		switch name {
		case "github":
			stage.HTTPStatus, stage.Failure = checkGitHub(stageCtx)
		case "rootless_docker":
			stage.Failure = checkDocker(stageCtx)
		case "gp_api":
			stage.HTTPStatus, stage.Failure = checkAPI(stageCtx, request.ControllerEndpoint)
		case "registry":
			stage.HTTPStatus, stage.Failure = checkRegistry(stageCtx, request.RunnerHome)
		}
		stageCancel()
		stage.ElapsedMilliseconds = time.Since(started).Milliseconds()
		if stage.Failure == "" {
			stage.Outcome = "passed"
		}
		result.Stages = append(result.Stages, stage)
	}
	result.ObservedAt = time.Now().UTC()
	return result, result.Validate(request)
}

func checkGitHub(ctx context.Context) (int, string) {
	client, err := publicTLSClient()
	if err != nil {
		return 0, "tls"
	}
	defer client.CloseIdleConnections()
	status, _, failure := get(ctx, client, "https://github.com/", "", 0)
	if failure != "" {
		return status, failure
	}
	// This stage proves DNS/TLS/HTTP reachability, not registration or account
	// permission. A protected or rate-limited HTTP response still proves a path.
	if status < 200 || status >= 500 {
		return status, "http_response"
	}
	return status, ""
}

func checkDocker(ctx context.Context) string {
	transport := newTransport()
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", contract.DockerSocket)
	}
	client := boundedClient(transport)
	defer client.CloseIdleConnections()
	status, body, failure := get(ctx, client, "http://docker/info", "", 64<<10)
	if failure != "" {
		return failure
	}
	if status != http.StatusOK {
		return statusFailure(status)
	}
	var info struct {
		SecurityOptions []string `json:"SecurityOptions"`
	}
	if json.Unmarshal(body, &info) != nil {
		return "protocol"
	}
	for _, option := range info.SecurityOptions {
		if option == "name=rootless" || strings.HasPrefix(option, "name=rootless,") {
			return ""
		}
	}
	return "not_rootless"
}

func checkAPI(ctx context.Context, endpoint string) (int, string) {
	client := boundedClient(newTransport())
	defer client.CloseIdleConnections()
	status, body, failure := get(ctx, client, "http://"+endpoint+"/api/v1/agents?limit=1", "", 64<<10)
	if failure != "" {
		return status, failure
	}
	if status != http.StatusOK {
		return status, statusFailure(status)
	}
	var page struct {
		Items *json.RawMessage `json:"items"`
	}
	if json.Unmarshal(body, &page) != nil || page.Items == nil || len(*page.Items) == 0 || (*page.Items)[0] != '[' {
		return status, "protocol"
	}
	return status, ""
}
