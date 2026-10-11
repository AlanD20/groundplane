// Package runnerconnectioncheck owns the fixed read-only Runner diagnostic exchange.
package runnerconnectioncheck

import (
	"encoding/hex"
	"net/netip"
	"path/filepath"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
)

const (
	HelperArgument     = "--groundplane-runner-connection-check"
	MaximumInputBytes  = 2048
	MaximumOutputBytes = 8192
	Timeout            = 30 * time.Second
	StageTimeout       = 5 * time.Second
	DockerSocket       = "/var/run/groundplane-docker/docker.sock"
)

type Request struct {
	RequestID          string `json:"request_id"`
	RunnerID           string `json:"runner_id"`
	RuntimeEpoch       uint64 `json:"runtime_epoch"`
	PlanSHA256         string `json:"plan_sha256"`
	ControllerEndpoint string `json:"controller_endpoint"`
	RunnerHome         string `json:"runner_home"`
}

type Stage struct {
	Name                string `json:"name"`
	Outcome             string `json:"outcome"`
	Failure             string `json:"failure,omitempty"`
	HTTPStatus          int    `json:"http_status,omitempty"`
	ElapsedMilliseconds int64  `json:"elapsed_ms"`
}

type Result struct {
	RequestID    string    `json:"request_id"`
	RunnerID     string    `json:"runner_id"`
	RuntimeEpoch uint64    `json:"runtime_epoch"`
	PlanSHA256   string    `json:"plan_sha256"`
	ObservedAt   time.Time `json:"observed_at"`
	Stages       []Stage   `json:"stages"`
}

func (request Request) Validate() error {
	_, requestErr := ulid.ParseStrict(request.RequestID)
	digest, digestErr := hex.DecodeString(strings.TrimPrefix(request.PlanSHA256, "sha256:"))
	endpoint, endpointErr := netip.ParseAddrPort(request.ControllerEndpoint)
	home := request.RunnerHome
	slot := strings.TrimSuffix(strings.TrimPrefix(home, "/var/lib/groundplane-runners/slot-"), "/runner")
	if requestErr != nil || strings.ToUpper(request.RequestID) != request.RequestID ||
		ids.Validate(ids.KindRunner, request.RunnerID) != nil || request.RuntimeEpoch == 0 ||
		!strings.HasPrefix(
			request.PlanSHA256,
			"sha256:",
		) || digestErr != nil || len(digest) != 32 || strings.ToLower(request.PlanSHA256) != request.PlanSHA256 ||
		endpointErr != nil || !endpoint.Addr().Is4() || !endpoint.Addr().IsPrivate() || endpoint.Port() == 0 ||
		filepath.Clean(
			home,
		) != home || len(slot) != 2 || slot[0] < '0' || slot[0] > '9' || slot[1] < '0' || slot[1] > '9' ||
		home != "/var/lib/groundplane-runners/slot-"+slot+"/runner" {
		return errs.New(errs.KindValidationFailed, "Runner connection-check input is invalid")
	}
	return nil
}

func (result Result) Validate(request Request) error {
	if request.Validate() != nil || result.RequestID != request.RequestID || result.RunnerID != request.RunnerID ||
		result.RuntimeEpoch != request.RuntimeEpoch || result.PlanSHA256 != request.PlanSHA256 || result.ObservedAt.IsZero() || len(result.Stages) != 4 {
		return errs.New(errs.KindValidationFailed, "Runner connection-check result is invalid")
	}
	for index, name := range []string{"github", "rootless_docker", "gp_api", "registry"} {
		stage := result.Stages[index]
		if stage.Name != name || stage.ElapsedMilliseconds < 0 || stage.ElapsedMilliseconds > Timeout.Milliseconds() ||
			stage.HTTPStatus != 0 && (stage.HTTPStatus < 100 || stage.HTTPStatus > 599) ||
			stage.Name == "rootless_docker" && stage.HTTPStatus != 0 {
			return errs.New(errs.KindValidationFailed, "Runner connection-check stage is invalid")
		}
		switch stage.Outcome {
		case "passed":
			if stage.Failure != "" {
				return errs.New(errs.KindValidationFailed, "Successful Runner check has a failure")
			}
			if name == "github" && (stage.HTTPStatus < 200 || stage.HTTPStatus >= 500) ||
				(name == "gp_api" || name == "registry") && stage.HTTPStatus != 200 {
				return errs.New(errs.KindValidationFailed, "Successful Runner check has no matching HTTP response")
			}
		case "failed":
			switch stage.Failure {
			case "connection",
				"timeout",
				"canceled",
				"tls",
				"authentication",
				"permission",
				"http_response",
				"protocol",
				"not_rootless",
				"credentials_unavailable":
			default:
				return errs.New(errs.KindValidationFailed, "Runner check failure is invalid")
			}
		default:
			return errs.New(errs.KindValidationFailed, "Runner check outcome is invalid")
		}
	}
	return nil
}
