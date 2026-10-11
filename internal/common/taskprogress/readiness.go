// Package taskprogress owns the output-free observations carried by Task events.
package taskprogress

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Readiness struct {
	Expected            uint32 `json:"expected" minimum:"1"`
	Observed            uint32 `json:"observed"`
	Running             uint32 `json:"running"`
	Ready               uint32 `json:"ready"`
	Starting            uint32 `json:"starting"`
	Unhealthy           uint32 `json:"unhealthy"`
	Exited              uint32 `json:"exited"`
	ElapsedMilliseconds uint64 `json:"elapsed_milliseconds"`
	TimeoutMilliseconds uint64 `json:"timeout_milliseconds"`
	Diagnostic          string `json:"diagnostic" enum:"none,pgrep_missing,curl_missing,nc_missing,command_missing,command_not_executable"`
}

// Payload extends the existing event journal, not raw output streaming. An empty
// payload is a state transition; observations contain no raw process output.
type Payload struct {
	ControllerOperation *ControllerOperation `json:"controller_operation,omitempty"`
	HealthWait          *Readiness           `json:"health_wait,omitempty"`
	Failure             *Failure             `json:"failure,omitempty"`
}

func FromAgent(value *agentpb.HealthWaitProgress) (*Readiness, error) {
	if value == nil {
		return nil, nil
	}
	diagnostic := "none"
	switch value.GetDiagnostic() {
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED:
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_PGREP_MISSING:
		diagnostic = "pgrep_missing"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_CURL_MISSING:
		diagnostic = "curl_missing"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_NC_MISSING:
		diagnostic = "nc_missing"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_MISSING:
		diagnostic = "command_missing"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_NOT_EXECUTABLE:
		diagnostic = "command_not_executable"
	default:
		return nil, errs.New(errs.KindValidationFailed, "health wait diagnostic is invalid")
	}
	p := &Readiness{Expected: value.Expected, Observed: value.Observed, Running: value.Running,
		Ready: value.Ready, Starting: value.Starting, Unhealthy: value.Unhealthy, Exited: value.Exited,
		ElapsedMilliseconds: value.ElapsedMilliseconds, TimeoutMilliseconds: value.TimeoutMilliseconds,
		Diagnostic: diagnostic}
	return p, p.Validate()
}

func (p *Readiness) Validate() error {
	if p == nil {
		return nil
	}
	if p.Expected == 0 || p.Expected > 65536 || p.Observed > 65536 || p.Running > p.Observed ||
		p.Ready > p.Running || p.Starting > p.Observed || p.Unhealthy > p.Observed || p.Exited > p.Observed ||
		uint64(p.Running)+uint64(p.Exited) > uint64(p.Observed) ||
		uint64(p.Starting)+uint64(p.Unhealthy) > uint64(p.Observed) ||
		p.TimeoutMilliseconds == 0 || p.TimeoutMilliseconds > 86400000 || p.ElapsedMilliseconds > 86400000 {
		return errs.New(errs.KindValidationFailed, "health wait observation counts or timing are invalid")
	}
	switch p.Diagnostic {
	case "none", "pgrep_missing", "curl_missing", "nc_missing", "command_missing", "command_not_executable":
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "health wait observation diagnostic is invalid")
	}
}

func DecodePayload(value json.RawMessage) (Payload, error) {
	var p Payload
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return Payload{}, errs.Wrap(errs.KindInternal, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Payload{}, errs.New(errs.KindInternal, "Task observation payload has trailing content")
	}
	if err := p.HealthWait.Validate(); err != nil {
		return Payload{}, err
	}
	if err := p.Failure.Validate(); err != nil {
		return Payload{}, err
	}
	if err := p.ControllerOperation.Validate(); err != nil {
		return Payload{}, err
	}
	if p.HealthWait != nil && (p.Failure != nil || p.ControllerOperation != nil) {
		return Payload{}, errs.New(errs.KindInternal, "Task event mixes readiness and failure")
	}
	return p, nil
}
