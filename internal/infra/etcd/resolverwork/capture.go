// Package resolverwork owns captured platform DNS execution and its independent
// applied configuration. It contains no old Task plans or event-journal authority.
package resolverwork

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumCaptureBytes = 192 * 1024

// Capture contains settlement and publication comparisons only. The executable
// Corefiles and artifacts are stored once in InputKey, not copied into this record.
type Capture struct {
	PlanID             string                                        `json:"plan_id"`
	TaskID             string                                        `json:"task_id"`
	WorkID             string                                        `json:"work_id"`
	HostID             string                                        `json:"host_id"`
	TargetName         string                                        `json:"target_name"`
	Actor              workexecution.Actor                           `json:"actor"`
	Action             workexecution.Action                          `json:"action"`
	Desired            componentrecord.DesiredRecord                 `json:"desired"`
	ReadRevision       int64                                         `json:"read_revision"`
	ComponentRevision  int64                                         `json:"component_revision"`
	AppliedRevision    int64                                         `json:"applied_revision"`
	IntentRevision     int64                                         `json:"intent_revision"`
	RequestRevision    int64                                         `json:"request_revision"`
	BaselineRevision   int64                                         `json:"baseline_revision"`
	BaselineSHA256     string                                        `json:"baseline_sha256"`
	ProjectionRevision int64                                         `json:"projection_revision"`
	Projection         hostresolution.HostResolutionProjectionRecord `json:"projection"`
	References         []keyvalue.Condition                          `json:"references"`
	Hosts              []platformcomponents.PlatformDNSHost          `json:"hosts"`
	EnvironmentDNS     []platformcomponents.EnvironmentDNSCapture    `json:"environment_dns"`
	PrivateListener    string                                        `json:"private_listener,omitempty"`
	ZoneListeners      []string                                      `json:"zone_listeners,omitempty"`
}

func (capture Capture) Validate(input resolverexecution.Input) error {
	if input.Validate() != nil || capture.TaskID != input.TaskID || capture.WorkID != input.WorkID || capture.HostID != input.HostID ||
		capture.Desired.ID != input.ComponentID || capture.Desired.Owner != core.ComponentOwnerPlatform || capture.Desired.OwnerID != "" ||
		capture.Desired.Kind != core.ComponentKindCoreDNS || capture.Desired.Enabled != input.Enabled ||
		componentrecord.ValidateRecord(componentrecord.Record{Desired: capture.Desired}) != nil ||
		capture.ReadRevision <= 0 || capture.ComponentRevision <= 0 || capture.ComponentRevision > capture.ReadRevision ||
		capture.AppliedRevision < 0 || capture.AppliedRevision > capture.ReadRevision ||
		capture.IntentRevision < 0 || capture.IntentRevision > capture.ReadRevision ||
		capture.RequestRevision < 0 || capture.RequestRevision > capture.ReadRevision ||
		capture.BaselineRevision <= 0 || capture.BaselineRevision > capture.ReadRevision || !recordcodec.ValidSHA256(capture.BaselineSHA256) ||
		capture.BaselineSHA256 != input.BaselineSHA256 ||
		capture.ProjectionRevision < 0 || capture.ProjectionRevision > capture.ReadRevision ||
		capture.TargetName == "" || len(capture.TargetName) > 256 || !utf8.ValidString(capture.TargetName) || len(capture.References) > 256 {
		return invalid("DNS resolver resource capture differs from its executable input")
	}
	if capture.Actor != workexecution.OperatorActor && capture.Actor != workexecution.SystemActor ||
		capture.Action != workexecution.ActionUpdate && capture.Action != workexecution.ActionStart && capture.Action != workexecution.ActionStop {
		return invalid("DNS resolver capture has no supported initiating action")
	}
	if _, err := hostresolution.EncodeHostResolutionProjectionRecord(capture.Projection); err != nil {
		return err
	}
	if _, _, err := resolverexecution.Capture(capture.PlanID, input); err != nil {
		return err
	}
	if platformcomponents.ValidateEnvironmentDNS(capture.EnvironmentDNS, capture.Hosts) != nil {
		return invalid("DNS resolver capture has invalid Environment DNS bindings")
	}
	for _, guard := range capture.References {
		if guard.Key == "" || guard.ModRevision < 0 || guard.ModRevision > capture.ReadRevision {
			return invalid("DNS resolver reference comparison is outside the captured snapshot")
		}
	}
	return nil
}

func DesiredDigest(desired componentrecord.DesiredRecord) (string, error) {
	if err := componentrecord.ValidateRecord(componentrecord.Record{Desired: desired}); err != nil {
		return "", err
	}
	raw, err := json.Marshal(desired)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func encodeCapture(capture Capture, input resolverexecution.Input) ([]byte, error) {
	if err := capture.Validate(input); err != nil {
		return nil, err
	}
	raw, err := recordcodec.Encode("resolver_work_capture", capture)
	if err != nil {
		return nil, err
	}
	if len(raw) > maximumCaptureBytes {
		return nil, invalid("DNS resolver resource capture exceeds its bound")
	}
	return raw, nil
}

func decodeCapture(raw []byte, input resolverexecution.Input) (Capture, error) {
	if len(raw) == 0 || len(raw) > maximumCaptureBytes {
		return Capture{}, invalid("DNS resolver resource capture is missing or oversized")
	}
	capture, err := recordcodec.Decode[Capture](raw, "resolver_work_capture")
	if err != nil {
		return Capture{}, err
	}
	return capture, capture.Validate(input)
}

func invalid(message string) error { return errs.New(errs.KindStateConflict, message) }

func matchesWork(input resolverexecution.Input, reference workexecution.Input, task workexecution.Task, item workexecution.Item) error {
	if task.Validate() != nil || item.Validate() != nil || task.ID != input.TaskID || item.ID != input.WorkID || item.TaskID != task.ID ||
		item.Operation != workexecution.Resolver || task.TargetID != input.ComponentID || item.TargetID != input.ComponentID ||
		item.Placement != (workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID}) ||
		item.Input == nil || *item.Input != reference || task.Intent != reference || task.Scope != (workexecution.Scope{}) ||
		!task.CreatedAt.Equal(input.CreatedAt) || !item.CreatedAt.Equal(input.CreatedAt) ||
		task.ExpectedItems != 1 || !task.PublicationComplete || len(item.Dependencies) != 0 ||
		len(item.Claims) != 1 || item.Claims[0] != (workexecution.Claim{HostID: input.HostID, ScopeID: input.ComponentID}) {
		return invalid("DNS resolver work differs from its captured action and host")
	}
	return nil
}
