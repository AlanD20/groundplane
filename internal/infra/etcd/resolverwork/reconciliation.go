package resolverwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Request retains only duplicate-suppression authority. Neither a retained Task
// nor its execution diagnostics are needed to avoid retrying the same failure.
type Request struct {
	HostID          string                `json:"host_id"`
	ComponentID     string                `json:"component_id"`
	WorkID          string                `json:"work_id"`
	TaskID          string                `json:"task_id"`
	Fingerprint     string                `json:"fingerprint"`
	DesiredSHA256   string                `json:"desired_sha256"`
	DNSFingerprint  string                `json:"dns_fingerprint"`
	PrivateListener string                `json:"private_listener,omitempty"`
	ZoneListeners   []string              `json:"zone_listeners,omitempty"`
	References      []keyvalue.Condition  `json:"references"`
	Outcome         workexecution.Outcome `json:"outcome,omitempty"`
}

func RequestKey(hostID, componentID string) string {
	return "/v1/desired/resolver-requests/" + hostID + "/" + componentID
}

func (request Request) validate() error {
	if ids.Validate(ids.KindHost, request.HostID) != nil ||
		ids.Validate(ids.KindComponent, request.ComponentID) != nil ||
		ids.Validate(ids.KindWorkItem, request.WorkID) != nil ||
		ids.Validate(ids.KindTask, request.TaskID) != nil ||
		!recordcodec.ValidSHA256(request.Fingerprint) ||
		!recordcodec.ValidSHA256(request.DesiredSHA256) ||
		!recordcodec.ValidSHA256(request.DNSFingerprint) ||
		len(request.References) > 256 {
		return invalid("CoreDNS reconciliation request is corrupt")
	}
	if request.Outcome != "" && request.Outcome != workexecution.Succeeded && request.Outcome != workexecution.Failed &&
		request.Outcome != workexecution.Cancelled && request.Outcome != workexecution.Superseded {
		return invalid("CoreDNS request outcome is invalid")
	}
	if _, err := platformcomponents.ParseZoneListeners(request.ZoneListeners); err != nil {
		return err
	}
	for _, guard := range request.References {
		if guard.Key == "" || guard.ModRevision < 0 {
			return invalid("CoreDNS request source comparison is invalid")
		}
	}
	return nil
}

// fingerprint excludes new Task/attempt/artifact identities. It includes exact
// desired configuration, DNS bindings and the actual applied predecessor.
func fingerprint(input resolverexecution.Input, capture Capture, predecessor int64) (string, error) {
	desired, err := DesiredDigest(capture.Desired)
	if err != nil {
		return "", err
	}
	file, image := "", ""
	if input.Candidate != nil {
		file = hex.EncodeToString(input.Candidate.Apply.ArtifactDigest)
		image = input.Candidate.Runtime.Services[0].ImageReference
	}
	raw, err := json.Marshal(struct {
		Desired     string
		Projection  string
		Baseline    string
		File        string
		Image       string
		DNS         []platformcomponents.EnvironmentDNSCapture
		Listener    string
		Zones       []string
		Predecessor int64
	}{desired, capture.Projection.InputSHA256, capture.BaselineSHA256, file, image,
		capture.EnvironmentDNS, capture.PrivateListener, capture.ZoneListeners, predecessor})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func (repository *Repository) ReadRequest(
	ctx context.Context,
	hostID, componentID string,
	revision int64,
) (keyvalue.Versioned[Request], error) {
	if ids.Validate(ids.KindHost, hostID) != nil || ids.Validate(ids.KindComponent, componentID) != nil {
		return keyvalue.Versioned[Request]{}, invalid("CoreDNS request requires its host and Component")
	}
	read, err := repository.read(ctx, []string{RequestKey(hostID, componentID)}, revision)
	if err != nil {
		return keyvalue.Versioned[Request]{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return keyvalue.Versioned[Request]{ReadRevision: read.ReadRevision}, nil
	}
	request, err := decodeRequest(value.Value)
	if err != nil {
		return keyvalue.Versioned[Request]{}, err
	}
	if request.validate() != nil || request.HostID != hostID || request.ComponentID != componentID {
		return keyvalue.Versioned[Request]{}, invalid("CoreDNS reconciliation request belongs to another resource")
	}
	return keyvalue.Versioned[Request]{
		Record:       request,
		Revision:     value.ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

func decodeRequest(raw []byte) (Request, error) {
	if len(raw) == 0 || len(raw) > maximumCaptureBytes {
		return Request{}, invalid("CoreDNS request exceeds its retained source bound")
	}
	request, err := recordcodec.Decode[Request](raw, "resolver_request")
	if err != nil {
		return Request{}, err
	}
	return request, request.validate()
}

func prepareRequest(input resolverexecution.Input, capture Capture) (workqueue.Change, error) {
	digest, err := fingerprint(input, capture, capture.AppliedRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	desired, err := DesiredDigest(capture.Desired)
	if err != nil {
		return workqueue.Change{}, err
	}
	dns, err := platformcomponents.ResolverConfigurationFingerprint(
		capture.PrivateListener,
		capture.EnvironmentDNS,
		capture.ZoneListeners,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	request := Request{
		HostID:          input.HostID,
		ComponentID:     input.ComponentID,
		WorkID:          input.WorkID,
		TaskID:          input.TaskID,
		Fingerprint:     digest,
		DesiredSHA256:   desired,
		DNSFingerprint:  dns,
		PrivateListener: capture.PrivateListener,
		ZoneListeners: append(
			[]string(nil),
			capture.ZoneListeners...),
		References: append([]keyvalue.Condition(nil), capture.References...),
	}
	if err := request.validate(); err != nil {
		return workqueue.Change{}, err
	}
	value, err := recordcodec.Encode("resolver_request", request)
	if err != nil {
		return workqueue.Change{}, err
	}
	if len(value) > maximumCaptureBytes {
		clear(value)
		return workqueue.Change{}, invalid("CoreDNS request exceeds its retained source bound")
	}
	key := RequestKey(input.HostID, input.ComponentID)
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: capture.RequestRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}}, nil
}

// NeedsAutomaticWork waits for the latest accepted action, then compares actual
// applied resource state. Failed input is not automatically retried, even after
// its Task is collected. A different target or predecessor is a new request.
func (repository *Repository) NeedsAutomaticWork(
	ctx context.Context,
	input resolverexecution.Input,
	capture Capture,
) (bool, error) {
	request, err := repository.ReadRequest(ctx, input.HostID, input.ComponentID, capture.ReadRevision)
	if err != nil {
		return false, err
	}
	if request.Revision != capture.RequestRevision {
		return false, invalid("CoreDNS request changed during automatic preparation")
	}
	if request.Revision > 0 {
		read, err := repository.read(
			ctx,
			[]string{IntentKey(input.HostID, input.ComponentID), workqueue.ItemKey(request.Record.WorkID)},
			capture.ReadRevision,
		)
		if err != nil {
			return false, err
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] == nil || string(read.Values[0].Value) != request.Record.WorkID {
			return false, invalid("CoreDNS latest request and work identity disagree")
		}
		if read.Values[1] != nil {
			item, err := workqueue.DecodeItem(read.Values[1].Value)
			if err != nil {
				return false, err
			}
			if item.ID != request.Record.WorkID || item.Operation != workexecution.Resolver {
				return false, invalid("CoreDNS request identifies another operation")
			}
			if item.Phase != workexecution.Settled {
				return false, nil
			}
		}
	}
	digest, err := fingerprint(input, capture, capture.AppliedRevision)
	if err != nil {
		return false, err
	}
	if request.Revision > 0 && request.Record.Fingerprint == digest {
		return false, nil
	}
	applied, found, err := repository.ReadApplied(ctx, input.HostID, input.ComponentID, capture.ReadRevision)
	if err != nil || !found {
		return !found, err
	}
	prior := applied.Record
	// Compare semantic target without the predecessor comparison. The applied
	// record is retained independently of the Task that produced it.
	actual := capture
	actual.Desired, actual.Projection.InputSHA256 = prior.Desired, prior.ProjectionSHA256
	actual.BaselineSHA256, actual.EnvironmentDNS = prior.BaselineSHA256, prior.EnvironmentDNS
	actual.PrivateListener, actual.ZoneListeners = prior.PrivateListener, prior.ZoneListeners
	previous := input
	previous.Candidate = prior.Configuration
	left, err := fingerprint(input, capture, 0)
	if err != nil {
		return false, err
	}
	right, err := fingerprint(previous, actual, 0)
	return left != right, err
}
