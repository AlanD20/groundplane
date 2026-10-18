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
	HostID      string `json:"host_id"`
	ComponentID string `json:"component_id"`
	WorkID      string `json:"work_id"`
	Fingerprint string `json:"fingerprint"`
}

func RequestKey(hostID, componentID string) string {
	return "/v1/desired/resolver-requests/" + hostID + "/" + componentID
}

func (request Request) validate() error {
	if ids.Validate(ids.KindHost, request.HostID) != nil || ids.Validate(ids.KindComponent, request.ComponentID) != nil ||
		ids.Validate(ids.KindWorkItem, request.WorkID) != nil || !recordcodec.ValidSHA256(request.Fingerprint) {
		return invalid("CoreDNS reconciliation request is corrupt")
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

func (repository *Repository) ReadRequest(ctx context.Context, hostID, componentID string, revision int64) (keyvalue.Versioned[Request], error) {
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
	request, err := recordcodec.Decode[Request](value.Value, "resolver_request")
	if err != nil {
		return keyvalue.Versioned[Request]{}, err
	}
	if request.validate() != nil || request.HostID != hostID || request.ComponentID != componentID {
		return keyvalue.Versioned[Request]{}, invalid("CoreDNS reconciliation request belongs to another resource")
	}
	return keyvalue.Versioned[Request]{Record: request, Revision: value.ModRevision, ReadRevision: read.ReadRevision}, nil
}

func prepareRequest(input resolverexecution.Input, capture Capture) (workqueue.Change, error) {
	digest, err := fingerprint(input, capture, capture.AppliedRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	request := Request{HostID: input.HostID, ComponentID: input.ComponentID, WorkID: input.WorkID, Fingerprint: digest}
	if err := request.validate(); err != nil {
		return workqueue.Change{}, err
	}
	value, err := recordcodec.Encode("resolver_request", request)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := RequestKey(input.HostID, input.ComponentID)
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: capture.RequestRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}}, nil
}

// NeedsAutomaticWork waits for the latest accepted action, then compares actual
// applied resource state. Failed input is not automatically retried, even after
// its Task is collected. A different target or predecessor is a new request.
func (repository *Repository) NeedsAutomaticWork(ctx context.Context, input resolverexecution.Input, capture Capture) (bool, error) {
	request, err := repository.ReadRequest(ctx, input.HostID, input.ComponentID, capture.ReadRevision)
	if err != nil {
		return false, err
	}
	if request.Revision != capture.RequestRevision {
		return false, invalid("CoreDNS request changed during automatic preparation")
	}
	if request.Revision > 0 {
		read, err := repository.read(ctx, []string{IntentKey(input.HostID, input.ComponentID), workqueue.ItemKey(request.Record.WorkID)}, capture.ReadRevision)
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
