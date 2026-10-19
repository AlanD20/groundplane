package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CaptureConsumer adds the applied consumer's network transition before the
// immutable input is published. Its comparisons belong in that publication,
// including absence of a runtime: a concurrent Deploy must not escape capture.
// Existing encrypted fields remain owned by the caller; only the newly read
// runtime and transition are allocated here.
func (repository *InputRepository) CaptureConsumer(ctx context.Context, generation blueprintattachinputs.Generation,
	hostID, artifactID string, revision int64,
) (blueprintattachinputs.Generation, []keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return blueprintattachinputs.Generation{}, nil, err
	}
	if repository == nil || repository.store == nil || revision <= 0 ||
		blueprintattachinputs.ValidateDraft(generation) != nil ||
		ids.Validate(ids.KindConfig, artifactID) != nil || ids.Validate(ids.KindHost, hostID) != nil ||
		len(
			generation.ConsumerTransition,
		) != 0 || generation.ConsumerRuntime != nil || generation.ConsumerRuntimeRevision != 0 {
		return blueprintattachinputs.Generation{}, nil, errs.New(
			errs.KindValidationFailed,
			"Attach consumer capture requires an unbound input and fixed host snapshot",
		)
	}
	keys := []string{
		services.ServiceRuntimeKey(generation.ConsumerServiceID),
		serviceruntimerecord.Key(generation.ConsumerServiceID),
		releases.ReleaseProjectionKey(generation.ConsumerServiceID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return blueprintattachinputs.Generation{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil {
		return blueprintattachinputs.Generation{}, nil, checkpointConflict(
			"Attach consumer runtime state is unavailable",
		)
	}
	guards := make([]keyvalue.Condition, len(keys))
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision) {
			return blueprintattachinputs.Generation{}, nil, recordcodec.CorruptRecord()
		}
		guards[index] = keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)}
	}
	state, err := services.DecodeServiceRuntimeRecord(read.Values[0].Value)
	if err != nil {
		return blueprintattachinputs.Generation{}, nil, err
	}
	if state.EnvironmentID != generation.EnvironmentID || state.ServiceID != generation.ConsumerServiceID {
		return blueprintattachinputs.Generation{}, nil, recordcodec.CorruptRecord()
	}
	if state.Runtime.RuntimeIntent != "running" && state.Runtime.RuntimeIntent != "stopped" {
		return blueprintattachinputs.Generation{}, nil, checkpointConflict("Attach consumer has no runnable Service")
	}
	value := read.Values[1]
	if value == nil {
		unapplied, err := unappliedConsumer(generation, read.Values[0], read.Values[2])
		if err != nil {
			return blueprintattachinputs.Generation{}, nil, err
		}
		if !unapplied {
			return blueprintattachinputs.Generation{}, nil, checkpointConflict(
				"Attach requires the Service's acknowledged runtime or a Service with no applied or pending deployment",
			)
		}
		return generation, guards, nil
	}
	runtime, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](
		value.Value,
		"service-acknowledged-runtime",
	)
	if err != nil {
		return blueprintattachinputs.Generation{}, nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			clear(runtime.Runtime.CurrentArtifact)
			clear(runtime.Runtime.RetainedPriorArtifact)
		}
	}()
	if serviceruntimerecord.Validate(runtime) != nil || runtime.EnvironmentID != generation.EnvironmentID ||
		runtime.Runtime.ServiceID != generation.ConsumerServiceID || runtime.Source.Executor.HostID != hostID {
		return blueprintattachinputs.Generation{}, nil, checkpointConflict(
			"Attach consumer runtime does not belong to the selected Service and host",
		)
	}
	baseline := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(runtime.Runtime.CurrentArtifact, baseline) != nil {
		return blueprintattachinputs.Generation{}, nil, recordcodec.CorruptRecord()
	}
	defer clear(baseline.CanonicalYaml)
	transition, err := executionplan.PrepareAttachConsumerConnection(ctx, baseline, generation.EnvironmentID,
		generation.BackingEnvironmentID, generation.ConsumerServiceID, generation.BackingNetworkID, artifactID)
	if err != nil {
		return blueprintattachinputs.Generation{}, nil, err
	}
	defer clear(transition.Baseline.CanonicalYaml)
	defer clear(transition.Candidate.CanonicalYaml)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(transition)
	if err != nil {
		return blueprintattachinputs.Generation{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	generation.ConsumerTransition = encoded
	generation.ConsumerRuntime, generation.ConsumerRuntimeRevision = &runtime, value.ModRevision
	if err := blueprintattachinputs.ValidateDraft(generation); err != nil {
		clear(encoded)
		return blueprintattachinputs.Generation{}, nil, err
	}
	transferred = true
	return generation, guards, nil
}
