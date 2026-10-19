package blueprintattachinputs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Consumer decodes the immutable applied-runtime transition. It never renders
// current desired state or consults a mutable Service name.
func (generation Generation) Consumer() (*agentpb.AttachConsumerTransition, error) {
	if len(generation.ConsumerTransition) == 0 {
		if generation.ConsumerRuntime != nil || generation.ConsumerRuntimeRevision != 0 {
			return nil, errs.New(errs.KindValidationFailed, "Attach runtime capture has no consumer transition")
		}
		return nil, nil
	}
	if generation.ConsumerRuntime == nil || generation.ConsumerRuntimeRevision <= 0 ||
		serviceruntimerecord.Validate(*generation.ConsumerRuntime) != nil ||
		generation.ConsumerRuntime.EnvironmentID != generation.EnvironmentID ||
		generation.ConsumerRuntime.Runtime.ServiceID != generation.ConsumerServiceID {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Attach consumer transition requires its captured applied runtime",
		)
	}
	if len(generation.ConsumerTransition) > executionplan.MaximumPlanBytes {
		return nil, errs.New(errs.KindValidationFailed, "Attach consumer capture is oversized")
	}
	consumer := &agentpb.AttachConsumerTransition{}
	if proto.Unmarshal(generation.ConsumerTransition, consumer) != nil ||
		executionplan.ValidateAttachConsumerTransition(context.Background(), consumer) != nil ||
		consumer.Action != agentpb.AttachNetworkAction_ATTACH_NETWORK_ACTION_CONNECT ||
		consumer.EnvironmentId != generation.EnvironmentID ||
		consumer.BackingEnvironmentId != generation.BackingEnvironmentID ||
		consumer.ServiceId != generation.ConsumerServiceID ||
		consumer.NetworkId != generation.BackingNetworkID {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Attach consumer capture does not describe its owned backing-network edge",
		)
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(consumer)
	if err != nil {
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	defer clear(canonical)
	if !bytes.Equal(canonical, generation.ConsumerTransition) {
		return nil, errs.New(errs.KindValidationFailed, "Attach consumer capture is not canonical")
	}
	baseline := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(generation.ConsumerRuntime.Runtime.CurrentArtifact, baseline) != nil ||
		!proto.Equal(baseline, consumer.Baseline) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Attach consumer baseline differs from its applied runtime capture",
		)
	}
	return consumer, nil
}

// ProvisionAuthority derives safe execution metadata without decrypting facts.
func (generation Generation) ProvisionAuthority() (attachexecution.ProvisionAuthority, error) {
	reference, err := WorkInput(generation)
	if err != nil {
		return attachexecution.ProvisionAuthority{}, err
	}
	authority := attachexecution.ProvisionAuthority{
		Source:           reference,
		AttachID:         generation.AttachID,
		BackingServiceID: generation.BackingServiceID,
		Custom:           generation.Provision == nil && generation.Binding == nil && generation.Hook.Attach != nil,
	}
	if authority.Custom {
		authority.Commands = 1
	}
	if generation.Provision != nil && generation.Provision.RequiresProvisioning {
		authority.Commands = uint32(1 + len(generation.Provision.Grants))
	}
	consumer, err := generation.Consumer()
	if err != nil {
		return attachexecution.ProvisionAuthority{}, err
	}
	if consumer != nil {
		digest := sha256.Sum256(generation.ConsumerTransition)
		authority.ConsumerSHA256 = hex.EncodeToString(digest[:])
		authority.ConsumerNetworkID, authority.ConsumerServiceID = consumer.NetworkId, consumer.ServiceId
		authority.ConsumerCandidateSHA256 = hex.EncodeToString(consumer.Candidate.YamlSha256)
		authority.Commands++
	}
	return authority, nil
}
