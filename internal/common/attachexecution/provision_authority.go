package attachexecution

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ProvisionAuthority is safe to retain in the Agent journal. Source identifies
// the Controller's immutable encrypted capture; no password, hook input, or
// returned fact is duplicated into command progress or diagnostic history.
type ProvisionAuthority struct {
	Source                  workexecution.Input `json:"source"`
	AttachID                string              `json:"attach_id"`
	BackingServiceID        string              `json:"backing_service_id"`
	Commands                uint32              `json:"commands"`
	Custom                  bool                `json:"custom"`
	ConsumerSHA256          string              `json:"consumer_sha256,omitempty"`
	ConsumerNetworkID       string              `json:"consumer_network_id,omitempty"`
	ConsumerServiceID       string              `json:"consumer_service_id,omitempty"`
	ConsumerCandidateSHA256 string              `json:"consumer_candidate_sha256,omitempty"`
}

func CaptureProvisionAuthority(input *agentpb.AttachProvisionInput) (ProvisionAuthority, error) {
	if err := ValidateProvision(input); err != nil {
		return ProvisionAuthority{}, err
	}
	authority := ProvisionAuthority{
		Source:   workexecution.Input{ID: input.CapturedSource.InputId, SHA256: hex.EncodeToString(input.CapturedSource.Sha256)},
		AttachID: input.AttachId, BackingServiceID: input.BackingServiceId,
		Custom: input.GetCustomAttach() != nil,
	}
	if input.Provision != nil {
		authority.Commands = 1
	}
	if credential := input.GetCredential(); credential != nil {
		authority.Commands += uint32(len(credential.Grants))
	}
	if consumer := input.Consumer; consumer != nil {
		raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(consumer)
		if err != nil {
			return ProvisionAuthority{}, invalid("Attach consumer capture cannot be encoded")
		}
		digest := sha256.Sum256(raw)
		clear(raw)
		authority.ConsumerSHA256 = hex.EncodeToString(digest[:])
		authority.ConsumerNetworkID, authority.ConsumerServiceID = consumer.NetworkId, consumer.ServiceId
		authority.ConsumerCandidateSHA256 = hex.EncodeToString(consumer.Candidate.YamlSha256)
		authority.Commands++
	}
	return authority, nil
}

func (authority ProvisionAuthority) Validate(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || attempt.Executor.Executor != workexecution.Agent ||
		authority.Source.Validate() != nil || authority.Source != attempt.Input || ids.Validate(ids.KindAttach, authority.AttachID) != nil ||
		ids.Validate(ids.KindService, authority.BackingServiceID) != nil || authority.Commands == 0 ||
		authority.Commands > MaximumGrants+2 {
		return invalid("Attach provisioning journal authority is invalid")
	}
	if authority.ConsumerSHA256 != "" {
		if !validConsumerDigest(authority.ConsumerSHA256) || !validConsumerDigest(authority.ConsumerCandidateSHA256) ||
			ids.Validate(ids.KindNetwork, authority.ConsumerNetworkID) != nil || ids.Validate(ids.KindService, authority.ConsumerServiceID) != nil ||
			authority.Custom && authority.Commands != 2 {
			return invalid("Attach connection journal authority is invalid")
		}
	} else if authority.ConsumerNetworkID != "" || authority.ConsumerServiceID != "" || authority.ConsumerCandidateSHA256 != "" || authority.Commands > MaximumGrants+1 || authority.Custom && authority.Commands != 1 {
		return invalid("Attach provisioning has unexpected connection authority")
	}
	return nil
}

func (authority ProvisionAuthority) IsConsumerCommand(ordinal uint32) bool {
	return authority.ConsumerSHA256 != "" && authority.Commands > 0 && ordinal == authority.Commands-1
}
