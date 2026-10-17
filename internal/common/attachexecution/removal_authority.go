package attachexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"google.golang.org/protobuf/proto"
)

// RemovalAuthority is the non-secret portion an Agent can retain in its local
// journal. Credentials and hook values exist only in transient assignment input.
type RemovalAuthority struct {
	Source                  workexecution.Input `json:"source"`
	AttachID                string              `json:"attach_id"`
	BackingServiceID        string              `json:"backing_service_id"`
	Commands                uint32              `json:"commands"`
	Custom                  bool                `json:"custom"`
	HasConsumer             bool                `json:"has_consumer"`
	ConsumerOrdinal         uint32              `json:"consumer_ordinal"`
	ConsumerSHA256          string              `json:"consumer_sha256,omitempty"`
	ConsumerNetworkID       string              `json:"consumer_network_id,omitempty"`
	ConsumerServiceID       string              `json:"consumer_service_id,omitempty"`
	ConsumerCandidateSHA256 string              `json:"consumer_candidate_sha256,omitempty"`
}

func (input RemovalInput) Authority() (RemovalAuthority, error) {
	if err := input.Validate(); err != nil {
		return RemovalAuthority{}, err
	}
	authority := RemovalAuthority{Source: workexecution.Input{ID: input.Removal.CapturedSource.InputId,
		SHA256: hex.EncodeToString(input.Removal.CapturedSource.Sha256)}, AttachID: input.Removal.AttachId,
		BackingServiceID: input.Removal.BackingServiceId, Commands: input.CommandCount(), Custom: input.Removal.GetCustomDetach() != nil}
	if consumer := input.Removal.Consumer; consumer != nil {
		raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(consumer)
		if err != nil {
			return RemovalAuthority{}, invalid("Detach consumer cannot be captured")
		}
		digest := sha256.Sum256(raw)
		clear(raw)
		authority.HasConsumer = true
		ordinal, present := input.ConsumerCommandOrdinal()
		if !present {
			return RemovalAuthority{}, invalid("Detach consumer command is missing")
		}
		authority.ConsumerOrdinal = ordinal
		authority.ConsumerSHA256 = hex.EncodeToString(digest[:])
		authority.ConsumerNetworkID, authority.ConsumerServiceID = consumer.NetworkId, consumer.ServiceId
		authority.ConsumerCandidateSHA256 = hex.EncodeToString(consumer.Candidate.YamlSha256)
	}
	return authority, nil
}

func (authority RemovalAuthority) Validate(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || attempt.Executor.Executor != workexecution.Agent ||
		authority.Source.Validate() != nil || ids.Validate(ids.KindAttach, authority.AttachID) != nil ||
		ids.Validate(ids.KindService, authority.BackingServiceID) != nil || authority.Commands == 0 || authority.Commands > MaximumGrants+2 {
		return invalid("Attach removal journal authority is invalid")
	}
	if authority.HasConsumer {
		if ids.Validate(ids.KindNetwork, authority.ConsumerNetworkID) != nil ||
			ids.Validate(ids.KindService, authority.ConsumerServiceID) != nil ||
			!validConsumerDigest(authority.ConsumerSHA256) || !validConsumerDigest(authority.ConsumerCandidateSHA256) ||
			authority.ConsumerOrdinal >= authority.Commands ||
			authority.Commands == 1 && authority.ConsumerOrdinal != 0 ||
			authority.Commands > 1 && authority.ConsumerOrdinal != authority.Commands-2 ||
			authority.Custom && authority.Commands != 2 {
			return invalid("Detach consumer journal authority is invalid")
		}
	} else if authority.ConsumerOrdinal != 0 || authority.ConsumerSHA256 != "" || authority.ConsumerCandidateSHA256 != "" ||
		authority.ConsumerNetworkID != "" || authority.ConsumerServiceID != "" ||
		authority.Commands > MaximumGrants+1 || authority.Custom && authority.Commands != 1 {
		return invalid("Attach command authority contains an unexpected consumer transition")
	}
	return nil
}

func validConsumerDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
