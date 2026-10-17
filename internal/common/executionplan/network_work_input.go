package executionplan

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Network work never runs Compose against an Environment-wide artifact. Its
// host effects are limited to one owned bridge and explicitly captured consumer
// endpoints. Blueprint removals order consumer changes through their own work.
func ValidateNetworkWorkInput(input *agentpb.NetworkWorkInput) error {
	if input == nil || proto.Size(input) > MaximumPlanBytes ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentId) != nil || len(input.IntentSha256) != sha256.Size {
		return invalidNetworkWork()
	}
	if err := RejectUnknown(input); err != nil {
		return err
	}
	name, err := networkname.New(input.NetworkId)
	if err != nil || name != input.DockerName {
		return invalidNetworkWork()
	}
	switch input.Action {
	case agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_ENSURE:
		if len(input.DisconnectServiceIds) != 0 || input.RemovalSubnet != "" {
			return invalidNetworkWork()
		}
		artifact := input.Candidate
		if err := validateArtifactContent(artifact); err != nil {
			return err
		}
		if artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			artifact.OwnerId != input.EnvironmentId || len(artifact.Services) != 0 || len(artifact.Volumes) != 0 ||
			len(artifact.Networks) != 1 || artifact.Networks[0] == nil ||
			artifact.Networks[0].NetworkId != input.NetworkId || artifact.Networks[0].DockerName != name {
			return invalidNetworkWork()
		}
		return validateNetworks(nil, artifact)
	case agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE:
		if input.Candidate != nil || len(input.DisconnectServiceIds) > 4096 ||
			!slices.IsSorted(input.DisconnectServiceIds) {
			return invalidNetworkWork()
		}
		for index, serviceID := range input.DisconnectServiceIds {
			if ids.Validate(ids.KindService, serviceID) != nil ||
				index > 0 && input.DisconnectServiceIds[index-1] == serviceID {
				return invalidNetworkWork()
			}
		}
		if input.RemovalSubnet != "" {
			subnet, err := ipam.ParseIPv4Prefix(input.RemovalSubnet)
			if err != nil || subnet.String() != input.RemovalSubnet || ipam.ValidateBridgeSubnet(subnet) != nil {
				return invalidNetworkWork()
			}
		}
		return nil
	default:
		return invalidNetworkWork()
	}
}

func CaptureNetworkWorkInput(inputID string, input *agentpb.NetworkWorkInput) (*agentpb.NetworkWorkInput, []byte, workexecution.Input, error) {
	if err := ValidateNetworkWorkInput(input); err != nil {
		return nil, nil, workexecution.Input{}, err
	}
	owned := proto.Clone(input).(*agentpb.NetworkWorkInput)
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, nil, workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(raw)
	reference := workexecution.Input{ID: inputID, SHA256: hex.EncodeToString(digest[:])}
	if err := reference.Validate(); err != nil {
		return nil, nil, workexecution.Input{}, err
	}
	return owned, raw, reference, nil
}

func OpenNetworkWorkInput(attempt workexecution.Attempt, input *agentpb.NetworkWorkInput) (*agentpb.NetworkWorkInput, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent ||
		attempt.Direction != workexecution.Forward || attempt.FinishedAt != nil {
		return nil, invalidNetworkWork()
	}
	owned, _, reference, err := CaptureNetworkWorkInput(attempt.Input.ID, input)
	if err != nil {
		return nil, err
	}
	if reference != attempt.Input {
		return nil, errs.New(errs.KindStateConflict, "Network assignment content does not match its captured input")
	}
	return owned, nil
}

func invalidNetworkWork() error {
	return errs.New(errs.KindValidationFailed, "Network execution requires one captured managed bridge and its Environment owner")
}
