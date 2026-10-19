package executionplan

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateVolumeWorkInput checks captured content without fabricating an old
// ExecutionPlan. Admission still owns impact approval, and the Agent must check
// its configured root and actual Docker/filesystem ownership before any effect.
func ValidateVolumeWorkInput(input *agentpb.VolumeWorkInput) error {
	if input == nil || proto.Size(input) > MaximumPlanBytes ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentId) != nil ||
		ids.Validate(ids.KindVolume, input.VolumeId) != nil || volumeidentity.ValidateKey(input.ComposeKey) != nil ||
		len(input.IntentSha256) != sha256.Size {
		return invalidVolumeWork()
	}
	if err := RejectUnknown(input); err != nil {
		return err
	}
	if err := validateArtifactContent(input.Candidate); err != nil {
		return err
	}
	if input.Candidate.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		input.Candidate.OwnerId != input.EnvironmentId {
		return invalidVolumeWork()
	}
	// Persistent Volume labels have no execution identity. Their validation
	// does not require, or manufacture, a generic execution plan.
	if err := validateVolumes(nil, input.Candidate); err != nil {
		return err
	}
	switch input.Action {
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_CREATE, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_VERIFY:
		if input.Baseline != nil || len(input.ConsumerIds) != 0 || len(input.RemovalImpactSha256) != 0 {
			return invalidVolumeWork()
		}
		selected := false
		for _, volume := range input.Candidate.Volumes {
			if volume.GetVolumeId() != input.VolumeId {
				continue
			}
			if selected || volume.ComposeName != input.ComposeKey ||
				volume.DockerName != "gp_vol_"+strings.ToLower(input.VolumeId) {
				return invalidVolumeWork()
			}
			selected = true
		}
		if !selected {
			return invalidVolumeWork()
		}
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE:
		if len(input.RemovalImpactSha256) != sha256.Size {
			return invalidVolumeWork()
		}
		if err := validateArtifactContent(input.Baseline); err != nil {
			return err
		}
		if err := validateVolumes(nil, input.Baseline); err != nil {
			return err
		}
		if _, err := (volumeexecution.Removal{
			VolumeID: input.VolumeId, Key: input.ComposeKey,
			Baseline: input.Baseline, Candidate: input.Candidate, ConsumerIDs: input.ConsumerIds,
		}).Consumers(); err != nil {
			return err
		}
	default:
		return invalidVolumeWork()
	}
	return nil
}

func invalidVolumeWork() error {
	return errs.New(
		errs.KindValidationFailed,
		"Volume execution requires its captured operation, owner, immutable key and removal approval",
	)
}

// CaptureVolumeWorkInput returns an owned message and deterministic bytes for
// immutable input storage. Its reference must be published atomically with work.
func CaptureVolumeWorkInput(
	inputID string,
	input *agentpb.VolumeWorkInput,
) (*agentpb.VolumeWorkInput, []byte, workexecution.Input, error) {
	if err := ValidateVolumeWorkInput(input); err != nil {
		return nil, nil, workexecution.Input{}, err
	}
	owned := proto.Clone(input).(*agentpb.VolumeWorkInput)
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

func OpenVolumeWorkInput(
	attempt workexecution.Attempt,
	input *agentpb.VolumeWorkInput,
) (*agentpb.VolumeWorkInput, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent ||
		attempt.Direction != workexecution.Forward || attempt.FinishedAt != nil {
		return nil, invalidVolumeWork()
	}
	owned, _, reference, err := CaptureVolumeWorkInput(attempt.Input.ID, input)
	if err != nil {
		return nil, err
	}
	if reference != attempt.Input {
		return nil, errs.New(errs.KindStateConflict, "Volume assignment content does not match its captured input")
	}
	return owned, nil
}
