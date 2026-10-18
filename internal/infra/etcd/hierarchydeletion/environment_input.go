package hierarchydeletion

import (
	"path"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// EnvironmentInput freezes only host cleanup data, not the full desired state
// or a second Task. Its digest is sealed into the removal action's procedure.
type EnvironmentInput struct {
	OperationID         string `json:"operation_id"`
	SnapshotRevision    int64  `json:"snapshot_revision"`
	EnvironmentID       string `json:"environment_id"`
	EnvironmentRevision int64  `json:"environment_revision"`
	VolumeDirectory     string `json:"volume_directory"`
	ComposeArtifact     []byte `json:"compose_artifact,omitempty"`
}

const environmentInputBytes = 4 << 20

func EnvironmentInputKey(operationID, environmentID string) string {
	return EnvironmentInputPrefix(operationID) + environmentID
}

func EnvironmentInputPrefix(operationID string) string {
	return "/v1/hierarchy-removal-environments/" + operationID + "/"
}

func EncodeEnvironmentInput(input EnvironmentInput) ([]byte, error) {
	if err := validateEnvironmentInput(input); err != nil {
		return nil, err
	}
	return EncodeHierarchyDeletionRecord(input, environmentInputBytes)
}

func DecodeEnvironmentInput(raw []byte) (EnvironmentInput, error) {
	var input EnvironmentInput
	if err := DecodeHierarchyDeletionRecord(raw, environmentInputBytes, &input); err != nil {
		return EnvironmentInput{}, err
	}
	if err := validateEnvironmentInput(input); err != nil {
		clear(input.ComposeArtifact)
		return EnvironmentInput{}, err
	}
	return input, nil
}

func validateEnvironmentInput(input EnvironmentInput) error {
	if !ValidHierarchyDeletionPrivateID(input.OperationID, "del") || ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		input.SnapshotRevision <= 0 || input.EnvironmentRevision <= 0 || input.EnvironmentRevision > input.SnapshotRevision ||
		!path.IsAbs(input.VolumeDirectory) || path.Clean(input.VolumeDirectory) != input.VolumeDirectory ||
		path.Base(input.VolumeDirectory) != input.EnvironmentID || len(input.ComposeArtifact) > environmentInputBytes {
		return CorruptHierarchyDeletion()
	}
	return nil
}
