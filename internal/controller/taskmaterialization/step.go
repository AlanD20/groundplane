package taskmaterialization

import (
	"bytes"
	"encoding/hex"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BuildTaskMaterializationStep projects one validated durable record into the
// authenticated Agent plan without resolving its content bytes.
func BuildTaskMaterializationStep(
	reference materializationrecord.Record,
	artifactID string,
	timeoutSeconds uint32,
) (*agentpb.ExecutionStep, error) {
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "durable materialization digest is corrupt")
	}
	outputKind, err := taskMaterializationOutputKind(reference.OutputKind)
	if err != nil {
		return nil, err
	}
	return &agentpb.ExecutionStep{
		StepId: reference.StepID, TimeoutSeconds: timeoutSeconds,
		Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: &agentpb.MaterializeFile{
			ArtifactId: artifactID, MaterializationId: reference.MaterializationID,
			EnvironmentId: reference.EnvironmentID, Destination: reference.Destination,
			ServiceId: reference.ServiceID, ServiceName: reference.ServiceName,
			OutputKind: outputKind, Uid: reference.UID, Gid: reference.GID, Mode: reference.Mode,
			Length: reference.Length, Sha256: digest,
		}},
	}, nil
}

func taskMaterializationOutputKind(
	value materializationrecord.OutputKind,
) (agentpb.MaterializationOutputKind, error) {
	switch value {
	case materializationrecord.OutputGeneratedEnvironment:
		return agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV, nil
	case materializationrecord.OutputPlainFile:
		return agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE, nil
	case materializationrecord.OutputSecretFile:
		return agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_SECRET_FILE, nil
	case materializationrecord.OutputRemoveGeneratedEnv:
		return agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_GENERATED_ENV, nil
	case materializationrecord.OutputRemovePlainFile:
		return agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_PLAIN_FILE, nil
	case materializationrecord.OutputRemoveSecretFile:
		return agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_SECRET_FILE, nil
	default:
		return 0, errs.New(errs.KindInternal, "durable materialization output kind is corrupt")
	}
}

func taskMaterializationMetadataMatches(
	reference materializationrecord.Record,
	materialization *agentpb.MaterializeFile,
) bool {
	if materialization == nil {
		return false
	}
	outputKind, err := taskMaterializationOutputKind(reference.OutputKind)
	if err != nil {
		return false
	}
	digest, err := hex.DecodeString(reference.SHA256)
	return err == nil && reference.MaterializationID == materialization.MaterializationId &&
		reference.EnvironmentID == materialization.EnvironmentId &&
		reference.Destination == materialization.Destination && reference.ServiceID == materialization.ServiceId &&
		reference.ServiceName == materialization.ServiceName && outputKind == materialization.OutputKind &&
		reference.UID == materialization.Uid && reference.GID == materialization.Gid &&
		reference.Mode == materialization.Mode && reference.Length == materialization.Length &&
		bytes.Equal(digest, materialization.Sha256)
}
