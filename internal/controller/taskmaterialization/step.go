package taskmaterialization

import (
	"encoding/hex"

	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BuildFileDescriptor carries captured file metadata to a typed operation.
// Content remains in its retained source; no generic execution step is required.
func BuildFileDescriptor(reference materializationrecord.Record, artifactID string) (*agentpb.MaterializeFile, error) {
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil || len(digest) != 32 {
		return nil, errs.New(errs.KindInternal, "durable materialization digest is corrupt")
	}
	outputKind, err := taskMaterializationOutputKind(reference.OutputKind)
	if err != nil {
		return nil, err
	}
	return &agentpb.MaterializeFile{
		ArtifactId: artifactID, MaterializationId: reference.MaterializationID,
		EnvironmentId: reference.EnvironmentID, Destination: reference.Destination,
		ServiceId: reference.ServiceID, ServiceName: reference.ServiceName,
		OutputKind: outputKind, Uid: reference.UID, Gid: reference.GID, Mode: reference.Mode,
		Length: reference.Length, Sha256: digest,
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
