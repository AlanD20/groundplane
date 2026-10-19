package executionplan

import (
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RemovalConfigurationHeaders authenticates the entire metadata selection before
// any file is materialized. The Controller selects these sources from its sealed
// snapshot; the Agent does not infer files from a current Blueprint.
func RemovalConfigurationHeaders(
	request *agentpb.ServiceRemovalRecoveryHelperRequest,
	manifest *agentpb.ServiceRemovalConfigurationManifest,
) ([]entrymaterialization.Header, error) {
	input, err := OpenRemovalRecoveryHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if input.Progress.Phase != removalexecution.RestoreConfiguration || manifest == nil ||
		RejectUnknown(manifest) != nil ||
		!proto.Equal(manifest.Attempt, request.Assignment.Attempt) ||
		manifest.StageId != input.Checkpoint.StageID ||
		!proto.Equal(manifest.Configuration, request.Assignment.Configuration) ||
		len(manifest.Files) > 512 ||
		manifest.Configuration == nil && len(manifest.Files) != 0 {
		return nil, invalidRemovalHelper()
	}
	headers := make([]entrymaterialization.Header, len(manifest.Files))
	for index, file := range manifest.Files {
		if file == nil || len(file.Sha256) != sha256.Size || file.Length > entrymaterialization.MaximumContentBytes ||
			index > 0 && manifest.Files[index-1].Destination >= file.Destination {
			return nil, invalidRemovalHelper()
		}
		kind, err := entrymaterialization.ParseOutputKind(file.OutputKind)
		if err != nil {
			return nil, err
		}
		var digest entrymaterialization.Digest
		copy(digest[:], file.Sha256)
		header, err := entrymaterialization.NewHeader(entrymaterialization.HeaderSpec{TaskID: input.Attempt.TaskID,
			StepID: manifest.StageId, EnvironmentID: input.Procedure.EnvironmentId, Generation: manifest.Configuration.Generation,
			Destination: file.Destination, ServiceID: file.ServiceId, ServiceName: file.ServiceName, OutputKind: kind,
			UID: file.Uid, GID: file.Gid, Mode: entrymaterialization.Mode(file.Mode), Length: file.Length, Digest: digest})
		if err != nil {
			return nil, err
		}
		headers[index] = header
	}
	return headers, nil
}

func RemovalConfigurationFileRoot(
	request *agentpb.ServiceRemovalRecoveryHelperRequest,
	manifest *agentpb.ServiceRemovalConfigurationManifest,
	header entrymaterialization.Header,
) (string, error) {
	headers, err := RemovalConfigurationHeaders(request, manifest)
	if err != nil {
		return "", err
	}
	if !slices.Contains(headers, header) || len(request.Assignment.Removal.Members) == 0 {
		return "", invalidRemovalHelper()
	}
	root := request.Assignment.Removal.Members[0].Artifact.AuthorizedVolumeDir
	for _, member := range request.Assignment.Removal.Members {
		if member.Artifact.AuthorizedVolumeDir != root {
			return "", invalidRemovalHelper()
		}
	}
	return root, nil
}
