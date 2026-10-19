package releaseexecution

import (
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeConfigurationFile(header entrymaterialization.Header) *agentpb.ReleaseConfigurationFile {
	digest := header.Digest()
	return &agentpb.ReleaseConfigurationFile{
		Destination: header.Destination(),
		ServiceId:   header.ServiceID(),
		ServiceName: header.ServiceName(),
		OutputKind: header.OutputKind().
			String(),
		Uid:    header.UID(),
		Gid:    header.GID(),
		Mode:   uint32(header.Mode()),
		Length: header.Length(),
		Sha256: slices.Clone(digest[:]),
	}
}

func DecodeConfigurationFile(
	input Input,
	report CheckpointReport,
	file *agentpb.ReleaseConfigurationFile,
) (entrymaterialization.Header, error) {
	if file == nil || executionplan.RejectUnknown(file) != nil || input.Release == nil ||
		input.Release.Configuration == nil ||
		report.Ack == nil ||
		report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(input) != nil ||
		!report.Progress.InFlight ||
		report.Progress.Phase != Configuration ||
		len(file.Sha256) != sha256.Size ||
		file.Length > entrymaterialization.MaximumContentBytes ||
		!slices.Contains(input.Release.ConfigurationDestinations, file.Destination) {
		return entrymaterialization.Header{}, invalidProgress()
	}
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID {
		return entrymaterialization.Header{}, invalidProgress()
	}
	kind, err := entrymaterialization.ParseOutputKind(file.OutputKind)
	if err != nil {
		return entrymaterialization.Header{}, err
	}
	var digest entrymaterialization.Digest
	copy(digest[:], file.Sha256)
	return entrymaterialization.NewHeader(
		entrymaterialization.HeaderSpec{
			TaskID:        input.Attempt.TaskID,
			StepID:        stage,
			EnvironmentID: input.Release.EnvironmentId,
			Generation:    input.Release.Configuration.Generation,
			Destination:   file.Destination,
			ServiceID:     file.ServiceId,
			ServiceName:   file.ServiceName,
			OutputKind:    kind,
			UID:           file.Uid,
			GID:           file.Gid,
			Mode:          entrymaterialization.Mode(file.Mode),
			Length:        file.Length,
			Digest:        digest,
		},
	)
}

func ClearConfigurationDelivery(delivery *agentpb.ReleaseConfigurationFileDelivery) {
	if delivery != nil {
		clear(delivery.Content)
		delivery.Content = nil
	}
}
