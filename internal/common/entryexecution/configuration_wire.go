package entryexecution

import (
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func EncodeConfigurationFile(header entrymaterialization.Header) *agentpb.EntryConfigurationFile {
	digest := header.Digest()
	return &agentpb.EntryConfigurationFile{
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
	input FileInput,
	file *agentpb.EntryConfigurationFile,
) (entrymaterialization.Header, error) {
	if file == nil || executionplan.RejectUnknown(file) != nil || len(file.Sha256) != sha256.Size ||
		file.Length > entrymaterialization.MaximumContentBytes {
		return entrymaterialization.Header{}, invalidLayout()
	}
	_, stage, generation, err := ConfigurationSelection(input)
	if err != nil {
		return entrymaterialization.Header{}, err
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
			EnvironmentID: input.Procedure.EnvironmentId,
			Generation:    generation,
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

func ClearConfigurationDelivery(delivery *agentpb.EntryConfigurationFileDelivery) {
	if delivery != nil {
		clear(delivery.Content)
		delivery.Content = nil
	}
}
