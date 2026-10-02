package backupconfigmaterialization

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareHeaders renders and hashes every bounded output before any host write.
// The Controller and Agent use independent value owners and the same encoding.
func (plan *Plan) PrepareHeaders(
	ctx context.Context,
	taskID, stepID string,
	read ReadValue,
) ([]entrymaterialization.Header, error) {
	if plan == nil {
		return nil, invalid()
	}
	headers := make([]entrymaterialization.Header, 0, len(plan.outputs))
	for index := range plan.outputs {
		content, err := plan.Render(ctx, index, read)
		if err != nil {
			clear(content)
			return nil, err
		}
		header, err := plan.Header(taskID, stepID, index, content)
		clear(content)
		if err != nil {
			return nil, err
		}
		headers = append(headers, header)
	}
	return headers, nil
}

func (plan *Plan) Proof(headers []entrymaterialization.Header) (*agentpb.BackupConfigMaterializationVerified, error) {
	if plan == nil || len(headers) != len(plan.outputs) {
		return nil, invalid()
	}
	contextSHA256, err := ContextSHA256(plan.authority.DestinationEnvironmentId, plan.authority.Files)
	if err != nil {
		return nil, err
	}
	contextDigest, err := hex.DecodeString(contextSHA256)
	if err != nil {
		return nil, invalid()
	}
	var encoded []byte
	appendText := func(value string) {
		encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(value)))
		encoded = append(encoded, value...)
	}
	appendText("groundplane.backup.config-materialization.v1")
	appendText(plan.authority.RestoreGenerationId)
	encoded = append(encoded, contextDigest...)
	encoded = append(encoded, plan.authority.ExpectedArchive.Content.ManifestSha256...)
	encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(headers)))
	for index, header := range headers {
		metadata := plan.outputs[index].metadata
		if header.EnvironmentID() != metadata.EnvironmentID || header.Generation() != metadata.Generation ||
			header.Destination() != metadata.Destination || header.ServiceID() != metadata.ServiceID ||
			header.ServiceName() != metadata.ServiceName || header.OutputKind() != metadata.OutputKind ||
			header.UID() != metadata.UID || header.GID() != metadata.GID || header.Mode() != metadata.Mode ||
			(index > 0 && (header.TaskID() != headers[0].TaskID() || header.StepID() != headers[0].StepID())) {
			clear(encoded)
			return nil, invalid()
		}
		appendText(header.TaskID())
		appendText(header.StepID())
		appendText(header.EnvironmentID())
		encoded = binary.BigEndian.AppendUint64(encoded, header.Generation())
		appendText(header.Destination())
		appendText(header.ServiceID())
		appendText(header.ServiceName())
		encoded = append(encoded, byte(header.OutputKind()))
		encoded = binary.BigEndian.AppendUint32(encoded, header.UID())
		encoded = binary.BigEndian.AppendUint32(encoded, header.GID())
		encoded = binary.BigEndian.AppendUint32(encoded, uint32(header.Mode()))
		encoded = binary.BigEndian.AppendUint64(encoded, header.Length())
		digest := header.Digest()
		encoded = append(encoded, digest[:]...)
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	return &agentpb.BackupConfigMaterializationVerified{RestoreGenerationId: plan.authority.RestoreGenerationId,
		MaterializedEntryCount: uint32(
			len(plan.entries),
		), MaterializationSha256: append([]byte(nil), digest[:]...)}, nil
}
