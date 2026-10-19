package backupwork

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// A receipt keeps the cumulative source cursor at that exact checkpoint.
// Replaying an old acknowledgement must not return a later source state.
type checkpointReceipt struct {
	Request []byte `json:"request"`
	Source  []byte `json:"source"`
}

func encodeCheckpointReceipt(
	request *agentpb.BackupWorkCheckpointRequest,
	source *agentpb.BackupStepResume,
) ([]byte, error) {
	rawRequest, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, conflict("Backup checkpoint request cannot be encoded")
	}
	defer clear(rawRequest)
	rawSource, err := (proto.MarshalOptions{Deterministic: true}).Marshal(source)
	if err != nil {
		return nil, conflict("Backup checkpoint source cannot be encoded")
	}
	defer clear(rawSource)
	raw, err := recordcodec.Encode("backup-work-checkpoint", checkpointReceipt{Request: rawRequest, Source: rawSource})
	if err != nil {
		return nil, err
	}
	if len(raw) > executionplan.MaximumPlanBytes {
		clear(raw)
		return nil, conflict("Backup checkpoint receipt exceeds its storage limit")
	}
	return raw, nil
}

func decodeCheckpointReceipt(raw []byte) (*agentpb.BackupWorkCheckpointRequest, *agentpb.BackupStepResume, error) {
	record, err := recordcodec.Decode[checkpointReceipt](raw, "backup-work-checkpoint")
	if err != nil {
		return nil, nil, err
	}
	defer clear(record.Request)
	defer clear(record.Source)
	request, source := &agentpb.BackupWorkCheckpointRequest{}, &agentpb.BackupStepResume{}
	if proto.Unmarshal(record.Request, request) != nil || proto.Unmarshal(record.Source, source) != nil ||
		executionplan.RejectUnknown(request) != nil || executionplan.RejectUnknown(source) != nil {
		return nil, nil, conflict("Backup checkpoint receipt is invalid")
	}
	sequence, fence := sourceCheckpoint(source)
	if sequence == 0 || sequence != request.CheckpointSequence || fence != nil {
		return nil, nil, conflict("Backup checkpoint receipt source differs from its report")
	}
	return request, source, nil
}
