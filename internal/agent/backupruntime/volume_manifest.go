package backupruntime

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type VolumeExchange interface {
	SendVolumeFrame(context.Context, backupvolumetransfer.Binding, *agentpb.BackupVolumeManifestTransfer) error
	SendVolumeCredit(context.Context, backupvolumetransfer.Binding, *agentpb.BackupVolumeManifestAckCredit) error
	ReceiveVolumeCredit(context.Context, backupvolumetransfer.Binding) (*agentpb.BackupVolumeManifestAckCredit, error)
	ReceiveVolumeFrame(context.Context, backupvolumetransfer.Binding) (*agentpb.BackupVolumeManifestTransfer, error)
}

func sendVolumeManifest(ctx context.Context, binding backupvolumetransfer.Binding, step *agentpb.BackupStepAuthority,
	role agentpb.BackupVolumeManifestRole, entries []backupvolume.Entry, archive *backupvolume.ArtifactEvidence, exchange VolumeExchange,
) error {
	pointID, generation := step.GetCapture().GetPointId(), ""
	if binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE {
		pointID, generation = step.GetRestore().GetPointId(), step.GetExecutionId()
	}
	frames, err := backupvolumetransfer.BuildFrames(binding, pointID, generation, role, entries, archive)
	if err != nil {
		return err
	}
	credit, err := exchange.ReceiveVolumeCredit(ctx, binding)
	if err != nil {
		return err
	}
	index, err := frames.ResumeIndex(binding, credit)
	if err != nil {
		return err
	}
	for ; index < len(frames.Frames); index++ {
		frame := frames.Frames[index]
		if err := exchange.SendVolumeFrame(ctx, binding, frame); err != nil {
			return err
		}
		credit, err = exchange.ReceiveVolumeCredit(ctx, binding)
		if err != nil {
			return err
		}
		if backupvolumetransfer.ValidateCredit(binding, credit) != nil || credit.CommittedRecordSequence != frame.RecordSequence || credit.NextOrdinal != frames.Next[index] || !bytes.Equal(credit.TransferChainSha256, frames.Chains[index][:]) {
			return errs.New(errs.KindStateConflict, "Volume manifest acknowledgement differs from the sent record")
		}
	}
	return nil
}
