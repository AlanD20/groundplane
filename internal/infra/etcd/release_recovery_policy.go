package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The captured policy can retain a candidate only after the original operation
// proved that complete candidate serving. Partial or unknown rollouts still own
// their captured restoration, not a manufactured successful runtime.
func (ledger *ReleaseLedger) SelectReleaseRecoveryDisposition(
	ctx context.Context,
	view workqueue.ResumeView,
	input releaseexecution.RecoveryInput,
) (agentpb.WorkReleaseRecoveryDisposition, []keyvalue.Condition, error) {
	if ledger == nil || ledger.store == nil {
		return 0, nil, errs.New(errs.KindInternal, "Release recovery policy has no retained source reader")
	}
	proof, err := releasework.LoadPreservedServing(ctx, ledger.store, input, view.Work.ReadRevision)
	if err != nil {
		return 0, nil, err
	}
	disposition := agentpb.WorkReleaseRecoveryDisposition_WORK_RELEASE_RECOVERY_DISPOSITION_RESTORE_CAPTURED
	if proof.Proof != nil {
		disposition = agentpb.WorkReleaseRecoveryDisposition_WORK_RELEASE_RECOVERY_DISPOSITION_PRESERVE_SERVING
	}
	return disposition, proof.Conditions, nil
}
