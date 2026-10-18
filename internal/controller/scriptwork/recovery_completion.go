package scriptwork

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) LoadRecoveryInput(ctx context.Context, view workqueue.ResumeView) (scriptexecution.RecoveryInput, error) {
	return service.repository.LoadScriptRecoveryInput(ctx, view)
}

// CompleteRecovery acknowledges the exact proof on replay, even after retained
// inputs have closed to execution. It never repeats cleanup or opens a new attempt.
func (service *Service) CompleteRecovery(ctx context.Context, authorization agentregistration.WorkAuthorization,
	claimed workexecution.Attempt, raw []byte, reference workexecution.Input,
) (workexecution.Attempt, error) {
	if ctx == nil || service == nil || claimed.Validate() != nil || claimed.Direction != workexecution.Recovery ||
		claimed.Executor != authorization.Identity() || reference.Validate() != nil || len(raw) == 0 || len(raw) > 16<<10 {
		return workexecution.Attempt{}, errs.New(errs.KindValidationFailed, "Script recovery completion is invalid")
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != reference.SHA256 {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Script recovery proof digest differs")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return workexecution.Attempt{}, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if view.Work.Item.Item.Operation != workexecution.Script || !workexecution.SameAssignment(view.Attempt.Attempt, claimed) {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "Script recovery completion belongs to another assignment")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		if err := service.repository.SettleRecoveredScript(ctx, view, raw, reference, guards, time.Now().UTC()); err != nil {
			return workexecution.Attempt{}, err
		}
		view, err = authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
		if err != nil {
			return workexecution.Attempt{}, err
		}
	}
	attempt := view.Attempt.Attempt
	if !workexecution.SameAssignment(attempt, claimed) || attempt.FinishedAt == nil || attempt.Result == nil ||
		*attempt.Result != reference || attempt.Outcome != workexecution.Succeeded {
		return workexecution.Attempt{}, errs.New(errs.KindStateConflict, "saved Script recovery does not acknowledge this proof")
	}
	return attempt, nil
}
