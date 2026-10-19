package app

import (
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-77 / VOL-03: losing a resource claim stops new removal grants, but does
// not discard an acknowledged effect's replay/confirmation. This distinction
// prevents both unauthorized deletion and recovery stuck on an unrecordable result.
func TestNativeVolumeClaimLossSeparatesNewGrantsFromEffectReceipts(t *testing.T) {
	for claimIndex := range 3 {
		for _, receipt := range []bool{false, true} {
			t.Run(strconv.Itoa(claimIndex)+"/receipt="+strconv.FormatBool(receipt), func(t *testing.T) {
				f := newNativeVolumeFixture(t)
				view := f.start(t, f.created.TaskID)
				progress := f.progress(t, view)
				for progress.Stage != volumeexecution.Complete {
					progress = f.advance(t, view, progress)
				}
				f.complete(t, view, progress)
				view, _, _ = f.remove(t)
				input, err := f.inputs.LoadVolumeWorkInput(t.Context(), view.Work)
				if err != nil {
					t.Fatal(err)
				}
				progress = f.progress(t, view)
				progress, err = progress.Begin(view.Attempt.Attempt, input.Action, input.VolumeId, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				wire, err := workexecution.EncodeAgentAttempt(view.Attempt.Attempt)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := volumeexecution.EncodeProgress(view.Attempt.Attempt, input.Action, input.VolumeId, progress)
				if err != nil {
					t.Fatal(err)
				}
				request := &agentpb.VolumeWorkCheckpointRequest{Attempt: wire, Progress: raw}
				if receipt {
					f.checkpoint(t, view, progress)
				}
				if len(view.Work.Item.Item.Claims) != 3 {
					t.Fatal("fixture requires Volume and both Environment claims")
				}
				key := workqueue.ClaimKey(view.Work.Item.Item.Claims[claimIndex])
				result, err := f.store.Transact(
					t.Context(),
					nil,
					[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}},
				)
				if err != nil || !result.Succeeded {
					t.Fatal("could not remove the captured ownership claim", err)
				}
				ack, err := f.reports.CheckpointVolume(t.Context(), f.authorization, request)
				if !receipt {
					if err == nil || ack != nil {
						t.Fatal("missing resource claim granted a new removal stage")
					}
					return
				}
				if err != nil || ack == nil || ack.Sequence != 1 {
					t.Fatal("claim loss discarded acknowledged effect receipt", err)
				}
				progress = f.confirm(t, view, progress)
				next, err := progress.Begin(view.Attempt.Attempt, input.Action, input.VolumeId, time.Now().UTC())
				if err != nil {
					t.Fatal(err)
				}
				request.Progress, err = volumeexecution.EncodeProgress(
					view.Attempt.Attempt,
					input.Action,
					input.VolumeId,
					next,
				)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := f.reports.CheckpointVolume(t.Context(), f.authorization, request); err == nil {
					t.Fatal("accepting the old receipt authorized another destructive stage")
				}
				// Consumer detachment has no authority to discard the Volume's
				// independently applied storage state before full absence proof.
				applied, err := f.inputs.ReadApplied(t.Context(), f.created.Volume.ID, 0)
				if err != nil || applied == nil {
					t.Fatal("unsettled removal lost applied Volume state", err)
				}
			})
		}
	}
}
