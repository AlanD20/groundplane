package hierarchydeletion

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Reports can account for a previously authorized effect after revocation.
// Reading its captured input does not grant the next stage permission.
func (owner *AgentWork) capturedEnvironmentAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.EnvironmentRemovalAssignment, error) {
	item, attempt := view.Work.Item.Item, view.Attempt.Attempt
	captured, err := owner.records.LoadHierarchyEnvironmentInput(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	defer clear(captured.Input.ComposeArtifact)
	raw, err := hierarchydeletion.EncodeEnvironmentInput(captured.Input)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	removal := &agentpb.EnvironmentRemovalInput{
		CapturedSource: &agentpb.WorkInputReference{InputId: ids.DeriveAt(ids.KindPlan, view.Work.Task.Task.CreatedAt, item.ID, "environment-removal-source"), Sha256: digest[:]},
		EnvironmentId:  captured.Input.EnvironmentID, ExpectedVolumeDir: captured.Input.VolumeDirectory,
	}
	if len(captured.Input.ComposeArtifact) != 0 {
		removal.Compose = &agentpb.ComposeArtifact{}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(captured.Input.ComposeArtifact, removal.Compose); err != nil {
			return nil, errs.New(errs.KindStateConflict, "captured Environment removal artifact is invalid")
		}
	}
	input := hierarchyexecution.EnvironmentInput{Attempt: attempt, Removal: removal}
	if attempt.Direction == workexecution.Recovery {
		source, err := owner.records.LoadHierarchyEnvironmentRecovery(ctx, view.Work)
		if err != nil {
			return nil, err
		}
		input.Recovery = &source.Source
	}
	return hierarchyexecution.EncodeEnvironmentAssignment(input)
}
