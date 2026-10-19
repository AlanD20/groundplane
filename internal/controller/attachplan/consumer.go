package attachplan

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ConsumerTransition uses only applied runtime and the captured complete
// binding union. Pending command/image/Entry edits never leak into Detach.
// Nil means that no applied consumer exists, or another binding retains the
// same edge; credential/hook cleanup remains independently required.
func ConsumerTransition(
	ctx context.Context,
	snapshot attachremoval.DetachSnapshot,
	cascade *attachremoval.CascadeStep,
	artifactID string,
) (*agentpb.AttachConsumerTransition, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindValidationFailed, "Detach consumer planning requires a context")
	}
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	if err := cascade.Validate(snapshot); err != nil {
		return nil, err
	}
	var removed []string
	if cascade != nil {
		removed = cascade.RemovedBefore
	}
	if snapshot.Runtime == nil || snapshot.Source.Attach.Status == "detached" ||
		snapshot.KeepsConsumerNetworkAfter(removed) {
		return nil, nil
	}
	baseline := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(snapshot.Runtime.Runtime.CurrentArtifact, baseline) != nil {
		return nil, errs.New(errs.KindStateConflict, "Detach consumer's captured runtime artifact is invalid")
	}
	target := snapshot.Source.Attach
	return executionplan.PrepareAttachConsumerTransition(ctx, baseline, target.EnvironmentID,
		target.BackingEnvironmentID, target.ServiceID, target.BackingNetworkID, artifactID)
}
