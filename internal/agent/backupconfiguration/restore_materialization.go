package backupconfiguration

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/backupconfigmaterialization"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ConfigFileMaterializer interface {
	RunConfigRestoreFile(context.Context, string, entrymaterialization.Header, io.ReadCloser, bool) error
}

// Materialize executes only after the native canonical-publication receipt.
// Every output is prepared before writes; a final independent read pass checks
// all files and removals before the worker may report its aggregate proof.
func (artifact *RestoreArtifact) Materialize(ctx context.Context, taskID, stepID, volumeRoot string,
	materializer ConfigFileMaterializer,
) (*agentpb.BackupConfigMaterializationVerified, error) {
	if ctx == nil || artifact == nil || artifact.validated == nil || materializer == nil ||
		artifact.authority.GetFiles().GetVolumeRoot() != volumeRoot {
		return nil, invalidRestoreArtifact()
	}
	values, err := artifact.validated.MaterializationValues(ctx)
	if err != nil {
		return nil, err
	}
	plan, err := backupconfigmaterialization.NewPlan(ctx, artifact.authority, values.Entries())
	if err != nil {
		return nil, err
	}
	headers, err := plan.PrepareHeaders(ctx, taskID, stepID, values.ReadValue)
	if err != nil {
		return nil, err
	}
	for _, verifyOnly := range []bool{false, true} {
		for index, expected := range headers {
			content, err := plan.Render(ctx, index, values.ReadValue)
			if err != nil {
				clear(content)
				return nil, err
			}
			header, err := plan.Header(taskID, stepID, index, content)
			if err != nil || header != expected {
				clear(content)
				return nil, invalidRestoreArtifact()
			}
			if err := materializer.RunConfigRestoreFile(ctx, plan.VolumeDir(), header, materialization.OwnContent(content), verifyOnly); err != nil {
				return nil, err
			}
		}
	}
	return plan.Proof(headers)
}
