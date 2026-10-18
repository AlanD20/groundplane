package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func databaseRestoreCleanup(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, staging *Staging,
	artifact *databaseRestoreArtifact, publisher *checkpointdelivery.BackupSource,
) error {
	if artifact != nil {
		if err := staging.Cleanup(ctx, artifact.stage); err != nil {
			return err
		}
	} else {
		absent, err := staging.SourceAbsent(assignment, identity, ordinal)
		if err != nil {
			return err
		}
		if !absent {
			return invalidDatabaseStaging()
		}
	}
	step := assignment.Input.Steps[ordinal]
	return publisher.SourceCleanupCompleted(ctx, &agentpb.BackupSourceCleanupCompleted{
		PointId: step.GetRestore().PointId, Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence),
	})
}
