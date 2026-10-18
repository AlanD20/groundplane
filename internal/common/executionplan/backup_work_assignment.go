package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// OpenBackupWorkAssignment binds the complete captured input and source cursors
// to an independently authenticated executor. Acceptance is not a start grant.
func OpenBackupWorkAssignment(assignment *agentpb.BackupWorkAssignment, authenticated workexecution.Identity) (
	*agentpb.BackupWorkAssignment, workexecution.Attempt, error,
) {
	if assignment == nil || proto.Size(assignment) > MaximumPlanBytes || RejectUnknown(assignment) != nil {
		return nil, workexecution.Attempt{}, errs.New(errs.KindValidationFailed, "Backup work assignment is invalid")
	}
	attempt, err := workexecution.DecodeAgentAttempt(assignment.Attempt, authenticated)
	if err != nil {
		return nil, workexecution.Attempt{}, err
	}
	input, err := OpenBackupWorkInput(attempt, assignment.Input)
	if err != nil {
		return nil, workexecution.Attempt{}, err
	}
	if err := ValidateBackupSourceResumes(assignment.Sources, input.Steps); err != nil {
		return nil, workexecution.Attempt{}, err
	}
	for ordinal, source := range assignment.Sources {
		if err := validateDatabaseWorkProgress(source.GetRestore(), input.Steps[ordinal], input.Services); err != nil {
			return nil, workexecution.Attempt{}, err
		}
	}
	owned := proto.CloneOf(assignment)
	owned.Input = input
	return owned, attempt, nil
}
