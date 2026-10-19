package blueprint

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// authoredApplyParent accepts a complete captured membership count. Inputs
// dependent on hook outputs may still wait, but membership is never invented
// after acceptance and the public Task is not an Agent execution payload.
func authoredApplyParent(
	claim blueprints.EnvironmentBlueprintStageClaim,
	scope workexecution.Scope,
	targetName string,
	desired projectionrecord.EnvironmentDesiredInput,
	expectedItems uint32,
	timeout time.Duration,
) (workexecution.Task, error) {
	if blueprints.ValidateEnvironmentBlueprintStageClaim(claim) != nil || scope.Validate() != nil ||
		claim.TaskID != claim.RevisionID || claim.EnvironmentID != scope.EnvironmentID ||
		desired.EnvironmentID != claim.EnvironmentID || desired.RevisionID != claim.RevisionID ||
		desired.RenderGeneration != claim.RenderGeneration || desired.RenderGeneration == 0 ||
		expectedItems > workexecution.MaximumTaskItems || timeout <= 0 {
		return workexecution.Task{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Apply identity, work count or deadline is invalid",
		)
	}
	encoded, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(desired)
	if err != nil {
		return workexecution.Task{}, err
	}
	defer clear(encoded)
	digest := sha256.Sum256(encoded)
	parent := workexecution.Task{
		ID: claim.TaskID, Actor: workexecution.OperatorActor, Scope: scope,
		Action: workexecution.ActionApply, TargetID: scope.EnvironmentID, TargetName: targetName,
		Intent: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, claim.CreatedAt, claim.TaskID, "blueprint-intent"),
			SHA256: hex.EncodeToString(digest[:]),
		},
		ExpectedItems: expectedItems, CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
		Deadline: claim.CreatedAt.Add(timeout),
	}
	if err := parent.Validate(); err != nil {
		return workexecution.Task{}, err
	}
	return parent, nil
}
