package blueprint

import (
	"crypto/sha256"
	"encoding/hex"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// An authored Apply accepts only a visible coordinator Task. Agent effects are
// prepared later as private children from this exact revision and its unit
// claims; no effective runtime artifact is asserted at head publication.
func authoredApplyParent(
	claim blueprints.EnvironmentBlueprintStageClaim,
	owner taskjournal.TaskOwner,
	target, idempotencyKey string,
	desired projectionrecord.EnvironmentDesiredInput,
	allocateNamed func(ids.Kind, string) string,
) (etcd.TaskRecord, error) {
	if allocateNamed == nil || claim.TaskID != claim.RevisionID ||
		claim.EnvironmentID != owner.EnvironmentID || target != owner.EnvironmentID ||
		desired.EnvironmentID != claim.EnvironmentID ||
		desired.RevisionID != claim.RevisionID ||
		desired.RenderGeneration != claim.RenderGeneration ||
		desired.RenderGeneration == 0 || desired.RenderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Blueprint parent input identity is invalid")
	}
	encoded, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(desired)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	defer clear(encoded)
	digest := sha256.Sum256(encoded)
	parent := etcd.TaskRecord{
		ID: claim.TaskID, OperationID: allocateNamed(ids.KindOperation, "operation"),
		IdempotencyKey: idempotencyKey, Owner: owner, Actor: taskjournal.TaskActorOperator,
		Executor:         taskjournal.TaskExecutorBlueprint,
		PlanID:           allocateNamed(ids.KindPlan, "blueprint-parent"),
		PlanHash:         hex.EncodeToString(digest[:]),
		RenderGeneration: int32(claim.RenderGeneration),
		Type:             taskjournal.TaskUpdate, Target: target,
		Params:         map[string]string{blueprints.EnvironmentDesiredRevisionParam: claim.RevisionID},
		TimeoutSeconds: desiredrevision.TaskTimeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
	}
	if err := etcd.ValidateTaskRecord(parent); err != nil {
		return etcd.TaskRecord{}, err
	}
	return parent, nil
}
