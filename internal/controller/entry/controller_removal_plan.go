package entry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const controllerRemovalTimeoutSeconds = int64(30)

func prepareControllerRemovalPlan(request RemovalPlanRequest) (RemovalTaskPlan, error) {
	if ids.Validate(ids.KindTask, request.TaskID) != nil || ids.Validate(ids.KindPlan, request.PlanID) != nil ||
		ids.Validate(ids.KindEnvEntry, request.EntryID) != nil ||
		ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil || request.EntryRevision <= 0 ||
		request.ProjectionRevision != 0 || request.CreatedAt.IsZero() {
		return RemovalTaskPlan{}, errs.New(errs.KindInternal, "controller Entry removal intent is invalid")
	}
	planHash, err := controllerRemovalPlanHash(request)
	if err != nil {
		return RemovalTaskPlan{}, err
	}
	return RemovalTaskPlan{
		Executor: RemovalExecutorController, PlanHash: planHash, RenderGeneration: 1,
		EnvironmentID: request.EnvironmentID, Identity: request.Identity,
		Steps: []RemovalStep{{ID: ids.New(ids.KindStep)}}, TimeoutSeconds: controllerRemovalTimeoutSeconds,
	}, nil
}

func controllerRemovalPlanHash(request RemovalPlanRequest) (string, error) {
	value, err := json.Marshal(struct {
		Version                   int    `json:"version"`
		Type                      string `json:"type"`
		EntryID                   string `json:"entry_id"`
		EnvironmentID             string `json:"environment_id"`
		EntryRevision             int64  `json:"entry_revision"`
		CurrentProjectionRevision int64  `json:"current_projection_revision"`
	}{
		Version: 1, Type: "remove", EntryID: request.EntryID,
		EnvironmentID: request.EnvironmentID, EntryRevision: request.EntryRevision,
		CurrentProjectionRevision: request.ProjectionRevision,
	})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}
