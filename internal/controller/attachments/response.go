package attachments

import (
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func newAttachMutationResponse(
	locator idempotencyrecord.IdempotencyLocator,
	intent idempotencyrecord.ProtectedIntentRecord,
	task etcd.TaskRecord,
	replayTarget *idempotencyrecord.IdempotencyReplayTarget,
) (idempotencyrecord.IdempotencyResponse, idempotencyrecord.IdempotencyMarker, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, errs.Wrap(
			errs.KindInternal,
			err,
		)
	}
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: replayTarget, Intent: intent, Response: response,
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	return response, marker, nil
}
