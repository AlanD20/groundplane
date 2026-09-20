package attachments

import (
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func newAttachMutationResponse(
	locator etcd.IdempotencyLocator,
	intent etcd.ProtectedIntentRecord,
	task etcd.TaskRecord,
	replayTarget *etcd.IdempotencyReplayTarget,
) (etcd.IdempotencyResponse, etcd.IdempotencyMarker, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return etcd.IdempotencyResponse{}, etcd.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, ReplayTarget: replayTarget, Intent: intent, Response: response,
		TaskID: task.ID, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	return response, marker, nil
}
