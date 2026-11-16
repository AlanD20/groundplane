package operations

import (
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func entryTaskResponse(taskID string) (idempotencyrecord.IdempotencyResponse, error) {
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}, nil
}

func entryDesiredRemovalOutcome(
	response idempotencyrecord.IdempotencyResponse,
) (entrycontroller.RemovalOutcome, error) {
	if response.Status != http.StatusAccepted {
		return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal response status is invalid")
	}
	accepted := apiTypes.TaskAccepted{}
	if json.Unmarshal(response.Body, &accepted) != nil || ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal response is invalid")
	}
	return entrycontroller.RemovalOutcome{TaskID: accepted.TaskID}, nil
}
