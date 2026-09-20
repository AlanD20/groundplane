package operations

import (
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func entryDesiredResponse(
	record *entryrecord.Record,
	taskID string,
	status int,
) (idempotencyrecord.IdempotencyResponse, error) {
	var value any = apiTypes.TaskAccepted{TaskID: taskID}
	if record != nil {
		value = entryCreationResponse(record.Entry)
	}
	body, err := json.Marshal(value)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyResponse{Status: status, ContentKind: "application/json", Body: body}, nil
}

func entryDesiredRemovalOutcome(response idempotencyrecord.IdempotencyResponse) (entrycontroller.RemovalOutcome, error) {
	if response.Status != http.StatusAccepted {
		return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal response status is invalid")
	}
	accepted := apiTypes.TaskAccepted{}
	if json.Unmarshal(response.Body, &accepted) != nil || ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal response is invalid")
	}
	return entrycontroller.RemovalOutcome{TaskID: accepted.TaskID}, nil
}
