package operations

import (
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func entryDesiredResponse(
	record *entryrecord.Record,
	taskID string,
	status int,
) (etcd.IdempotencyResponse, error) {
	var value any = apiTypes.TaskAccepted{TaskID: taskID}
	if record != nil {
		value = entryCreationResponse(record.Entry)
	}
	body, err := json.Marshal(value)
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return etcd.IdempotencyResponse{Status: status, ContentKind: "application/json", Body: body}, nil
}

func entryDesiredRemovalOutcome(response etcd.IdempotencyResponse) (entrycontroller.RemovalOutcome, error) {
	if response.Status != http.StatusAccepted {
		return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal response status is invalid")
	}
	accepted := apiTypes.TaskAccepted{}
	if json.Unmarshal(response.Body, &accepted) != nil || ids.Validate(ids.KindTask, accepted.TaskID) != nil {
		return entrycontroller.RemovalOutcome{}, errs.New(errs.KindInternal, "Entry removal response is invalid")
	}
	return entrycontroller.RemovalOutcome{TaskID: accepted.TaskID}, nil
}
