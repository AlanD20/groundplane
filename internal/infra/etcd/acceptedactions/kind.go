package acceptedactions

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// KindRecord is immutable routing metadata, not the encrypted operator request.
// It survives request retirement so crash-left inert drafts can be collected.
type KindRecord struct {
	TaskID    string              `json:"task_id"`
	Kind      string              `json:"kind"`
	Reference workexecution.Input `json:"reference"`
}

type KindSnapshot struct {
	Record   KindRecord
	Revision int64
}

func KindKey(taskID string) string { return "/v1/execution/accepted-kinds/" + taskID }

func (record KindRecord) validate() error {
	if ids.Validate(ids.KindTask, record.TaskID) != nil || record.Reference.Validate() != nil ||
		len(record.Kind) == 0 || len(record.Kind) > 100 || strings.ContainsAny(record.Kind, " \n\r\t/\\") {
		return errs.New(errs.KindValidationFailed, "accepted action routing metadata is invalid")
	}
	return nil
}

func prepareKind(record Record) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	kind := KindRecord{TaskID: record.TaskID, Kind: record.Kind, Reference: record.Reference}
	if err := kind.validate(); err != nil {
		return nil, nil, err
	}
	value, err := recordcodec.Encode("accepted-action-kind", kind)
	if err != nil {
		return nil, nil, err
	}
	return []keyvalue.Condition{{Key: KindKey(record.TaskID)}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: KindKey(record.TaskID), Value: value}}, nil
}

func (repository *Repository) ReadKind(
	ctx context.Context,
	taskID string,
	reference workexecution.Input,
	revision int64,
) (KindSnapshot, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || reference.Validate() != nil || revision <= 0 {
		return KindSnapshot{}, errs.New(
			errs.KindValidationFailed,
			"accepted action kind read requires a fixed Task snapshot",
		)
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{KindKey(taskID)}, Revision: revision},
	)
	if err != nil {
		return KindSnapshot{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return KindSnapshot{}, errs.New(errs.KindStateConflict, "accepted action routing metadata is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	kind, err := recordcodec.Decode[KindRecord](value.Value, "accepted-action-kind")
	if err != nil || kind.validate() != nil || kind.TaskID != taskID || kind.Reference != reference ||
		value.Key != KindKey(taskID) || value.ModRevision <= 0 || value.ModRevision > revision {
		return KindSnapshot{}, errs.New(errs.KindInternal, "accepted action routing metadata differs from its Task")
	}
	return KindSnapshot{Record: kind, Revision: value.ModRevision}, nil
}
