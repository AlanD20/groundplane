package controllersoftware

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store store }

func New(backend store) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "Controller software execution storage is required")
	}
	return &Repository{store: backend}, nil
}

func (repository *Repository) Load(
	ctx context.Context,
	item workexecution.Item,
	revision int64,
) (keyvalue.Versioned[Capture], error) {
	if item.Validate() != nil || item.Operation != workexecution.ControllerSoftware || revision < 0 {
		return keyvalue.Versioned[Capture]{}, errs.New(errs.KindValidationFailed, "Controller software work is invalid")
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{InputKey(item.ID)}, Revision: revision},
	)
	if err != nil {
		return keyvalue.Versioned[Capture]{}, err
	}
	if read == nil {
		return keyvalue.Versioned[Capture]{}, errs.New(
			errs.KindStorageUnavailable,
			"Controller software input read is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 ||
		read.Values[0] == nil {
		return keyvalue.Versioned[Capture]{}, errs.New(
			errs.KindStateConflict,
			"Controller software captured input is missing",
		)
	}
	capture, err := decodeCapture(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[Capture]{}, err
	}
	if err := capture.Match(item); err != nil {
		return keyvalue.Versioned[Capture]{}, err
	}
	return keyvalue.Versioned[Capture]{
		Record:       capture,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

func decodeCapture(raw []byte) (Capture, error) {
	value, err := recordcodec.Decode[Capture](raw, "controller_software_input")
	if err != nil {
		return Capture{}, err
	}
	encoded, err := EncodeCapture(value)
	clear(encoded)
	if err != nil {
		return Capture{}, errs.New(errs.KindInternal, "Controller software capture is corrupt")
	}
	return value, nil
}

// Current is a small independent operator projection. Retiring diagnostic Task
// history never removes the last accepted update's release identity or outcome.
type Current struct {
	Capture Capture               `json:"capture"`
	Started bool                  `json:"started"`
	Outcome workexecution.Outcome `json:"outcome,omitempty"`
	Settled bool                  `json:"settled"`
}

func EncodeCurrent(current Current) ([]byte, error) {
	encoded, err := EncodeCapture(current.Capture)
	clear(encoded)
	if err != nil {
		return nil, err
	}
	if current.Settled && current.Outcome == "" ||
		current.Outcome != "" && current.Outcome != workexecution.Succeeded &&
			current.Outcome != workexecution.Failed && current.Outcome != workexecution.Cancelled && current.Outcome != workexecution.Superseded {
		return nil, errs.New(errs.KindValidationFailed, "Controller software current outcome is invalid")
	}
	return recordcodec.Encode("controller_software_current", current)
}

func (repository *Repository) Current(ctx context.Context, revision int64) (keyvalue.Versioned[Current], bool, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{LatestKey}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[Current]{}, false, err
	}
	if read == nil {
		return keyvalue.Versioned[Current]{}, false, errs.New(
			errs.KindStorageUnavailable,
			"Controller software current read is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[Current]{}, false, errs.New(
			errs.KindStateConflict,
			"Controller software current snapshot is unavailable",
		)
	}
	if read.Values[0] == nil {
		return keyvalue.Versioned[Current]{ReadRevision: read.ReadRevision}, false, nil
	}
	value, err := recordcodec.Decode[Current](read.Values[0].Value, "controller_software_current")
	if err != nil {
		return keyvalue.Versioned[Current]{}, false, err
	}
	raw, err := EncodeCurrent(value)
	clear(raw)
	if err != nil {
		return keyvalue.Versioned[Current]{}, false, errs.New(
			errs.KindInternal,
			"Controller software current projection is corrupt",
		)
	}
	return keyvalue.Versioned[Current]{
		Record:       value,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, true, nil
}
