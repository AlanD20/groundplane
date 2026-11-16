// Package acceptedaction provides the single typed encryption boundary for
// operator intents accepted before their execution inputs can be compiled.
package acceptedaction

import (
	"bytes"
	"context"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/acceptedactions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumPlaintextBytes = acceptedactions.MaximumPlaintextBytes

type Payloads struct {
	repository *acceptedactions.Repository
	protector  *secretvalue.Protector
}

func NewPayloads(repository *acceptedactions.Repository, protector *secretvalue.Protector) (*Payloads, error) {
	if repository == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "accepted action payload dependencies are required")
	}
	return &Payloads{repository: repository, protector: protector}, nil
}

// Seal protects the complete typed request, never a hash pretending to be the
// request. Join the returned change to Task/dependency/idempotency acceptance.
func Seal[T any](
	ctx context.Context,
	payloads *Payloads,
	taskID, kind string,
	input T,
) (workexecution.Input, workqueue.Change, error) {
	if payloads == nil {
		return workexecution.Input{}, workqueue.Change{}, errs.New(
			errs.KindInternal,
			"accepted action payloads are required",
		)
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(input); err != nil {
		return workexecution.Input{}, workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"accepted action input cannot be encoded",
		)
	}
	raw := encoded.Bytes()
	defer clear(raw)
	if len(raw) > maximumPlaintextBytes {
		return workexecution.Input{}, workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"accepted action input is too large",
		)
	}
	var ciphertext []byte
	defer func() { clear(ciphertext) }()
	parts := make(
		[]acceptedactions.Part,
		0,
		(len(raw)+acceptedactions.PartPlaintextBytes-1)/acceptedactions.PartPlaintextBytes,
	)
	for offset := 0; offset < len(raw); offset += acceptedactions.PartPlaintextBytes {
		plaintext := raw[offset:min(offset+acceptedactions.PartPlaintextBytes, len(raw))]
		envelope, err := payloads.protector.Seal(ctx, plaintext)
		if err != nil {
			return workexecution.Input{}, workqueue.Change{}, err
		}
		part := envelope.Ciphertext()
		parts = append(
			parts,
			acceptedactions.Part{
				Bytes:          uint32(len(part)),
				PlaintextBytes: uint32(len(plaintext)),
				SHA256:         envelope.Metadata().Digest.Value,
			},
		)
		ciphertext = append(ciphertext, part...)
		clear(part)
		envelope.Clear()
	}
	record, err := acceptedactions.NewRecord(taskID, kind, ids.New(ids.KindPlan), uint32(len(raw)), parts, ciphertext)
	if err != nil {
		return workexecution.Input{}, workqueue.Change{}, err
	}
	conditions, mutations, err := payloads.repository.Stage(ctx, record, ciphertext)
	return record.Reference, workqueue.Change{Conditions: conditions, Mutations: mutations}, err
}

// Open confines decrypted request use to its typed compiler callback. The
// immutable payload comparison joins the eventual execution capture promotion.
func Open[T any](ctx context.Context, payloads *Payloads, task workqueue.TaskSnapshot, kind string,
	revision int64, consume func(T) error,
) (workqueue.Change, error) {
	if payloads == nil || task.Task.AcceptedIntent == nil || consume == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "accepted action compiler input is missing")
	}
	snapshot, err := payloads.repository.Read(ctx, task.Task.ID, *task.Task.AcceptedIntent, kind, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(snapshot.Ciphertext)
	raw := make([]byte, 0, snapshot.Record.PlaintextBytes)
	defer func() { clear(raw) }()
	offset := 0
	for _, part := range snapshot.Record.Parts {
		envelope, err := secretvalue.Restore(
			secretvalue.Metadata{Version: secretvalue.EnvelopeVersion1, Cipher: secretvalue.CipherSuiteAgeX25519,
				Digest: secretvalue.Digest{
					Algorithm: secretvalue.DigestAlgorithmSHA256,
					Value:     part.SHA256,
				}},
			snapshot.Ciphertext[offset:offset+int(part.Bytes)],
		)
		if err != nil {
			return workqueue.Change{}, err
		}
		err = payloads.protector.Open(ctx, envelope, func(plaintext []byte) error {
			if uint32(len(plaintext)) != part.PlaintextBytes {
				return errs.New(
					errs.KindStateConflict,
					"accepted action plaintext part differs from its sealed manifest",
				)
			}
			raw = append(raw, plaintext...)
			return nil
		})
		envelope.Clear()
		if err != nil {
			return workqueue.Change{}, err
		}
		offset += int(part.Bytes)
	}
	if uint32(len(raw)) != snapshot.Record.PlaintextBytes || len(raw) > maximumPlaintextBytes {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "accepted action input exceeds its sealed limit")
	}
	var input T
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "accepted action typed input is invalid")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "accepted action input contains trailing data")
	}
	if err := consume(input); err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: acceptedactions.Key(task.Task.ID), ModRevision: snapshot.Revision}},
	}, nil
}
