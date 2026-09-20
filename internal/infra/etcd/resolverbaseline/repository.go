package resolverbaseline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	hostResolverBaselineKey          = "/v1/records/platform/host-resolution/baseline"
	maximumHostResolverBaselineBytes = 64 * 1024
)

type Record struct {
	Generation uint64    `json:"generation"`
	Content    []byte    `json:"content"`
	SHA256     string    `json:"sha256"`
	CapturedAt time.Time `json:"captured_at"`
}

type baselineStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type Repository struct {
	store baselineStore
}

func New(store etcdstore.Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "host resolver baseline store is required")
	}
	return &Repository{store: store}, nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "hierarchy context is required")
	}
	return ctx.Err()
}

func (repository *Repository) GetHostResolverBaseline(
	ctx context.Context,
) (etcdstore.Versioned[Record], bool, error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, false, err
	}
	result, err := repository.store.Get(ctx, hostResolverBaselineKey)
	if err != nil {
		return etcdstore.Versioned[Record]{}, false, err
	}
	if result == nil || result.Entry == nil {
		readRevision := int64(0)
		if result != nil {
			readRevision = result.ReadRevision
		}
		return etcdstore.Versioned[Record]{ReadRevision: readRevision}, false, nil
	}
	record, err := decodeHostResolverBaseline(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[Record]{}, false, err
	}
	return etcdstore.Versioned[Record]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *Repository) EnsureHostResolverBaseline(
	ctx context.Context,
	content []byte,
	capturedAt time.Time,
) (etcdstore.Versioned[Record], error) {
	current, found, err := repository.GetHostResolverBaseline(ctx)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if found {
		return current, nil
	}
	digest := sha256.Sum256(content)
	record := Record{
		Generation: 1, Content: append([]byte(nil), content...),
		SHA256: hex.EncodeToString(digest[:]), CapturedAt: capturedAt.UTC(),
	}
	if err := validateHostResolverBaseline(record); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	encoded, err := recordcodec.Encode("host_resolver_baseline", record)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	defer clear(encoded)
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: hostResolverBaselineKey}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: hostResolverBaselineKey, Value: encoded}},
	)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if result.Succeeded {
		return etcdstore.Versioned[Record]{
			Record: cloneHostResolverBaseline(record), Revision: result.Revision, ReadRevision: result.Revision,
		}, nil
	}
	current, found, err = repository.GetHostResolverBaseline(ctx)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if !found {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindStateConflict,
			"host resolver baseline creation conflicted without a durable winner",
		)
	}
	return current, nil
}

func validateHostResolverBaseline(record Record) error {
	if record.Generation != 1 || len(record.Content) == 0 ||
		len(record.Content) > maximumHostResolverBaselineBytes ||
		!bytes.HasSuffix(record.Content, []byte("\n")) || !recordcodec.ValidSHA256(record.SHA256) ||
		!recordcodec.IsCanonicalUTC(record.CapturedAt) {
		return errs.New(errs.KindValidationFailed, "host resolver baseline is invalid")
	}
	digest := sha256.Sum256(record.Content)
	if hex.EncodeToString(digest[:]) != record.SHA256 {
		return errs.New(errs.KindValidationFailed, "host resolver baseline digest is invalid")
	}
	return nil
}

func decodeHostResolverBaseline(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "host_resolver_baseline")
	if err != nil || validateHostResolverBaseline(record) != nil {
		return Record{}, errs.New(errs.KindInternal, "host resolver baseline is corrupt")
	}
	return cloneHostResolverBaseline(record), nil
}

func cloneHostResolverBaseline(record Record) Record {
	record.Content = append([]byte(nil), record.Content...)
	return record
}
