package etcd

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

type HostResolverBaselineRecord struct {
	Generation uint64    `json:"generation"`
	Content    []byte    `json:"content"`
	SHA256     string    `json:"sha256"`
	CapturedAt time.Time `json:"captured_at"`
}

func (repository *ComponentRepository) GetHostResolverBaseline(
	ctx context.Context,
) (Versioned[HostResolverBaselineRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[HostResolverBaselineRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, hostResolverBaselineKey)
	if err != nil {
		return Versioned[HostResolverBaselineRecord]{}, false, err
	}
	if result == nil || result.Entry == nil {
		readRevision := int64(0)
		if result != nil {
			readRevision = result.ReadRevision
		}
		return Versioned[HostResolverBaselineRecord]{ReadRevision: readRevision}, false, nil
	}
	record, err := decodeHostResolverBaseline(result.Entry.Value)
	if err != nil {
		return Versioned[HostResolverBaselineRecord]{}, false, err
	}
	return Versioned[HostResolverBaselineRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func (repository *ComponentRepository) EnsureHostResolverBaseline(
	ctx context.Context,
	content []byte,
	capturedAt time.Time,
) (Versioned[HostResolverBaselineRecord], error) {
	current, found, err := repository.GetHostResolverBaseline(ctx)
	if err != nil {
		return Versioned[HostResolverBaselineRecord]{}, err
	}
	if found {
		return current, nil
	}
	digest := sha256.Sum256(content)
	record := HostResolverBaselineRecord{
		Generation: 1, Content: append([]byte(nil), content...),
		SHA256: hex.EncodeToString(digest[:]), CapturedAt: capturedAt.UTC(),
	}
	if err := validateHostResolverBaseline(record); err != nil {
		return Versioned[HostResolverBaselineRecord]{}, err
	}
	encoded, err := recordcodec.Encode("host_resolver_baseline", record)
	if err != nil {
		return Versioned[HostResolverBaselineRecord]{}, err
	}
	defer clear(encoded)
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: hostResolverBaselineKey}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: hostResolverBaselineKey, Value: encoded}},
	)
	if err != nil {
		return Versioned[HostResolverBaselineRecord]{}, err
	}
	if result.Succeeded {
		return Versioned[HostResolverBaselineRecord]{
			Record: cloneHostResolverBaseline(record), Revision: result.Revision, ReadRevision: result.Revision,
		}, nil
	}
	current, found, err = repository.GetHostResolverBaseline(ctx)
	if err != nil {
		return Versioned[HostResolverBaselineRecord]{}, err
	}
	if !found {
		return Versioned[HostResolverBaselineRecord]{}, errs.New(
			errs.KindStateConflict,
			"host resolver baseline creation conflicted without a durable winner",
		)
	}
	return current, nil
}

func validateHostResolverBaseline(record HostResolverBaselineRecord) error {
	if record.Generation != 1 || len(record.Content) == 0 ||
		len(record.Content) > maximumHostResolverBaselineBytes ||
		!bytes.HasSuffix(record.Content, []byte("\n")) || !recordcodec.ValidSHA256(record.SHA256) ||
		!validMarkerTime(record.CapturedAt) {
		return errs.New(errs.KindValidationFailed, "host resolver baseline is invalid")
	}
	digest := sha256.Sum256(record.Content)
	if hex.EncodeToString(digest[:]) != record.SHA256 {
		return errs.New(errs.KindValidationFailed, "host resolver baseline digest is invalid")
	}
	return nil
}

func decodeHostResolverBaseline(value []byte) (HostResolverBaselineRecord, error) {
	record, err := recordcodec.Decode[HostResolverBaselineRecord](value, "host_resolver_baseline")
	if err != nil || validateHostResolverBaseline(record) != nil {
		return HostResolverBaselineRecord{}, errs.New(errs.KindInternal, "host resolver baseline is corrupt")
	}
	return cloneHostResolverBaseline(record), nil
}

func cloneHostResolverBaseline(record HostResolverBaselineRecord) HostResolverBaselineRecord {
	record.Content = append([]byte(nil), record.Content...)
	return record
}
