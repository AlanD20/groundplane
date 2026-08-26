package environment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const environmentPoolChangeTestID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"

func TestEnvironmentPoolEditReplaysBeforeLookupAndPropagatesMismatch(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status: 200, ContentKind: "application/json",
		Body: []byte(`{"id":"` + environmentPoolChangeTestID + `","network_pool":"10.40.0.0/15"}`),
	}
	for _, test := range []struct {
		name        string
		idempotency *fakeEnvironmentPoolChangeIdempotency
		want        etcd.IdempotencyResponse
		wantKind    errs.Kind
	}{
		{
			name: "exact replay",
			idempotency: &fakeEnvironmentPoolChangeIdempotency{
				evidence: environmentPoolChangeEvidence(), existing: true,
				existingResolution: idempotentintent.Resolution{
					Kind: idempotentintent.ResolutionReplay, Response: want,
				},
			},
			want: want,
		},
		{
			name: "same key different network pool",
			idempotency: &fakeEnvironmentPoolChangeIdempotency{
				evidence: environmentPoolChangeEvidence(),
				existingErr: errs.New(
					errs.KindIdempotencyMismatch,
					"idempotency key was used for a different request",
				),
			},
			wantKind: errs.KindIdempotencyMismatch,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repository := &fakeEnvironmentPoolChangeRepository{}
			service, err := NewChangeService("10.0.0.0/8", repository, repository, test.idempotency)
			if err != nil {
				t.Fatalf("NewChangeService() error = %v", err)
			}
			got, err := service.EditEnvironment(
				context.Background(),
				environmentPoolChangeTestID,
				EditEnvironmentInput{NetworkPool: "10.40.0.0/15"},
				"environment-edit-replay-0001",
			)
			if test.wantKind != 0 {
				if !errors.Is(err, errs.New(test.wantKind, "")) {
					t.Fatalf("EditEnvironment() error = %v", err)
				}
			} else if err != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("EditEnvironment() = %#v, %v", got, err)
			}
			if repository.getCalls != 0 || repository.mutateCalls != 0 {
				t.Fatalf("repository calls = get %d, mutate %d", repository.getCalls, repository.mutateCalls)
			}
		})
	}
}

func TestEnvironmentPoolEditResolvesUnknownTransactionOutcomeToExactReplay(t *testing.T) {
	t.Parallel()
	want := etcd.IdempotencyResponse{
		Status: 200, ContentKind: "application/json",
		Body: []byte(`{"id":"` + environmentPoolChangeTestID + `","network_pool":"10.40.0.0/15"}`),
	}
	repository := &fakeEnvironmentPoolChangeRepository{
		current:    environmentPoolChangeCurrent(),
		replaceErr: errs.New(errs.KindStorageUnavailable, "Environment pool transaction outcome is unknown"),
	}
	idempotency := &fakeEnvironmentPoolChangeIdempotency{
		evidence: environmentPoolChangeEvidence(),
		unknownResolution: idempotentintent.Resolution{
			Kind: idempotentintent.ResolutionReplay, Response: want,
		},
	}
	service, err := NewChangeService("10.0.0.0/8", repository, repository, idempotency)
	if err != nil {
		t.Fatalf("NewChangeService() error = %v", err)
	}
	got, err := service.EditEnvironment(
		context.Background(),
		environmentPoolChangeTestID,
		EditEnvironmentInput{NetworkPool: "10.40.0.0/15"},
		"environment-edit-unknown-0001",
	)
	if err != nil || !reflect.DeepEqual(got, want) || repository.getCalls != 1 ||
		repository.mutateCalls != 1 || idempotency.unknownCalls != 1 {
		t.Fatalf(
			"EditEnvironment(unknown) = %#v, %v; calls get/mutate/unknown %d/%d/%d",
			got,
			err,
			repository.getCalls,
			repository.mutateCalls,
			idempotency.unknownCalls,
		)
	}
	if repository.replacement.NetworkPool != "10.40.0.0/15" ||
		repository.root != netip.MustParsePrefix("10.0.0.0/8") ||
		repository.marker.Locator.Method != "PATCH" || repository.marker.Locator.Route != environmentEditRoute {
		t.Fatalf("unknown publication = %#v / %s / %#v", repository.replacement, repository.root, repository.marker)
	}
}

type fakeEnvironmentPoolChangeRepository struct {
	current     etcd.Versioned[etcd.EnvironmentRecord]
	replacement etcd.EnvironmentRecord
	marker      etcd.IdempotencyMarker
	root        netip.Prefix
	replaceErr  error
	getCalls    int
	mutateCalls int
}

func (repository *fakeEnvironmentPoolChangeRepository) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	repository.getCalls++
	return repository.current, nil
}

func (repository *fakeEnvironmentPoolChangeRepository) ListZoneSubnetReservationsAtRevision(
	context.Context,
	string,
	int64,
) ([]string, error) {
	return []string{}, nil
}

func (repository *fakeEnvironmentPoolChangeRepository) MutateEnvironmentIdempotent(
	context.Context,
	etcd.Versioned[etcd.EnvironmentRecord],
	etcd.EnvironmentRecord,
	etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.mutateCalls++
	return etcd.IdempotencyTransactionResult{}, nil
}

func (repository *fakeEnvironmentPoolChangeRepository) ReplaceEnvironmentPoolIdempotent(
	_ context.Context,
	root netip.Prefix,
	_ etcd.Versioned[etcd.EnvironmentRecord],
	replacement etcd.EnvironmentRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	repository.mutateCalls++
	repository.root = root
	repository.replacement = replacement
	repository.marker = marker
	repository.marker.Intent.Ciphertext = append([]byte(nil), marker.Intent.Ciphertext...)
	repository.marker.Response.Body = append([]byte(nil), marker.Response.Body...)
	return etcd.IdempotencyTransactionResult{}, repository.replaceErr
}

type fakeEnvironmentPoolChangeIdempotency struct {
	evidence           environmentChangeEvidence
	existingResolution idempotentintent.Resolution
	unknownResolution  idempotentintent.Resolution
	existing           bool
	existingErr        error
	unknownCalls       int
}

func (idempotency *fakeEnvironmentPoolChangeIdempotency) PrepareEdit(
	context.Context,
	string,
	EditEnvironmentInput,
) (environmentChangeEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeEnvironmentPoolChangeIdempotency) PrepareRename(
	context.Context,
	string,
	RenameEnvironmentInput,
) (environmentChangeEvidence, error) {
	return idempotency.evidence, nil
}

func (idempotency *fakeEnvironmentPoolChangeIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	environmentChangeEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.existingResolution, idempotency.existing, idempotency.existingErr
}

func (idempotency *fakeEnvironmentPoolChangeIdempotency) ResolveKnown(
	context.Context,
	environmentChangeEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied}, nil
}

func (idempotency *fakeEnvironmentPoolChangeIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	environmentChangeEvidence,
	error,
) (idempotentintent.Resolution, error) {
	idempotency.unknownCalls++
	return idempotency.unknownResolution, nil
}

func environmentPoolChangeCurrent() etcd.Versioned[etcd.EnvironmentRecord] {
	return etcd.Versioned[etcd.EnvironmentRecord]{
		Record: etcd.EnvironmentRecord{
			ID:                environmentPoolChangeTestID,
			ProjectID:         "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Name:              "production",
			NetworkPool:       "10.40.0.0/16",
			VolumeDir:         "/var/lib/groundplane/vol/platform/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentPoolChangeTestID,
			ProvisioningState: etcd.EnvironmentProvisioningReady,
			CreatedAt:         time.Date(2026, 8, 25, 8, 0, 0, 0, time.UTC),
		},
		Revision: 10, ReadRevision: 10,
	}
}

func environmentPoolChangeEvidence() environmentChangeEvidence {
	ciphertext := []byte("protected-environment-pool-edit-intent")
	digest := sha256.Sum256(ciphertext)
	return environmentChangeEvidence{durable: etcd.ProtectedIntentRecord{
		EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
		CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
	}}
}
