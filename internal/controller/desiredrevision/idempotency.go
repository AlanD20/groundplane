package desiredrevision

import (
	"context"
	"crypto/sha256"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"sort"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const blueprintRoute = "/environments/{id}/blueprint"

type Evidence struct {
	candidate requestidempotency.ProtectedEvidence
	Durable   idempotencyrecord.ProtectedIntentRecord
}

type IntentAddress struct {
	Method string
	Route  string
	Scope  requestidempotency.Scope
	Path   []requestidempotency.PathBinding
}

type Idempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*Idempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint idempotency is not configured")
	}
	return &Idempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *Idempotency) MatchesStaged(
	ctx context.Context,
	evidence Evidence,
	existing idempotencyrecord.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func (service *Idempotency) Prepare(
	ctx context.Context,
	address IntentAddress,
	bundle core.BlueprintBundle,
) (Evidence, error) {
	manifest, err := IntentManifest(bundle)
	if err != nil {
		return Evidence{}, err
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: address.Method,
		Route:  address.Route,
		Scope:  address.Scope,
		Path:   append([]requestidempotency.PathBinding(nil), address.Path...),
		Query:  requestidempotency.Object(),
		Body:   requestidempotency.BlueprintBody(manifest),
	})
	if err != nil {
		return Evidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return Evidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return Evidence{}, err
	}
	return Evidence{candidate: candidate, Durable: durable}, nil
}

func (service *Idempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence Evidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *Idempotency) ResolveKnown(
	ctx context.Context,
	evidence Evidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *Idempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence Evidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func IntentManifest(bundle core.BlueprintBundle) (requestidempotency.BlueprintManifestV1, error) {
	if err := bundle.Validate(); err != nil {
		return requestidempotency.BlueprintManifestV1{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint bundle is invalid",
		)
	}
	keys := make([]string, 0, len(bundle.Interpolation))
	for key := range bundle.Interpolation {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	manifest := requestidempotency.BlueprintManifestV1{
		FormatVersion:  1,
		RootPath:       bundle.RootPath,
		ComposeSources: append([]string(nil), bundle.ComposeSources...),
		Interpolation:  make([]requestidempotency.Interpolation, len(keys)),
		Files:          make([]requestidempotency.BlueprintFile, len(bundle.Files)),
	}
	for index, key := range keys {
		manifest.Interpolation[index] = requestidempotency.Interpolation{Name: key, Value: bundle.Interpolation[key]}
	}
	for index, file := range bundle.Files {
		manifest.Files[index] = requestidempotency.BlueprintFile{
			Path: file.Path, Part: "file-" + leftPadPart(index+1),
			Size: uint64(len(file.Content)), SHA256: sha256.Sum256(file.Content),
		}
	}
	return manifest, nil
}

func leftPadPart(value int) string {
	digits := []byte{'0', '0', '0', '0', '0', '0'}
	for index := len(digits) - 1; index >= 0 && value > 0; index-- {
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits)
}
