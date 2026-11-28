package releaseoperation

import (
	"context"
	"net/http"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadDeployServiceReceipt resolves the exact request in its captured Environment
// even when the Service is gone. A missing receipt supplies a comparison for
// callers that must atomically release their admission fence.
func (service *Service) ReadDeployServiceReceipt(ctx context.Context, environmentID, serviceID string,
	request release.ServiceDeployInput, key string,
) (*idempotency.IdempotencyResponse, keyvalue.Condition, error) {
	locator, protected, err := service.protectReleaseIntent(
		ctx, environmentID, http.MethodPost, serviceDeployRoute, serviceID, key, serviceDeployBody(request),
	)
	defer protected.Destroy()
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	receipt, exists, err := service.coordinator.ResolveOperationRootExisting(
		ctx,
		service.idempotency,
		locator,
		protected,
	)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if exists {
		if receipt.Kind != requestidempotency.ResolutionReplay {
			return nil, keyvalue.Condition{}, errs.New(errs.KindInternal, "release acceptance receipt is invalid")
		}
		return &receipt.Response, keyvalue.Condition{}, nil
	}
	marker, err := idempotency.IdempotencyMarkerKey(locator)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	return nil, keyvalue.Condition{Key: marker}, nil
}

func serviceDeployBody(request release.ServiceDeployInput) requestidempotency.Body {
	fields := []requestidempotency.Field{
		requestidempotency.Field{Name: "image", Value: requestidempotency.String(request.Image)},
		requestidempotency.Field{Name: "tag", Value: requestidempotency.String(request.Tag)},
		requestidempotency.Field{Name: "strategy", Value: requestidempotency.String(string(request.Strategy))},
		requestidempotency.Field{Name: "on_failure", Value: requestidempotency.String(string(request.OnFailure))},
		requestidempotency.Field{Name: "after", Value: releaseAfterValue(request.After)},
	}
	if request.HostID != "" {
		fields = append(
			fields,
			requestidempotency.Field{Name: "host_id", Value: requestidempotency.String(request.HostID)},
		)
	}
	return requestidempotency.JSONBody(requestidempotency.Object(fields...))
}
