package networkreservations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareProxyAddressRelease is called only by a proven Service-removal
// finalizer. Failed Deploys and Destroy-runtime operations retain reservations.
func PrepareProxyAddressRelease(ctx context.Context, store interface {
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}, serviceID string, revision int64) (ProxyAddresses, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return ProxyAddresses{}, err
	}
	if ids.Validate(ids.KindService, serviceID) != nil {
		return ProxyAddresses{}, errs.New(errs.KindValidationFailed, "Service id is invalid")
	}
	result := ProxyAddresses{}
	start := ""
	for {
		page, err := store.Range(
			ctx,
			keyvalue.RangeRequest{
				Prefix:         ComponentAddressRegistryKey(""),
				StartExclusive: start,
				Revision:       revision,
				Limit:          200,
			},
		)
		if err != nil {
			result.Clear()
			return ProxyAddresses{}, err
		}
		if page == nil || page.ReadRevision <= 0 {
			result.Clear()
			return ProxyAddresses{}, errs.New(errs.KindInternal, "proxy address release read is incomplete")
		}
		if revision == 0 {
			revision = page.ReadRevision
		}
		for _, value := range page.Values {
			start = value.Key
			registry, err := recordcodec.Decode[ComponentAddressRegistry](value.Value, "component_address_registry")
			if err != nil {
				result.Clear()
				return ProxyAddresses{}, err
			}
			if _, found := registry.ServiceReservations[serviceID]; !found {
				continue
			}
			delete(registry.ServiceReservations, serviceID)
			encoded, err := recordcodec.Encode("component_address_registry", registry)
			if err != nil {
				result.Clear()
				return ProxyAddresses{}, err
			}
			result.conditions = append(
				result.conditions,
				keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision},
			)
			result.mutations = append(
				result.mutations,
				keyvalue.Mutation{Type: keyvalue.MutationPut, Key: value.Key, Value: encoded},
			)
		}
		if !page.More {
			return result, nil
		}
		if len(page.Values) == 0 {
			result.Clear()
			return ProxyAddresses{}, errs.New(errs.KindInternal, "proxy address release scan did not advance")
		}
	}
}
