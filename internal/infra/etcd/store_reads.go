package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
)

func (s *store) Get(ctx context.Context, key string) (*etcdstore.GetResult, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return nil, err
	}
	response, err := s.client.Get(ctx, physical)
	if err != nil {
		return nil, wrap(ctx, err)
	}
	if response.Header == nil {
		return nil, errs.New(errs.KindInternal, "etcd get response is missing its read revision")
	}
	result := &etcdstore.GetResult{ReadRevision: response.Header.Revision}
	if len(response.Kvs) == 0 {
		return result, nil
	}
	item := response.Kvs[0]
	logical, ok := s.logicalKey(string(item.Key))
	if !ok {
		return nil, errs.New(errs.KindInternal, "etcd returned a key outside the configured prefix")
	}
	result.Entry = &etcdstore.KeyValue{
		Key:         logical,
		Value:       append([]byte(nil), item.Value...),
		Version:     item.Version,
		ModRevision: item.ModRevision,
	}
	return result, nil
}
func (s *store) GetMany(ctx context.Context, request etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error) {
	if len(request.Keys) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd multi-get requires at least one key")
	}
	if request.Revision < 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd multi-get revision must not be negative")
	}

	physicalKeys := make([]string, len(request.Keys))
	operations := make([]clientv3.Op, len(request.Keys))
	for index, key := range request.Keys {
		physical, err := s.physicalKey(key)
		if err != nil {
			return nil, err
		}
		physicalKeys[index] = physical
		options := make([]clientv3.OpOption, 0, 1)
		if request.Revision > 0 {
			options = append(options, clientv3.WithRev(request.Revision))
		}
		operations[index] = clientv3.OpGet(physical, options...)
	}

	response, err := s.client.Txn(ctx).Then(operations...).Commit()
	if err != nil {
		return nil, wrap(ctx, err)
	}
	if response.Header == nil {
		clearGetManyResponseValues(response)
		return nil, errs.New(errs.KindInternal, "etcd multi-get response is missing its revision")
	}
	if len(response.Responses) != len(request.Keys) {
		clearGetManyResponseValues(response)
		return nil, errs.New(errs.KindInternal, "etcd multi-get returned an unexpected response count")
	}

	result := &etcdstore.GetManyResult{
		Values:           make([]*etcdstore.KeyValue, len(request.Keys)),
		ReadRevision:     readRevision(request.Revision, response.Header.Revision),
		ResponseRevision: response.Header.Revision,
	}
	for index, operation := range response.Responses {
		rangeResponse := operation.GetResponseRange()
		if rangeResponse == nil || len(rangeResponse.Kvs) > 1 {
			etcdstore.ClearValues(result.Values)
			clearGetManyResponseValues(response)
			return nil, errs.New(errs.KindInternal, "etcd multi-get returned an invalid range response")
		}
		if len(rangeResponse.Kvs) == 0 {
			continue
		}
		item := rangeResponse.Kvs[0]
		if string(item.Key) != physicalKeys[index] {
			etcdstore.ClearValues(result.Values)
			clearGetManyResponseValues(response)
			return nil, errs.New(errs.KindInternal, "etcd multi-get returned an unexpected key")
		}
		logical, ok := s.logicalKey(string(item.Key))
		if !ok {
			etcdstore.ClearValues(result.Values)
			clearGetManyResponseValues(response)
			return nil, errs.New(errs.KindInternal, "etcd returned a key outside the configured prefix")
		}
		result.Values[index] = &etcdstore.KeyValue{
			Key:         logical,
			Value:       append([]byte(nil), item.Value...),
			Version:     item.Version,
			ModRevision: item.ModRevision,
		}
	}
	return result, nil
}

// clearGetManyResponseValues releases response buffers on paths where no
// caller can take ownership. Successful reads retain the etcdstore.Store's established
// copy-out behavior and do not mutate the client response.
func clearGetManyResponseValues(response *clientv3.TxnResponse) {
	if response == nil {
		return
	}
	for _, operation := range response.Responses {
		rangeResponse := operation.GetResponseRange()
		if rangeResponse == nil {
			continue
		}
		for _, item := range rangeResponse.Kvs {
			if item == nil {
				continue
			}
			clear(item.Value)
			item.Value = nil
		}
	}
}

func (s *store) Range(ctx context.Context, request etcdstore.RangeRequest) (*etcdstore.RangeResult, error) {
	if request.Limit <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd range limit must be positive")
	}
	if request.Revision < 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd range revision must not be negative")
	}

	physicalPrefix, err := s.physicalKey(request.Prefix)
	if err != nil {
		return nil, err
	}
	start := physicalPrefix
	rangeEnd := clientv3.GetPrefixRangeEnd(physicalPrefix)
	if request.StartExclusive != "" {
		if !strings.HasPrefix(request.StartExclusive, request.Prefix) {
			return nil, errs.New(errs.KindValidationFailed, "etcd range start must be within its prefix")
		}
		physicalStart, err := s.physicalKey(request.StartExclusive)
		if err != nil {
			return nil, err
		}
		if request.Descending {
			rangeEnd = physicalStart
		} else {
			start = physicalStart + "\x00"
		}
	}
	direction := clientv3.SortAscend
	if request.Descending {
		direction = clientv3.SortDescend
	}

	options := []clientv3.OpOption{
		clientv3.WithRange(rangeEnd),
		clientv3.WithLimit(request.Limit),
		clientv3.WithSort(clientv3.SortByKey, direction),
	}
	if request.Revision > 0 {
		options = append(options, clientv3.WithRev(request.Revision))
	}
	response, err := s.client.Get(ctx, start, options...)
	if err != nil {
		return nil, wrap(ctx, err)
	}
	if response.Header == nil {
		return nil, errs.New(errs.KindInternal, "etcd range response is missing its revision")
	}

	result := &etcdstore.RangeResult{
		Values:           make([]etcdstore.KeyValue, 0, len(response.Kvs)),
		ReadRevision:     readRevision(request.Revision, response.Header.Revision),
		ResponseRevision: response.Header.Revision,
		More:             response.More,
	}
	for _, item := range response.Kvs {
		key, ok := s.logicalKey(string(item.Key))
		if !ok {
			return nil, errs.New(errs.KindInternal, "etcd returned a key outside the configured prefix")
		}
		if !strings.HasPrefix(key, request.Prefix) {
			return nil, errs.New(errs.KindInternal, "etcd range returned a key outside the requested prefix")
		}
		result.Values = append(result.Values, etcdstore.KeyValue{
			Key:         key,
			Value:       append([]byte(nil), item.Value...),
			Version:     item.Version,
			ModRevision: item.ModRevision,
		})
	}
	return result, nil
}

func readRevision(requested, response int64) int64 {
	if requested > 0 {
		return requested
	}
	return response
}
