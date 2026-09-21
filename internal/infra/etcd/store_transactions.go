package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"strings"
)

func (s *store) Transact(
	ctx context.Context,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {
	if len(mutations) == 0 {
		return etcdstore.TransactionResult{}, errs.New(errs.KindValidationFailed, "etcd transaction requires a mutation")
	}
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return etcdstore.TransactionResult{}, errs.Newf(
			errs.KindValidationFailed,
			"etcd transaction exceeds the %d compare-and-mutation limit",
			etcdstore.MaximumOperations,
		)
	}
	return s.transact(ctx, conditions, mutations)
}

func (s *store) transact(
	ctx context.Context,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (etcdstore.TransactionResult, error) {

	prepared, err := s.prepareTransaction(conditions, mutations)
	if err != nil {
		return etcdstore.TransactionResult{}, err
	}

	transaction := s.client.Txn(ctx)
	if len(prepared.comparisons) > 0 {
		transaction = transaction.If(prepared.comparisons...)
	}
	transaction = transaction.Then(prepared.operations...)
	if len(prepared.failureReads) > 0 {
		transaction = transaction.Else(prepared.failureReads...)
	}
	response, err := transaction.Commit()
	if err != nil {
		return etcdstore.TransactionResult{}, wrap(ctx, err)
	}
	if response.Header == nil {
		return etcdstore.TransactionResult{}, errs.New(errs.KindInternal, "etcd transaction response is missing its revision")
	}
	result := etcdstore.TransactionResult{Succeeded: response.Succeeded, Revision: response.Header.Revision}
	if !response.Succeeded {
		reads, err := transactionFailureReads(response, conditions, prepared.physicalConditions, s.root)
		if err != nil {
			return etcdstore.TransactionResult{}, err
		}
		result.FailureReads = reads
	}
	return result, nil
}

func transactionRequest(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	physicalConditions []string,
	physicalMutations []string,
) *etcdserverpb.TxnRequest {
	request := &etcdserverpb.TxnRequest{
		Compare: make([]*etcdserverpb.Compare, 0, len(conditions)),
		Success: make([]*etcdserverpb.RequestOp, 0, len(mutations)),
		Failure: make([]*etcdserverpb.RequestOp, 0, len(conditions)),
	}
	for index, condition := range conditions {
		comparison := &etcdserverpb.Compare{
			Result:      etcdserverpb.Compare_EQUAL,
			Target:      etcdserverpb.Compare_MOD,
			Key:         []byte(physicalConditions[index]),
			TargetUnion: &etcdserverpb.Compare_ModRevision{ModRevision: condition.ModRevision},
		}
		failure := &etcdserverpb.RangeRequest{Key: []byte(physicalConditions[index])}
		if condition.Prefix {
			end := []byte(clientv3.GetPrefixRangeEnd(physicalConditions[index]))
			comparison.RangeEnd = end
			failure.RangeEnd = end
			failure.Limit = 1
		}
		request.Compare = append(request.Compare, comparison)
		request.Failure = append(request.Failure, &etcdserverpb.RequestOp{
			Request: &etcdserverpb.RequestOp_RequestRange{RequestRange: failure},
		})
	}
	for index, mutation := range mutations {
		operation := &etcdserverpb.RequestOp{}
		switch mutation.Type {
		case etcdstore.MutationPut:
			operation.Request = &etcdserverpb.RequestOp_RequestPut{RequestPut: &etcdserverpb.PutRequest{
				Key: []byte(physicalMutations[index]), Value: mutation.Value,
			}}
		case etcdstore.MutationDelete:
			request := &etcdserverpb.DeleteRangeRequest{Key: []byte(physicalMutations[index])}
			if mutation.Prefix {
				request.RangeEnd = []byte(clientv3.GetPrefixRangeEnd(physicalMutations[index]))
			}
			operation.Request = &etcdserverpb.RequestOp_RequestDeleteRange{
				RequestDeleteRange: request,
			}
		}
		request.Success = append(request.Success, operation)
	}
	return request
}

func transactionFailureReads(
	response *clientv3.TxnResponse,
	conditions []etcdstore.Condition,
	physicalKeys []string,
	root string,
) ([]*etcdstore.KeyValue, error) {
	if len(response.Responses) != len(physicalKeys) || len(conditions) != len(physicalKeys) {
		return nil, errs.New(errs.KindInternal, "etcd transaction failure reads are incomplete")
	}
	values := make([]*etcdstore.KeyValue, len(physicalKeys))
	for index, operation := range response.Responses {
		rangeResponse := operation.GetResponseRange()
		if rangeResponse == nil || (!conditions[index].Prefix && rangeResponse.More) || len(rangeResponse.Kvs) > 1 {
			return nil, errs.New(errs.KindInternal, "etcd transaction failure read is invalid")
		}
		if len(rangeResponse.Kvs) == 0 {
			continue
		}
		entry := rangeResponse.Kvs[0]
		matches := string(entry.Key) == physicalKeys[index]
		if conditions[index].Prefix {
			matches = strings.HasPrefix(string(entry.Key), physicalKeys[index])
		}
		if !matches || entry.ModRevision <= 0 {
			return nil, errs.New(errs.KindInternal, "etcd transaction failure read is inconsistent")
		}
		values[index] = &etcdstore.KeyValue{
			Key:         strings.TrimPrefix(string(entry.Key), root),
			Value:       append([]byte(nil), entry.Value...),
			Version:     entry.Version,
			ModRevision: entry.ModRevision,
		}
	}
	return values, nil
}
