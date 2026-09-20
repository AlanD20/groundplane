package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (s *store) Put(ctx context.Context, key string, value []byte) (int64, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Put(ctx, physical, string(value))
	if err != nil {
		return 0, wrap(ctx, err)
	}
	if response.Header == nil {
		return 0, errs.New(errs.KindInternal, "etcd put response is missing its revision")
	}
	return response.Header.Revision, nil
}

func (s *store) Delete(ctx context.Context, key string) (int64, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return 0, err
	}
	response, err := s.client.Delete(ctx, physical)
	if err != nil {
		return 0, wrap(ctx, err)
	}
	if response.Header == nil {
		return 0, errs.New(errs.KindInternal, "etcd delete response is missing its revision")
	}
	return response.Header.Revision, nil
}
