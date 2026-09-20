package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (s *store) Watch(ctx context.Context, prefix string, startRevision int64) (*etcdstore.WatchStream, error) {
	physical, err := s.physicalKey(prefix)
	if err != nil {
		return nil, err
	}
	if startRevision < 0 {
		return nil, errs.New(errs.KindValidationFailed, "etcd watch revision must not be negative")
	}

	options := []clientv3.OpOption{clientv3.WithPrefix()}
	if startRevision > 0 {
		options = append(options, clientv3.WithRev(startRevision))
	}
	upstream := s.client.Watch(ctx, physical, options...)
	events := make(chan etcdstore.Event)
	watchErrors := make(chan error, 1)
	go func() {
		defer close(events)
		defer close(watchErrors)
		for response := range upstream {
			if err := response.Err(); err != nil {
				deliverWatchError(ctx, watchErrors, wrap(ctx, err))
				return
			}
			for _, item := range response.Events {
				if item == nil || item.Kv == nil {
					deliverWatchError(
						ctx,
						watchErrors,
						errs.New(errs.KindInternal, "etcd watch returned an empty event"),
					)
					return
				}
				key, ok := s.logicalKey(string(item.Kv.Key))
				if !ok {
					deliverWatchError(
						ctx,
						watchErrors,
						errs.New(errs.KindInternal, "etcd watch returned a key outside the configured prefix"),
					)
					return
				}
				event := etcdstore.Event{
					Key:         key,
					Value:       append([]byte(nil), item.Kv.Value...),
					ModRevision: item.Kv.ModRevision,
				}
				switch item.Type {
				case mvccpb.PUT:
					event.Type = etcdstore.EventPut
				case mvccpb.DELETE:
					event.Type = etcdstore.EventDelete
				default:
					continue
				}

				select {
				case events <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return &etcdstore.WatchStream{Events: events, Errors: watchErrors}, nil
}

func deliverWatchError(ctx context.Context, destination chan<- error, err error) {
	select {
	case destination <- err:
	case <-ctx.Done():
	}
}
