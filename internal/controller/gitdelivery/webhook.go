package gitdelivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	pushTargets  = root + "webhook/targets/"
	pushQueue    = root + "webhook/pending/"
	pushKind     = "git-delivery-push"
	pushInterval = 2 * time.Second
	pushParallel = 4
	pushLifetime = time.Hour
)

// Push identifies one branch event authenticated against the exact GitHub App
// connection and generation in the public receiver. Raw payloads never enter
// the repository.
type Push struct {
	ConnectionID         string   `json:"connection_id"`
	ConnectionScopeKind  ids.Kind `json:"connection_scope_kind"`
	ConnectionScopeID    string   `json:"connection_scope_id"`
	ConnectionGeneration int64    `json:"connection_generation"`
	InstallationID       int64    `json:"installation_id"`
	RepositoryID         int64    `json:"repository_id"`
	Branch               string   `json:"branch"`
	Commit               string   `json:"commit"`
}

func (event Push) valid() bool {
	switch event.ConnectionScopeKind {
	case ids.KindTenant, ids.KindProject, ids.KindEnvironment:
	default:
		return false
	}
	return namePattern.MatchString(event.ConnectionID) &&
		ids.Validate(event.ConnectionScopeKind, event.ConnectionScopeID) == nil &&
		event.ConnectionGeneration > 0 && event.InstallationID > 0 && event.RepositoryID > 0 &&
		validBranch(event.Branch) && commitPattern.MatchString(event.Commit) &&
		event.Commit != strings.Repeat("0", 40)
}

func (event Push) group() string {
	source := strings.Join([]string{
		string(event.ConnectionScopeKind), event.ConnectionScopeID, event.ConnectionID,
		strconv.FormatInt(event.ConnectionGeneration, 10),
		strconv.FormatInt(event.InstallationID, 10), strconv.FormatInt(event.RepositoryID, 10),
		event.Branch,
	}, ":")
	digest := sha256.Sum256([]byte(source))
	return hex.EncodeToString(digest[:])
}

func bindingPushGroup(binding Binding) string {
	event := Push{
		ConnectionID: binding.ConnectionID, ConnectionScopeKind: binding.ConnectionScopeKind,
		ConnectionScopeID: binding.ConnectionScopeID, ConnectionGeneration: binding.ConnectionGeneration,
		InstallationID: binding.InstallationID, RepositoryID: binding.RepositoryID, Branch: binding.Branch,
	}
	return event.group()
}

func (repository *Repository) pushTargetChanges(
	ctx context.Context,
	binding Binding,
	expectedRevision int64,
) ([]keyvalue.Mutation, error) {
	var before, after string
	if expectedRevision != 0 {
		current, err := repository.binding(ctx, binding.ServiceID)
		if err != nil {
			return nil, err
		}
		if current.Revision != expectedRevision {
			return nil, errs.New(errs.KindStateConflict, "GitHub binding changed before webhook routing was saved")
		}
		if current.Binding.Automatic {
			before = pushTargets + bindingPushGroup(current.Binding) + "/" + binding.ServiceID
		}
	}
	if binding.Automatic {
		after = pushTargets + bindingPushGroup(binding) + "/" + binding.ServiceID
	}
	var mutations []keyvalue.Mutation
	if before != "" && before != after {
		mutations = append(mutations, remove(before))
	}
	if after != "" {
		mutations = append(mutations, put(after, []byte(binding.ServiceID)))
	}
	return mutations, nil
}

type queuedPush struct {
	Push
	Cursor   string    `json:"cursor,omitempty"`
	QueuedAt time.Time `json:"queued_at"`
}

// QueuePush atomically acknowledges a signed event only while the credentials
// used to verify it remain active. One hint per configured branch bounds
// the persisted queue. ObserveHead rechecks the source's live branch and
// admits the eventual deployment through the same durable key as polling.
func (service *Service) QueuePush(ctx context.Context, event Push, authority []keyvalue.Condition) error {
	if !event.valid() || len(authority) == 0 {
		return errs.New(errs.KindValidationFailed, "invalid GitHub webhook event or credential authority")
	}
	group := event.group()
	matches, err := service.repository.store.Range(ctx, keyvalue.RangeRequest{
		Prefix: pushTargets + group + "/", Limit: 1,
	})
	if err != nil || len(matches.Values) == 0 {
		return err
	}
	target := matches.Values[0]
	key := pushQueue + group
	value, err := recordcodec.Encode(pushKind, queuedPush{Push: event, QueuedAt: service.now().UTC()})
	if err != nil {
		return err
	}
	return service.repository.enqueuePush(ctx, key, value, target, event, authority)
}

func (repository *Repository) enqueuePush(ctx context.Context, key string, value []byte,
	target keyvalue.KeyValue, event Push, authority []keyvalue.Condition,
) error {
	for tries := 0; tries < 3; tries++ {
		current, err := repository.store.Get(ctx, key)
		if err != nil {
			return err
		}
		if current.Entry != nil {
			queued, err := decodePush(*current.Entry)
			if err != nil {
				return err
			}
			if queued.Commit == event.Commit {
				return nil
			}
		}
		guards := append(
			[]keyvalue.Condition{condition(target.Key, target.ModRevision), condition(key, revision(current.Entry))},
			authority...)
		result, err := repository.store.Transact(ctx, guards, []keyvalue.Mutation{put(key, value)})
		if err != nil || result.Succeeded {
			return err
		}
	}
	return errs.New(errs.KindExecutionWriteContention, "GitHub webhook receipt changed during admission")
}

func decodePush(entry keyvalue.KeyValue) (queuedPush, error) {
	queued, err := recordcodec.Decode[queuedPush](entry.Value, pushKind)
	if err != nil {
		return queuedPush{}, err
	}
	if !queued.Push.valid() || !recordcodec.IsCanonicalUTC(queued.QueuedAt) ||
		(queued.Cursor != "" && ids.Validate(ids.KindService, queued.Cursor) != nil) || entry.Key != pushQueue+queued.group() {
		return queuedPush{}, errs.New(errs.KindInternal, "stored GitHub webhook receipt is invalid")
	}
	return queued, nil
}

func (service *Service) pollPushes(ctx context.Context, logger *slog.Logger) {
	timer := time.NewTicker(pushInterval)
	defer timer.Stop()
	var after string
	for ctx.Err() == nil {
		page, err := service.repository.store.Range(ctx, keyvalue.RangeRequest{
			Prefix: pushQueue, Limit: pushParallel, StartExclusive: after,
		})
		if err == nil {
			after = ""
			var work sync.WaitGroup
			for _, item := range page.Values {
				if ctx.Err() != nil {
					break
				}
				work.Go(func() {
					if err := service.processPush(ctx, item); err != nil && ctx.Err() == nil {
						kind, _ := errs.KindOf(err)
						logger.DebugContext(ctx, "GitHub webhook work deferred", "kind", kind)
					}
				})
				if page.More {
					after = item.Key
				}
			}
			work.Wait()
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}

func (service *Service) processPush(ctx context.Context, entry keyvalue.KeyValue) error {
	queued, err := decodePush(entry)
	if err != nil {
		return err
	}
	mutation := remove(entry.Key)
	if service.now().Sub(queued.QueuedAt) < pushLifetime {
		next, err := service.notifyPushTargets(ctx, queued)
		if err != nil {
			return err
		}
		if next != "" {
			queued.Cursor = next
			value, err := recordcodec.Encode(pushKind, queued)
			if err != nil {
				return err
			}
			mutation = put(entry.Key, value)
		}
	}
	_, err = service.repository.store.Transact(ctx,
		[]keyvalue.Condition{condition(entry.Key, entry.ModRevision)}, []keyvalue.Mutation{mutation})
	return err
}

func (service *Service) notifyPushTargets(ctx context.Context, queued queuedPush) (string, error) {
	targets, next, err := service.repository.index(
		ctx,
		pushTargets+queued.group()+"/",
		ids.KindService,
		queued.Cursor,
		1,
	)
	if err != nil {
		return "", err
	}
	for _, serviceID := range targets {
		attempt, cancel := context.WithTimeout(ctx, automaticCallTimeout)
		err := service.notifyPushTarget(attempt, serviceID, queued.Push)
		cancel()
		if err != nil {
			return "", err
		}
	}
	return next, nil
}

func (service *Service) notifyPushTarget(ctx context.Context, serviceID string, event Push) error {
	target, err := service.repository.binding(ctx, serviceID)
	if err != nil {
		if kind, _ := errs.KindOf(err); kind == errs.KindRequestNotFound {
			return nil
		}
		return err
	}
	if !target.Binding.Automatic || bindingPushGroup(target.Binding) != event.group() {
		return nil
	}
	_, err = service.ObserveHead(ctx, target, event.Commit)
	if authorityRejected(err) {
		return nil
	}
	return err
}
