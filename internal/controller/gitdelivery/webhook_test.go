package gitdelivery

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func TestWebhookAndBranchPollShareOneDurableDeployment(t *testing.T) {
	ctx := t.Context()
	service, binding, _ := testService(t, &testReleases{})
	store := &pushTestStore{service.repository.store.(*testStore)}
	service.repository.store = store
	current, err := service.GetBinding(ctx, binding.ServiceID)
	if err != nil {
		t.Fatal(err)
	}
	binding.Automatic = true
	current, err = service.Bind(ctx, binding, current.Revision)
	if err != nil {
		t.Fatal(err)
	}
	event := Push{
		ConnectionID: binding.ConnectionID, ConnectionScopeKind: binding.ConnectionScopeKind,
		ConnectionScopeID: binding.ConnectionScopeID, ConnectionGeneration: binding.ConnectionGeneration,
		InstallationID: binding.InstallationID, RepositoryID: binding.RepositoryID,
		Branch: binding.Branch, Commit: testCommit,
	}
	guards := []keyvalue.Condition{{Key: "/credentials/revoked"}}
	invalid := event
	invalid.RepositoryID++
	if err := service.QueuePush(ctx, invalid, guards); err != nil {
		t.Fatal(err)
	}
	if store.lookup(pushQueue+invalid.group()) != nil {
		t.Fatal("another repository incorrectly matched the binding")
	}
	if err := service.QueuePush(ctx, event, []keyvalue.Condition{{Key: "/credentials/current", ModRevision: 42}}); err == nil {
		t.Fatal("changed credential fence admitted a webhook")
	}
	if store.lookup(pushQueue+event.group()) != nil {
		t.Fatal("changed credential fence left a durable receipt")
	}
	for range 2 {
		if err := service.QueuePush(ctx, event, guards); err != nil {
			t.Fatal(err)
		}
	}
	receipt := store.lookup(pushQueue + event.group())
	if receipt == nil {
		t.Fatal("missing receipt after the event was acknowledged")
	}
	if err := service.processPush(ctx, *receipt); err != nil {
		t.Fatal(err)
	}
	pushID := string(store.lookup(headKey(binding.ServiceID)).Value)
	poll, err := service.ObserveHead(ctx, current, event.Commit)
	if err != nil {
		t.Fatal(err)
	}
	if pushID != poll.ID || store.lookup(pushQueue+event.group()) != nil {
		t.Fatal("a duplicate delivery or poll left another deployment or receipt")
	}
}

type pushTestStore struct{ *testStore }

func (store *pushTestStore) Range(_ context.Context, request keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	var entries []keyvalue.KeyValue
	for key, entry := range store.entries {
		if strings.HasPrefix(key, request.Prefix) && key > request.StartExclusive {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	more := int64(len(entries)) > request.Limit
	if more {
		entries = entries[:request.Limit]
	}
	return &keyvalue.RangeResult{Values: entries, ReadRevision: store.revision, More: more}, nil
}
