package runner

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTokenBrokerConsumesExactAttemptOnce(t *testing.T) {
	t.Parallel()
	broker := NewTokenBroker()
	t.Cleanup(broker.Clear)
	const tokenValue = "github-registration-token"
	source := []byte(tokenValue)
	token, err := NewRegistrationToken(source)
	if err != nil {
		t.Fatalf("NewRegistrationToken() error = %v", err)
	}
	key := TokenKey{
		TaskID:     "task_01M0ZJAQH5YE4KVB81DW6G0M3W",
		WorkItemID: "work_01M0ZJAQH5YE4KVB81DW6G0M3W",
		Attempt:    1,
	}
	if err := broker.Stage(key, token, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	owned := broker.tokens[key]
	if owned == nil || owned.token == token || string(owned.token.value) != tokenValue || len(token.value) != 0 {
		t.Fatalf("broker ownership = %#v, caller token length = %d", owned, len(token.value))
	}
	token.clear()
	if _, found := broker.Consume(TokenKey{TaskID: key.TaskID, WorkItemID: key.WorkItemID, Attempt: 2}); found {
		t.Fatal("Consume() used a token from another attempt")
	}
	if _, found := broker.Consume(TokenKey{TaskID: key.TaskID, WorkItemID: "work_01M0ZJAQH5YE4KVB81DW6G0M3X", Attempt: key.Attempt}); found {
		t.Fatal("Consume() used a token from another work item")
	}
	consumed, found := broker.Consume(key)
	if !found || consumed == nil {
		t.Fatalf("Consume() = (%p, %t), want a token", consumed, found)
	}
	if consumed == owned.token || string(consumed.value) != tokenValue {
		t.Fatalf("Consume() token = %p, value %q", consumed, consumed.value)
	}
	if len(owned.token.value) != 0 {
		t.Fatal("Consume() did not clear broker-owned bytes")
	}
	if _, found := broker.Consume(key); found {
		t.Fatal("Consume() returned the same token twice")
	}
	consumed.clear()
	if len(consumed.value) != 0 {
		t.Fatal("consumer did not clear transferred token bytes")
	}
}

func TestTokenBrokerRejectsDuplicateAndClearsRejectedToken(t *testing.T) {
	t.Parallel()
	broker := NewTokenBroker()
	t.Cleanup(broker.Clear)
	key := TokenKey{
		TaskID:     "task_01M0ZJAQH5YE4KVB81DW6G0M3W",
		WorkItemID: "work_01M0ZJAQH5YE4KVB81DW6G0M3W",
		Attempt:    1,
	}
	first, err := NewRegistrationToken([]byte("first-token"))
	if err != nil {
		t.Fatalf("NewRegistrationToken(first) error = %v", err)
	}
	second, err := NewRegistrationToken([]byte("second-token"))
	if err != nil {
		t.Fatalf("NewRegistrationToken(second) error = %v", err)
	}
	if err := broker.Stage(key, first, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Stage(first) error = %v", err)
	}
	if err := broker.Stage(key, second, time.Now().Add(time.Minute)); err == nil {
		t.Fatal("Stage(second) error = nil, want state.conflict")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("Stage(second) error = %v, want state.conflict", err)
	}
	if len(second.value) != 0 {
		t.Fatal("duplicate token bytes were not cleared")
	}
	consumed, found := broker.Consume(key)
	if !found || string(consumed.value) != "first-token" {
		t.Fatal("duplicate stage replaced the original token")
	}
	consumed.clear()
}

func TestTokenBrokerDropAndClearEraseOwnedBytes(t *testing.T) {
	t.Parallel()
	broker := NewTokenBroker()
	t.Cleanup(broker.Clear)
	first, err := NewRegistrationToken([]byte("first-token"))
	if err != nil {
		t.Fatalf("NewRegistrationToken(first) error = %v", err)
	}
	second, err := NewRegistrationToken([]byte("second-token"))
	if err != nil {
		t.Fatalf("NewRegistrationToken(second) error = %v", err)
	}
	firstKey := TokenKey{
		TaskID:     "task_01M0ZJAQH5YE4KVB81DW6G0M3W",
		WorkItemID: "work_01M0ZJAQH5YE4KVB81DW6G0M3W",
		Attempt:    1,
	}
	secondKey := TokenKey{
		TaskID:     "task_01M0ZJAQH5YE4KVB81DW6G0M3X",
		WorkItemID: "work_01M0ZJAQH5YE4KVB81DW6G0M3X",
		Attempt:    1,
	}
	if err := broker.Stage(firstKey, first, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Stage(first) error = %v", err)
	}
	if err := broker.Stage(secondKey, second, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("Stage(second) error = %v", err)
	}
	ownedFirst := broker.tokens[firstKey]
	ownedSecond := broker.tokens[secondKey]
	broker.Drop(firstKey)
	if len(first.value) != 0 || len(ownedFirst.token.value) != 0 {
		t.Fatal("Drop() did not clear token bytes")
	}
	broker.Clear()
	if len(second.value) != 0 || len(ownedSecond.token.value) != 0 {
		t.Fatal("Clear() did not clear token bytes")
	}
	if _, found := broker.Consume(secondKey); found {
		t.Fatal("Clear() left a staged token")
	}
}
