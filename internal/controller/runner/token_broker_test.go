package runner

import (
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTokenBrokerConsumesExactAttemptOnce(t *testing.T) {
	t.Parallel()
	broker := NewTokenBroker()
	source := []byte("github-registration-token")
	token, err := NewRegistrationToken(source)
	if err != nil {
		t.Fatalf("NewRegistrationToken() error = %v", err)
	}
	key := TokenKey{TaskID: "tsk_01M0ZJAQH5YE4KVB81DW6G0M3W", Attempt: 1}
	if err := broker.Stage(key, token); err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	if _, found := broker.Consume(TokenKey{TaskID: key.TaskID, Attempt: 2}); found {
		t.Fatal("Consume() used a token from another attempt")
	}
	consumed, found := broker.Consume(key)
	if !found || consumed != token {
		t.Fatalf("Consume() = (%p, %t), want (%p, true)", consumed, found, token)
	}
	if _, found := broker.Consume(key); found {
		t.Fatal("Consume() returned the same token twice")
	}
}

func TestTokenBrokerRejectsDuplicateAndClearsRejectedToken(t *testing.T) {
	t.Parallel()
	broker := NewTokenBroker()
	key := TokenKey{TaskID: "tsk_01M0ZJAQH5YE4KVB81DW6G0M3W", Attempt: 1}
	first, err := NewRegistrationToken([]byte("first-token"))
	if err != nil {
		t.Fatalf("NewRegistrationToken(first) error = %v", err)
	}
	second, err := NewRegistrationToken([]byte("second-token"))
	if err != nil {
		t.Fatalf("NewRegistrationToken(second) error = %v", err)
	}
	if err := broker.Stage(key, first); err != nil {
		t.Fatalf("Stage(first) error = %v", err)
	}
	if err := broker.Stage(key, second); err == nil {
		t.Fatal("Stage(second) error = nil, want state.conflict")
	} else if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStateConflict {
		t.Fatalf("Stage(second) error = %v, want state.conflict", err)
	}
	if len(second.value) != 0 {
		t.Fatal("duplicate token bytes were not cleared")
	}
	consumed, found := broker.Consume(key)
	if !found || consumed != first {
		t.Fatal("duplicate stage replaced the original token")
	}
}

func TestTokenBrokerDropAndClearEraseOwnedBytes(t *testing.T) {
	t.Parallel()
	broker := NewTokenBroker()
	first, err := NewRegistrationToken([]byte("first-token"))
	if err != nil {
		t.Fatalf("NewRegistrationToken(first) error = %v", err)
	}
	second, err := NewRegistrationToken([]byte("second-token"))
	if err != nil {
		t.Fatalf("NewRegistrationToken(second) error = %v", err)
	}
	firstKey := TokenKey{TaskID: "tsk_01M0ZJAQH5YE4KVB81DW6G0M3W", Attempt: 1}
	secondKey := TokenKey{TaskID: "tsk_01M0ZJAQH5YE4KVB81DW6G0M3X", Attempt: 1}
	if err := broker.Stage(firstKey, first); err != nil {
		t.Fatalf("Stage(first) error = %v", err)
	}
	if err := broker.Stage(secondKey, second); err != nil {
		t.Fatalf("Stage(second) error = %v", err)
	}
	broker.Drop(firstKey)
	if len(first.value) != 0 {
		t.Fatal("Drop() did not clear token bytes")
	}
	broker.Clear()
	if len(second.value) != 0 {
		t.Fatal("Clear() did not clear token bytes")
	}
	if _, found := broker.Consume(secondKey); found {
		t.Fatal("Clear() left a staged token")
	}
}
