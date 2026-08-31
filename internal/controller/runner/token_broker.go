package runner

import (
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// TokenKey identifies one native Runner Task attempt. Tokens are deliberately
// scoped to attempts so a retry can never consume material from an earlier
// failed execution.
type TokenKey struct {
	TaskID  string
	Attempt uint32
}

func (key TokenKey) valid() bool {
	return key.TaskID != "" && key.Attempt != 0
}

// TokenBroker owns transient Runner registration tokens between intent
// publication and native Controller execution. It is intentionally in-memory:
// a Controller restart loses the token and requires an operator retry.
type TokenBroker struct {
	mu     sync.Mutex
	tokens map[TokenKey]*RegistrationToken
}

func NewTokenBroker() *TokenBroker {
	return &TokenBroker{tokens: make(map[TokenKey]*RegistrationToken)}
}

// Stage copies token into broker-owned zeroable storage and clears the caller's
// object. A duplicate key cannot replace another Task attempt's token.
func (broker *TokenBroker) Stage(key TokenKey, token *RegistrationToken) error {
	if broker == nil || !key.valid() || token == nil {
		if token != nil {
			token.clear()
		}
		return errs.New(errs.KindValidationFailed, "runner token broker input is invalid")
	}
	owned, err := NewRegistrationToken(append([]byte(nil), token.value...))
	token.clear()
	if err != nil {
		return err
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if _, exists := broker.tokens[key]; exists {
		owned.clear()
		return errs.New(errs.KindStateConflict, "runner registration token is already staged")
	}
	broker.tokens[key] = owned
	return nil
}

// Consume removes one token atomically, copies it to caller ownership, and
// clears the broker-owned bytes before returning.
func (broker *TokenBroker) Consume(key TokenKey) (*RegistrationToken, bool) {
	if broker == nil || !key.valid() {
		return nil, false
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	owned, exists := broker.tokens[key]
	if exists {
		delete(broker.tokens, key)
	}
	if !exists || owned == nil {
		return nil, false
	}
	token := &RegistrationToken{value: append([]byte(nil), owned.value...)}
	owned.clear()
	return token, true
}

// Drop erases staged material when publication fails or an attempt terminates
// before consuming it.
func (broker *TokenBroker) Drop(key TokenKey) {
	if broker == nil || !key.valid() {
		return
	}
	broker.mu.Lock()
	token := broker.tokens[key]
	delete(broker.tokens, key)
	broker.mu.Unlock()
	if token != nil {
		token.clear()
	}
}

// Clear erases all staged material during Controller shutdown.
func (broker *TokenBroker) Clear() {
	if broker == nil {
		return
	}
	broker.mu.Lock()
	tokens := broker.tokens
	broker.tokens = make(map[TokenKey]*RegistrationToken)
	broker.mu.Unlock()
	for _, token := range tokens {
		token.clear()
	}
}
