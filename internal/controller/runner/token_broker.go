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

// Stage transfers ownership of token to the broker. A duplicate key is a
// caller error because replacing it could let one request alter another Task.
func (broker *TokenBroker) Stage(key TokenKey, token *RegistrationToken) error {
	if broker == nil || !key.valid() || token == nil || len(token.value) == 0 {
		if token != nil {
			token.clear()
		}
		return errs.New(errs.KindValidationFailed, "runner token broker input is invalid")
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if _, exists := broker.tokens[key]; exists {
		token.clear()
		return errs.New(errs.KindStateConflict, "runner registration token is already staged")
	}
	broker.tokens[key] = token
	return nil
}

// Consume transfers token ownership to the caller and removes it atomically.
func (broker *TokenBroker) Consume(key TokenKey) (*RegistrationToken, bool) {
	if broker == nil || !key.valid() {
		return nil, false
	}
	broker.mu.Lock()
	defer broker.mu.Unlock()
	token, exists := broker.tokens[key]
	if exists {
		delete(broker.tokens, key)
	}
	return token, exists
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
