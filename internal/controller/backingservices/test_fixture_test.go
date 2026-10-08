package backingservices

import (
	"context"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/adapters/custom"
	"github.com/AlanD20/groundplane/internal/adapters/postgres"
	"github.com/AlanD20/groundplane/internal/adapters/valkey"
)

func registerAdapters() {
	if _, registered := adapters.Get("postgres"); !registered {
		postgres.Register()
	}
	if _, registered := adapters.Get("valkey"); !registered {
		valkey.Register()
	}
	if _, registered := adapters.Get("custom"); !registered {
		custom.Register()
	}
}

type backingTestCrypt struct{}

func (backingTestCrypt) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte(nil), plaintext...), nil
}

func (backingTestCrypt) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}
