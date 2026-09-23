package operations

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
)

type entryReadTestCipher struct{}

func (entryReadTestCipher) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte{1}, plaintext...), nil
}

func (entryReadTestCipher) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext[1:]...), nil
}

func secretReadTestProtector(t *testing.T) *secretvalue.Protector {
	t.Helper()
	cipher := entryReadTestCipher{}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	return protector
}

func secretReadTestTime() time.Time {
	return time.Date(2026, time.August, 22, 14, 0, 0, 0, time.UTC)
}

func allBytesZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
