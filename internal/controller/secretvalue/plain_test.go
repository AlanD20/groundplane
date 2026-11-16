package secretvalue

import (
	"context"
	"errors"
	"testing"
)

// Explicit plaintext storage must not call the key provider or become a
// fallback when decryption of an encrypted record fails.
func TestExplicitPlainValuesAndFailedDecryptionStayDistinct(t *testing.T) {
	provider := &unavailablePlainTestCipher{}
	protector, err := NewProtector(provider, provider)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{"", "operator-selected-unencrypted"} {
		envelope, err := Plain([]byte(input))
		if err != nil {
			t.Fatal(err)
		}
		var retained []byte
		err = protector.OpenOwned(context.Background(), &envelope, func(value []byte) error {
			if string(value) != input {
				t.Fatalf("value = %q, want %q", value, input)
			}
			retained = value
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !allZero(retained) {
			t.Fatal("plaintext callback retained uncleared bytes")
		}
	}
	if provider.calls != 0 {
		t.Fatal("plaintext called the key provider")
	}
	crypted, err := Restore(currentMetadata([]byte("unreadable-age")), []byte("unreadable-age"))
	if err != nil {
		t.Fatal(err)
	}
	called := false
	err = protector.Open(context.Background(), crypted, func([]byte) error { called = true; return nil })
	if err == nil || called || provider.calls != 1 {
		t.Fatal("failed decryption was accepted as plaintext")
	}
}

type unavailablePlainTestCipher struct{ calls int }

func (cipher *unavailablePlainTestCipher) Seal(context.Context, []byte) ([]byte, error) {
	cipher.calls++
	return nil, errors.New("key unavailable")
}
func (cipher *unavailablePlainTestCipher) Open(context.Context, []byte) ([]byte, error) {
	cipher.calls++
	return nil, errors.New("key unavailable")
}
