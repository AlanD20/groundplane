package recordcodec

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestGDR1EnvelopeHasExactLayout(t *testing.T) {
	t.Parallel()
	payload := []byte("projection")
	value, err := Encode(7, payload)
	if err != nil {
		t.Fatal(err)
	}
	wantDigest := sha256.Sum256(payload)
	if len(value) != HeaderBytes+len(payload) || string(value[:4]) != "GDR1" ||
		value[4] != 7 || value[5] != 1 || value[6] != 0 || value[7] != 0 ||
		binary.BigEndian.Uint32(value[8:12]) != uint32(len(payload)) ||
		string(value[12:44]) != string(wantDigest[:]) {
		t.Fatalf("unexpected GDR1 envelope: %x", value)
	}
	decoded, err := Decode(value, 7, len(payload))
	if err != nil || string(decoded) != string(payload) {
		t.Fatalf("Decode() = %q, %v", decoded, err)
	}
}

func TestGDR1DecodeRejectsEveryHeaderAuthorityMismatch(t *testing.T) {
	t.Parallel()
	valid, err := Encode(3, []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func([]byte){
		"magic":   func(value []byte) { value[0] = 'X' },
		"kind":    func(value []byte) { value[4] = 4 },
		"version": func(value []byte) { value[5] = 2 },
		"flags":   func(value []byte) { value[7] = 1 },
		"length":  func(value []byte) { binary.BigEndian.PutUint32(value[8:12], 1) },
		"digest":  func(value []byte) { value[12] ^= 1 },
		"payload": func(value []byte) { value[len(value)-1] ^= 1 },
	}
	for name, mutate := range cases {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			value := append([]byte(nil), valid...)
			mutate(value)
			_, decodeErr := Decode(value, 3, 0)
			if !errors.Is(decodeErr, errs.New(errs.KindInternal, "")) {
				t.Fatalf("Decode() error = %v", decodeErr)
			}
		})
	}
}
