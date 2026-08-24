package age

import (
	"context"
	"errors"
	"io"
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Backup staging depends on exact age overhead at empty, chunk,
// and int64 overflow boundaries rather than an observed average ciphertext size.
func TestStoredSizeUpperBoundGolden(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		plaintext int64
		want      int64
		wantErr   bool
	}{
		{name: "negative", plaintext: -1, wantErr: true},
		{name: "empty", plaintext: 0, want: 200},
		{name: "one byte", plaintext: 1, want: 201},
		{name: "below one chunk", plaintext: 65535, want: 65735},
		{name: "one full chunk", plaintext: 65536, want: 65736},
		{name: "above one chunk", plaintext: 65537, want: 65753},
		{name: "two full chunks", plaintext: 131072, want: 131288},
		{
			name:      "largest representable stored size",
			plaintext: 9221120786662719287,
			want:      math.MaxInt64,
		},
		{name: "first stored size overflow", plaintext: 9221120786662719288, wantErr: true},
		{name: "maximum plaintext integer", plaintext: math.MaxInt64, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := StoredSizeUpperBound(test.plaintext)
			if test.wantErr {
				if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
					t.Fatalf("StoredSizeUpperBound(%d) error = %v, want validation failure", test.plaintext, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("StoredSizeUpperBound(%d) error = %v", test.plaintext, err)
			}
			if got != test.want {
				t.Fatalf("StoredSizeUpperBound(%d) = %d, want %d", test.plaintext, got, test.want)
			}
		})
	}
}

// Rationale: The arithmetic contract must remain equal to the pinned v1.3.1
// one-X25519-recipient binary encoder, including its required empty final chunk.
func TestStoredSizeUpperBoundMatchesEncryptStream(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}

	for _, plaintext := range []int64{0, 1, 65535, 65536, 65537, 131071, 131072, 131073} {
		t.Run(stringSize(plaintext), func(t *testing.T) {
			input := io.LimitReader(zeroReader{}, plaintext)
			output := &countingWriter{}
			if err := encryptStreamNonClosing(context.Background(), keypair.Recipient, input, output); err != nil {
				t.Fatalf("EncryptStream(%d bytes) error = %v", plaintext, err)
			}
			want, err := StoredSizeUpperBound(plaintext)
			if err != nil {
				t.Fatalf("StoredSizeUpperBound(%d) error = %v", plaintext, err)
			}
			if output.written != want {
				t.Fatalf("EncryptStream(%d bytes) wrote %d bytes, want bound %d", plaintext, output.written, want)
			}
		})
	}
}

// Rationale: Overflow checks must agree with arbitrary-precision arithmetic
// across the whole int64 input domain, not only near hand-picked boundaries.
func TestStoredSizeUpperBoundMatchesBigIntOracle(t *testing.T) {
	t.Parallel()
	random := rand.New(rand.NewSource(47))
	inputs := []int64{-1, 0, 1, 65535, 65536, 65537, math.MaxInt64}
	for range 4096 {
		inputs = append(inputs, int64(random.Uint64()))
	}

	for _, plaintext := range inputs {
		want, representable := storedSizeBigInt(plaintext)
		got, err := StoredSizeUpperBound(plaintext)
		if !representable {
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("StoredSizeUpperBound(%d) error = %v, want validation failure", plaintext, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("StoredSizeUpperBound(%d) error = %v", plaintext, err)
		}
		if got != want.Int64() {
			t.Fatalf("StoredSizeUpperBound(%d) = %d, want %s", plaintext, got, want)
		}
	}
}

func storedSizeBigInt(plaintext int64) (*big.Int, bool) {
	if plaintext < 0 {
		return nil, false
	}
	plain := big.NewInt(plaintext)
	chunks := big.NewInt(1)
	if plaintext > 0 {
		chunks.Sub(plain, big.NewInt(1))
		chunks.Div(chunks, big.NewInt(65536))
		chunks.Add(chunks, big.NewInt(1))
	}
	stored := new(big.Int).Mul(chunks, big.NewInt(16))
	stored.Add(stored, plain)
	stored.Add(stored, big.NewInt(184))
	return stored, stored.IsInt64()
}

type zeroReader struct{}

func (zeroReader) Read(data []byte) (int, error) {
	clear(data)
	return len(data), nil
}

type countingWriter struct {
	written int64
}

func (w *countingWriter) Write(data []byte) (int, error) {
	w.written += int64(len(data))
	return len(data), nil
}

func stringSize(size int64) string {
	return new(big.Int).SetInt64(size).String()
}
