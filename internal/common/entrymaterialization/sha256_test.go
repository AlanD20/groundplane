package entrymaterialization

import (
	"bytes"
	"context"
	standardsha256 "crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// Rationale: the wipeable implementation must remain byte-for-byte compatible
// with SHA-256 across padding and compression-block boundaries.
func TestHasherMatchesSHA256KnownVectorsAndBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value []byte
	}{
		{name: "empty"},
		{name: "abc", value: []byte("abc")},
		{name: "55 bytes", value: bytes.Repeat([]byte{0xa5}, 55)},
		{name: "56 bytes", value: bytes.Repeat([]byte{0xa5}, 56)},
		{name: "63 bytes", value: bytes.Repeat([]byte{0xa5}, 63)},
		{name: "64 bytes", value: bytes.Repeat([]byte{0xa5}, 64)},
		{name: "65 bytes", value: bytes.Repeat([]byte{0xa5}, 65)},
		{name: "multiple blocks", value: bytes.Repeat([]byte("groundplane"), 1000)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hasher := NewHasher()
			for offset := 0; offset < len(test.value); {
				end := min(offset+7, len(test.value))
				written, err := hasher.Write(test.value[offset:end])
				if err != nil || written != end-offset {
					t.Fatalf("Write = %d, %v", written, err)
				}
				offset = end
			}
			standard := Digest(standardsha256.Sum256(test.value))
			if !hasher.Verify(standard) {
				t.Fatal("Verify rejected the standard SHA-256 digest")
			}
			assertHasherWiped(t, hasher)
			if DigestBytes(test.value) != standard {
				t.Fatal("DigestBytes differs from standard SHA-256")
			}
			generator := NewHasher()
			if _, err := generator.Write(test.value); err != nil {
				t.Fatalf("generator Write: %v", err)
			}
			generated, err := generator.SumAndDestroy()
			if err != nil || generated != standard {
				t.Fatalf("SumAndDestroy = %x, %v", generated, err)
			}
			assertHasherWiped(t, generator)
		})
	}
}

// Rationale: a failed comparison still ends the hasher's ownership lifetime
// and must erase buffered plaintext and derived compression state.
func TestHasherVerifyMismatchDestroysState(t *testing.T) {
	t.Parallel()

	hasher := NewHasher()
	if _, err := hasher.Write([]byte("secret")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if hasher.Verify(Digest{}) {
		t.Fatal("Verify accepted a mismatched digest")
	}
	assertHasherWiped(t, hasher)
	hasher.Destroy()
	assertHasherWiped(t, hasher)
}

// Rationale: encoder errors after hashing plaintext and cancellation while a
// content write is blocked must both wipe the injected incremental state.
func TestEncodeWipesHasherOnErrorAndCancellation(t *testing.T) {
	t.Parallel()

	spec := validHeaderSpec(OutputSecretFile, "secrets/token", 1000, 1001, ModePrivate)
	header, err := NewHeader(spec)
	if err != nil {
		t.Fatalf("NewHeader: %v", err)
	}

	t.Run("writer error", func(t *testing.T) {
		hasher := NewHasher()
		source := newTrackingReadCloser([]byte("secret"), nil)
		err := encodeWithHasher(
			context.Background(),
			&failAtWrite{failAt: 5},
			header,
			source,
			testLimits(64),
			hasher,
		)
		if err == nil {
			t.Fatal("encodeWithHasher unexpectedly succeeded")
		}
		assertHasherWiped(t, hasher)
	})

	t.Run("cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		hasher := NewHasher()
		writer := newCancelWrite(ctx, 5)
		source := newTrackingReadCloser([]byte("secret"), nil)
		result := make(chan error, 1)
		go func() {
			result <- encodeWithHasher(ctx, writer, header, source, testLimits(64), hasher)
		}()
		<-writer.started
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("encodeWithHasher error = %v, want context canceled", err)
		}
		assertHasherWiped(t, hasher)
	})
}

// Rationale: decoder digest failure occurs after plaintext has entered the
// incremental state and must still leave no buffered or derived hash state.
func TestDecodeWipesHasherOnDigestError(t *testing.T) {
	t.Parallel()

	frame := canonicalFrame(t, []byte("secret"))
	headerLength := int(binary.BigEndian.Uint32(frame[preludeBytes+1:]))
	contentOffset := preludeBytes + recordHeaderBytes + headerLength + recordHeaderBytes + chunkSequenceBytes
	frame[contentOffset] ^= 0xff
	hasher := NewHasher()
	_, err := decodeWithHasher(
		context.Background(),
		OwnBytes(frame),
		testLimits(64),
		func(_ context.Context, _ Header, content io.Reader) error {
			_, consumeErr := io.Copy(io.Discard, content)
			return consumeErr
		},
		hasher,
	)
	if err == nil {
		t.Fatal("decodeWithHasher accepted a digest mismatch")
	}
	assertHasherWiped(t, hasher)
}

func assertHasherWiped(t *testing.T, value Hasher) {
	t.Helper()
	hasher, ok := value.(*sha256Hasher)
	if !ok {
		t.Fatalf("NewHasher concrete type = %T, want private *sha256Hasher", value)
	}
	if hasher == nil || !hasher.destroyed || hasher.length != 0 || hasher.buffered != 0 {
		t.Fatalf("hasher lifecycle was not destroyed: %#v", hasher)
	}
	for index, value := range hasher.state {
		if value != 0 {
			t.Fatalf("state word %d was not cleared", index)
		}
	}
	for index, value := range hasher.block {
		if value != 0 {
			t.Fatalf("block byte %d was not cleared", index)
		}
	}
}

type failAtWrite struct {
	writes int
	failAt int
}

func (writer *failAtWrite) Write(value []byte) (int, error) {
	writer.writes++
	if writer.writes == writer.failAt {
		return 0, io.ErrClosedPipe
	}
	return len(value), nil
}

type cancelWrite struct {
	ctx     context.Context
	started chan struct{}
	writes  int
	blockAt int
}

func newCancelWrite(ctx context.Context, blockAt int) *cancelWrite {
	return &cancelWrite{ctx: ctx, started: make(chan struct{}), blockAt: blockAt}
}

func (writer *cancelWrite) Write(value []byte) (int, error) {
	writer.writes++
	if writer.writes != writer.blockAt {
		return len(value), nil
	}
	close(writer.started)
	<-writer.ctx.Done()
	return 0, io.ErrClosedPipe
}
