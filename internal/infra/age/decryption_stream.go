package age

import (
	"context"
	"fmt"
	"io"
	"strings"

	age "filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DecryptStream authenticates an age stream under one X25519 identity and
// writes at most maximumPlaintextBytes to caller-owned private staging. Output
// may contain an authenticated prefix when a later chunk fails: callers must
// not publish it or mutate a live restore target until this function succeeds
// and the complete plaintext artifact has also passed its format validation.
//
// Zero-length plaintext is valid, including with a zero byte maximum. Input
// and output remain caller-owned except for actions performed by interrupt.
// Cancellation interrupts header reads, payload reads and output writes, and
// this function joins the interrupt before returning. The owned copy buffer
// is bounded and cleared before return; success requires authenticated EOF.
func DecryptStream(
	ctx context.Context,
	identity string,
	input io.Reader,
	output io.Writer,
	maximumPlaintextBytes uint64,
	interrupt InterruptFunc,
) error {
	if ctx == nil {
		return errs.New(errs.KindValidationFailed, "age: decryption context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if input == nil || output == nil || interrupt == nil {
		return errs.New(errs.KindValidationFailed, "age: decryption input, output and interrupt are required")
	}
	if maximumPlaintextBytes > backupformat.MaxAgeSourceBytes {
		return errs.New(errs.KindValidationFailed, "age: plaintext maximum exceeds the encrypted source limit")
	}
	if len(identity) > maxIdentitySize {
		return errs.New(errs.KindValidationFailed, "age: decryption identity exceeds its size limit")
	}
	parsed, err := age.ParseX25519Identity(strings.TrimSpace(identity))
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, fmt.Errorf("age: invalid decryption identity: %w", err))
	}

	stopInterrupt := watchStreamCancellation(ctx, interrupt)
	defer stopInterrupt()
	checkedInput := &decryptionInput{ctx: ctx, reader: input, headerRemaining: storedFixedSize}
	plaintext, err := age.Decrypt(checkedInput, parsed)
	if err != nil {
		return checkedInput.classifyFailure(err)
	}
	checkedInput.headerRemaining = -1
	checkedOutput := exactWriter{writer: contextWriter{ctx: ctx, writer: output}}
	buffer := make([]byte, encryptionStreamBufferSize)
	defer clear(buffer)
	var written uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, readErr := plaintext.Read(buffer)
		if err := ctx.Err(); err != nil {
			return err
		}
		if readErr != nil && readErr != io.EOF {
			return checkedInput.classifyFailure(readErr)
		}
		if uint64(count) > maximumPlaintextBytes-written {
			return errs.New(errs.KindValidationFailed, "age: plaintext exceeds its size limit")
		}
		if count > 0 {
			_, writeErr := checkedOutput.Write(buffer[:count])
			clear(buffer[:count])
			if writeErr != nil {
				return classifyStreamFailure(ctx, writeErr, "age: write decrypted stream")
			}
			written += uint64(count)
		}
		if readErr == io.EOF {
			return ctx.Err()
		}
		// An authenticated empty final chunk yields (0, nil) before EOF.
	}
}

// decryptionInput distinguishes a source I/O failure from an invalid encrypted
// artifact without allowing dependency-owned cancellation to cross as if it
// belonged to the operation context.
type decryptionInput struct {
	ctx             context.Context
	reader          io.Reader
	failure         error
	headerRemaining int64
}

func (input *decryptionInput) Read(buffer []byte) (int, error) {
	if err := input.ctx.Err(); err != nil {
		return 0, err
	}
	// age's header parser otherwise accumulates arbitrary lines and stanzas.
	// GP artifacts have the fixed one-X25519 header and stream nonce; the
	// payload reader is enabled only after both have been authenticated/read.
	if input.headerRemaining == 0 {
		return 0, errs.New(errs.KindValidationFailed, "age: encrypted header exceeds its size limit")
	}
	if input.headerRemaining > 0 && int64(len(buffer)) > input.headerRemaining {
		buffer = buffer[:input.headerRemaining]
	}
	count, err := input.reader.Read(buffer)
	if input.headerRemaining > 0 {
		input.headerRemaining -= int64(count)
	}
	if contextErr := input.ctx.Err(); contextErr != nil {
		return count, contextErr
	}
	if err != nil && err != io.EOF {
		input.failure = err
	}
	return count, err
}

func (input *decryptionInput) classifyFailure(err error) error {
	if contextErr := input.ctx.Err(); contextErr != nil {
		return contextErr
	}
	if input.failure != nil {
		return classifyStreamFailure(input.ctx, input.failure, "age: read encrypted stream")
	}
	return errs.Wrap(errs.KindValidationFailed, fmt.Errorf("age: authenticate encrypted stream: %w", err))
}
