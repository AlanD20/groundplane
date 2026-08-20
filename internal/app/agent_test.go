package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the Agent must decode only the exact Controller-materialized
// 43-byte unpadded base64url credential, without trimming or normalization.
func TestReadAgentTokenRequiresExactEncoding(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte{0x5a}, agentprotocol.RawTokenBytes)
	encoded := []byte(base64.RawURLEncoding.EncodeToString(raw))
	tests := []struct {
		name    string
		content []byte
		wantErr bool
	}{
		{name: "exact", content: encoded},
		{name: "newline", content: append(append([]byte(nil), encoded...), '\n'), wantErr: true},
		{name: "padding", content: append(append([]byte(nil), encoded...), '='), wantErr: true},
		{name: "invalid alphabet", content: bytes.Repeat([]byte{'!'}, agentprotocol.EncodedTokenBytes), wantErr: true},
		{name: "short", content: encoded[:len(encoded)-1], wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(path, test.content, 0o400); err != nil {
				t.Fatalf("write token: %v", err)
			}
			got, err := readAgentTokenForUID(context.Background(), path, uint32(os.Geteuid()))
			if (err != nil) != test.wantErr {
				t.Fatalf("readAgentToken() error = %v, want error = %t", err, test.wantErr)
			}
			if !test.wantErr && !bytes.Equal(got, raw) {
				t.Fatal("readAgentToken() decoded bytes differ")
			}
			clear(got)
		})
	}
}

// Rationale: token-file failures must be canonical and must not disclose file content.
func TestReadAgentTokenDoesNotLeakMalformedCredential(t *testing.T) {
	t.Parallel()

	plaintext := bytes.Repeat([]byte{'!'}, agentprotocol.EncodedTokenBytes)
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, plaintext, 0o400); err != nil {
		t.Fatalf("write token: %v", err)
	}
	_, err := readAgentTokenForUID(context.Background(), path, uint32(os.Geteuid()))
	if err == nil {
		t.Fatal("readAgentToken() error = nil, want malformed token failure")
	}
	if bytes.Contains([]byte(err.Error()), plaintext) {
		t.Fatalf("error leaked credential: %v", err)
	}
	var domainErr *errs.Error
	if !errors.As(err, &domainErr) || domainErr.Code != errs.CodeValidationFailed {
		t.Fatalf("error = %v, want validation.failed", err)
	}
}

// Rationale: the Agent must refuse path substitution, weak permissions, and
// wrong ownership before credential bytes reach the gRPC client.
func TestReadAgentTokenRejectsUnsafeFileMetadata(t *testing.T) {
	t.Parallel()

	encoded := []byte(base64.RawURLEncoding.EncodeToString(bytes.Repeat(
		[]byte{0x71},
		agentprotocol.RawTokenBytes,
	)))
	tests := []struct {
		name        string
		prepare     func(*testing.T, string)
		expectedUID uint32
	}{
		{
			name: "symlink",
			prepare: func(t *testing.T, path string) {
				t.Helper()
				target := filepath.Join(filepath.Dir(path), "target")
				if err := os.WriteFile(target, encoded, 0o400); err != nil {
					t.Fatalf("write target: %v", err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatalf("create symlink: %v", err)
				}
			},
			expectedUID: uint32(os.Geteuid()),
		},
		{
			name: "weak mode",
			prepare: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, encoded, 0o600); err != nil {
					t.Fatalf("write token: %v", err)
				}
			},
			expectedUID: uint32(os.Geteuid()),
		},
		{
			name: "wrong owner",
			prepare: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, encoded, 0o400); err != nil {
					t.Fatalf("write token: %v", err)
				}
			},
			expectedUID: uint32(os.Geteuid()) ^ 1,
		},
		{
			name: "directory",
			prepare: func(t *testing.T, path string) {
				t.Helper()
				if err := os.Mkdir(path, 0o400); err != nil {
					t.Fatalf("create directory: %v", err)
				}
			},
			expectedUID: uint32(os.Geteuid()),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "token")
			test.prepare(t, path)
			if _, err := readAgentTokenForUID(context.Background(), path, test.expectedUID); err == nil {
				t.Fatal("readAgentTokenForUID() error = nil, want unsafe-file rejection")
			}
		})
	}
}
