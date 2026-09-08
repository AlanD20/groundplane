package controller

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testRecoveryPointEnvironmentID = "env_01J00000000000000000000000"
	testRecoveryPointID            = "rp_01J00000000000000000000000"
)

type recoveryPointEnvironmentReaderStub struct{ err error }

func (stub *recoveryPointEnvironmentReaderStub) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	if stub.err != nil {
		return etcd.Versioned[etcd.EnvironmentRecord]{}, stub.err
	}
	return etcd.Versioned[etcd.EnvironmentRecord]{
		Record:       etcd.EnvironmentRecord{ID: testRecoveryPointEnvironmentID},
		Revision:     5,
		ReadRevision: 5,
	}, nil
}

type recoveryPointRepositoryStub struct {
	requests []etcd.BackupRecoveryPointPageRequest
	pages    []etcd.BackupRecoveryPointPage
}

func (stub *recoveryPointRepositoryStub) ListVerifiedRecoveryPointsByEnvironment(
	_context context.Context,
	_environmentID string,
	request etcd.BackupRecoveryPointPageRequest,
) (etcd.BackupRecoveryPointPage, error) {
	stub.requests = append(stub.requests, request)
	page := stub.pages[0]
	stub.pages = stub.pages[1:]
	return page, nil
}

type recoveryPointCursorTestCipher struct{ key []byte }

func newRecoveryPointCursorTestCipher() *recoveryPointCursorTestCipher {
	return &recoveryPointCursorTestCipher{key: []byte("recovery-point-test-cursor-key")}
}

func (cipher *recoveryPointCursorTestCipher) Seal(ctx context.Context, plaintext []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	masked := make([]byte, len(plaintext))
	for index := range plaintext {
		masked[index] = plaintext[index] ^ cipher.key[index%len(cipher.key)]
	}
	mac := hmac.New(sha256.New, cipher.key)
	_, _ = mac.Write(masked)
	return append(masked, mac.Sum(nil)...), nil
}

func (cipher *recoveryPointCursorTestCipher) Open(ctx context.Context, ciphertext []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(ciphertext) <= sha256.Size {
		return nil, errors.New("invalid test ciphertext")
	}
	body := ciphertext[:len(ciphertext)-sha256.Size]
	provided := ciphertext[len(ciphertext)-sha256.Size:]
	mac := hmac.New(sha256.New, cipher.key)
	_, _ = mac.Write(body)
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return nil, errors.New("invalid test authentication")
	}
	plaintext := make([]byte, len(body))
	for index := range body {
		plaintext[index] = body[index] ^ cipher.key[index%len(cipher.key)]
	}
	return plaintext, nil
}

// Rationale: continuation state is an authenticated capability bound to the
// fixed revision and stable Recovery Point id, never a caller-visible etcd key.
func TestRecoveryPointReadServiceUsesAuthenticatedStableBoundary(t *testing.T) {
	t.Parallel()
	points := &recoveryPointRepositoryStub{pages: []etcd.BackupRecoveryPointPage{
		{Revision: 41, NextID: testRecoveryPointID},
		{Revision: 41},
	}}
	service, err := NewRecoveryPointReadService(
		&recoveryPointEnvironmentReaderStub{}, points, newRecoveryPointCursorTestCipher(),
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.ListRecoveryPoints(context.Background(), testRecoveryPointEnvironmentID, "")
	if err != nil || first.Revision != 41 || first.NextCursor == "" {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	if len(first.NextCursor) > recoveryPointMaximumCursor {
		t.Fatalf("cursor length = %d, want <= %d", len(first.NextCursor), recoveryPointMaximumCursor)
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(decoded), "/v1/indexes/") ||
		strings.Contains(string(decoded), testRecoveryPointEnvironmentID) ||
		strings.Contains(string(decoded), testRecoveryPointID) {
		t.Fatalf("cursor exposes storage or boundary data: %q", decoded)
	}
	second, err := service.ListRecoveryPoints(
		context.Background(), testRecoveryPointEnvironmentID, first.NextCursor,
	)
	if err != nil || second.Revision != 41 || second.NextCursor != "" {
		t.Fatalf("second page = %#v, %v", second, err)
	}
	if len(points.requests) != 2 ||
		points.requests[0] != (etcd.BackupRecoveryPointPageRequest{Limit: 50}) ||
		points.requests[1] != (etcd.BackupRecoveryPointPageRequest{
			Limit: 50, AfterID: testRecoveryPointID, Revision: 41,
		}) {
		t.Fatalf("repository requests = %#v", points.requests)
	}
}

// Rationale: changing any authenticated cursor byte must fail as one generic
// malformed cursor without trusting its revision, environment, or boundary.
func TestRecoveryPointReadServiceRejectsTamperedCursor(t *testing.T) {
	t.Parallel()
	points := &recoveryPointRepositoryStub{pages: []etcd.BackupRecoveryPointPage{{
		Revision: 41, NextID: testRecoveryPointID,
	}}}
	service, err := NewRecoveryPointReadService(
		&recoveryPointEnvironmentReaderStub{}, points, newRecoveryPointCursorTestCipher(),
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.ListRecoveryPoints(context.Background(), testRecoveryPointEnvironmentID, "")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	decoded[len(decoded)-1] ^= 1
	tampered := base64.RawURLEncoding.EncodeToString(decoded)
	_, err = service.ListRecoveryPoints(context.Background(), testRecoveryPointEnvironmentID, tampered)
	if !errors.Is(err, errs.New(errs.KindMalformedRequest, "")) {
		t.Fatalf("tampered cursor error = %v", err)
	}
	if len(points.requests) != 1 {
		t.Fatalf("repository requests after tamper = %#v", points.requests)
	}
}

// Rationale: even authenticated plaintext must use the one canonical JSON
// representation so alternate encodings cannot create multiple cursor forms.
func TestRecoveryPointCursorRejectsNonCanonicalPlaintext(t *testing.T) {
	t.Parallel()
	cipher := newRecoveryPointCursorTestCipher()
	plaintext := []byte(
		`{"e":"env_01J00000000000000000000000","v":1,"l":50,"a":"rp_01J00000000000000000000000","r":41,"o":"recovery_point_id_desc"}`,
	)
	ciphertext, err := cipher.Seal(context.Background(), plaintext)
	if err != nil {
		t.Fatal(err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(ciphertext)
	if _, err := decodeRecoveryPointCursor(context.Background(), cipher, encoded); !errors.Is(
		err,
		errs.New(errs.KindMalformedRequest, ""),
	) {
		t.Fatalf("non-canonical cursor error = %v", err)
	}
}

// Rationale: an authenticated cursor is still scoped to the exact Environment;
// a valid token from another collection owner cannot be replayed.
func TestRecoveryPointReadServiceRejectsCursorForAnotherEnvironment(t *testing.T) {
	t.Parallel()
	points := &recoveryPointRepositoryStub{pages: []etcd.BackupRecoveryPointPage{{
		Revision: 41, NextID: testRecoveryPointID,
	}}}
	service, err := NewRecoveryPointReadService(
		&recoveryPointEnvironmentReaderStub{}, points, newRecoveryPointCursorTestCipher(),
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.ListRecoveryPoints(context.Background(), testRecoveryPointEnvironmentID, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ListRecoveryPoints(
		context.Background(), "env_01J00000000000000000000001", first.NextCursor,
	)
	if !errors.Is(err, errs.New(errs.KindMalformedRequest, "")) {
		t.Fatalf("cross-environment cursor error = %v", err)
	}
}

// Rationale: a missing Environment must stop the read before any Recovery
// Point index is consulted, preserving the public ownership boundary.
func TestRecoveryPointReadServiceDoesNotListMissingEnvironment(t *testing.T) {
	t.Parallel()
	points := &recoveryPointRepositoryStub{}
	service, err := NewRecoveryPointReadService(
		&recoveryPointEnvironmentReaderStub{err: errs.New(
			errs.KindEnvironmentNotFound,
			"environment was not found",
		)},
		points,
		newRecoveryPointCursorTestCipher(),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ListRecoveryPoints(context.Background(), testRecoveryPointEnvironmentID, "")
	if !errors.Is(err, errs.New(errs.KindEnvironmentNotFound, "")) {
		t.Fatalf("missing environment error = %v", err)
	}
	if len(points.requests) != 0 {
		t.Fatalf("repository requests after missing environment = %#v", points.requests)
	}
}
