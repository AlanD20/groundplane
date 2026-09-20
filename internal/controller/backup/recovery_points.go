package backup

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"io"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	recoveryPointPageLimit     = 50
	recoveryPointCursorVersion = 1
	recoveryPointCursorOrder   = "recovery_point_id_desc"
	recoveryPointMaximumCursor = 2048
)

type recoveryPointEnvironmentReader interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[hierarchyrecord.EnvironmentRecord], error)
}

type recoveryPointRepository interface {
	ListVerifiedRecoveryPointsByEnvironment(
		context.Context,
		string,
		etcd.BackupRecoveryPointPageRequest,
	) (etcd.BackupRecoveryPointPage, error)
}

type recoveryPointCursorCipher interface {
	Seal(context.Context, []byte) ([]byte, error)
	Open(context.Context, []byte) ([]byte, error)
}

// RecoveryPointReadService owns the verified-only public collection and its
// authenticated fixed-revision pagination contract.
type RecoveryPointReadService struct {
	environments recoveryPointEnvironmentReader
	points       recoveryPointRepository
	cursorCipher recoveryPointCursorCipher
}

type recoveryPointCursor struct {
	Version       int    `json:"v"`
	EnvironmentID string `json:"e"`
	Limit         int    `json:"l"`
	AfterID       string `json:"a"`
	Revision      int64  `json:"r"`
	Order         string `json:"o"`
}

func NewRecoveryPointReadService(
	environments recoveryPointEnvironmentReader,
	points recoveryPointRepository,
	cursorCipher recoveryPointCursorCipher,
) (*RecoveryPointReadService, error) {
	if environments == nil || points == nil || cursorCipher == nil {
		return nil, errs.New(errs.KindInternal, "recovery point read dependencies are not configured")
	}
	return &RecoveryPointReadService{
		environments: environments,
		points:       points,
		cursorCipher: cursorCipher,
	}, nil
}

func (service *RecoveryPointReadService) ListRecoveryPoints(
	ctx context.Context,
	environmentID string,
	encodedCursor string,
) (etcd.Page[etcd.BackupRecoveryPointRecord], error) {
	if ctx == nil {
		return etcd.Page[etcd.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindInternal,
			"recovery point list context is required",
		)
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.Page[etcd.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindValidationFailed,
			"recovery point list requires a stable environment id",
		)
	}
	if _, err := service.environments.GetEnvironment(ctx, environmentID); err != nil {
		return etcd.Page[etcd.BackupRecoveryPointRecord]{}, err
	}

	request := etcd.BackupRecoveryPointPageRequest{Limit: recoveryPointPageLimit}
	if encodedCursor != "" {
		cursor, err := decodeRecoveryPointCursor(ctx, service.cursorCipher, encodedCursor)
		if err != nil {
			return etcd.Page[etcd.BackupRecoveryPointRecord]{}, err
		}
		if cursor.EnvironmentID != environmentID || cursor.Limit != recoveryPointPageLimit ||
			cursor.Order != recoveryPointCursorOrder {
			return etcd.Page[etcd.BackupRecoveryPointRecord]{}, invalidRecoveryPointCursor()
		}
		request.AfterID = cursor.AfterID
		request.Revision = cursor.Revision
	}
	page, err := service.points.ListVerifiedRecoveryPointsByEnvironment(ctx, environmentID, request)
	if err != nil {
		return etcd.Page[etcd.BackupRecoveryPointRecord]{}, err
	}
	if page.Revision <= 0 {
		return etcd.Page[etcd.BackupRecoveryPointRecord]{}, errs.New(
			errs.KindInternal,
			"recovery point list returned no fixed revision",
		)
	}
	response := etcd.Page[etcd.BackupRecoveryPointRecord]{
		Items:    page.Items,
		Revision: page.Revision,
	}
	if page.NextID != "" {
		response.NextCursor, err = encodeRecoveryPointCursor(ctx, service.cursorCipher, recoveryPointCursor{
			Version:       recoveryPointCursorVersion,
			EnvironmentID: environmentID,
			Limit:         recoveryPointPageLimit,
			AfterID:       page.NextID,
			Revision:      page.Revision,
			Order:         recoveryPointCursorOrder,
		})
		if err != nil {
			return etcd.Page[etcd.BackupRecoveryPointRecord]{}, err
		}
	}
	return response, nil
}

func encodeRecoveryPointCursor(
	ctx context.Context,
	cipher recoveryPointCursorCipher,
	cursor recoveryPointCursor,
) (string, error) {
	if !validRecoveryPointCursor(cursor) {
		return "", errs.New(errs.KindInternal, "recovery point cursor is invalid")
	}
	plaintext, err := json.Marshal(cursor)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(plaintext)
	ciphertext, err := cipher.Seal(ctx, plaintext)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return "", contextErr
		}
		return "", errs.New(errs.KindInternal, "recovery point cursor encryption failed")
	}
	defer clear(ciphertext)
	encoded := base64.RawURLEncoding.EncodeToString(ciphertext)
	if len(encoded) > recoveryPointMaximumCursor {
		return "", errs.New(errs.KindInternal, "recovery point cursor exceeds the public size limit")
	}
	return encoded, nil
}

func decodeRecoveryPointCursor(
	ctx context.Context,
	cipher recoveryPointCursorCipher,
	encoded string,
) (recoveryPointCursor, error) {
	if len(encoded) == 0 || len(encoded) > recoveryPointMaximumCursor {
		return recoveryPointCursor{}, invalidRecoveryPointCursor()
	}
	ciphertext, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(ciphertext) != encoded {
		clear(ciphertext)
		return recoveryPointCursor{}, invalidRecoveryPointCursor()
	}
	defer clear(ciphertext)
	plaintext, err := cipher.Open(ctx, ciphertext)
	if err != nil {
		clear(plaintext)
		if contextErr := ctx.Err(); contextErr != nil {
			return recoveryPointCursor{}, contextErr
		}
		return recoveryPointCursor{}, invalidRecoveryPointCursor()
	}
	defer clear(plaintext)

	decoder := json.NewDecoder(bytes.NewReader(plaintext))
	decoder.DisallowUnknownFields()
	var cursor recoveryPointCursor
	if err := decoder.Decode(&cursor); err != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		!validRecoveryPointCursor(cursor) {
		return recoveryPointCursor{}, invalidRecoveryPointCursor()
	}
	canonical, err := json.Marshal(cursor)
	if err != nil {
		return recoveryPointCursor{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(canonical)
	if !bytes.Equal(canonical, plaintext) {
		return recoveryPointCursor{}, invalidRecoveryPointCursor()
	}
	return cursor, nil
}

func validRecoveryPointCursor(cursor recoveryPointCursor) bool {
	return cursor.Version == recoveryPointCursorVersion &&
		ids.Validate(ids.KindEnvironment, cursor.EnvironmentID) == nil &&
		cursor.Limit == recoveryPointPageLimit &&
		ids.Validate(ids.KindRecoveryPoint, cursor.AfterID) == nil &&
		cursor.Revision > 0 && cursor.Order == recoveryPointCursorOrder
}

func invalidRecoveryPointCursor() error {
	return errs.New(errs.KindMalformedRequest, "recovery point cursor is invalid")
}
