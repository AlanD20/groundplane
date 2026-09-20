package recordcodec

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumEncodedCursorBytes = 2048
	CursorVersion             = 1
	cursorOrder               = "id_asc"
)

type Cursor struct {
	Version  int    `json:"v"`
	Revision int64  `json:"revision"`
	LastID   string `json:"last_id"`
	Query    string `json:"query"`
}

type cursorQuery struct {
	Collection string `json:"collection"`
	OwnerKind  string `json:"owner_kind"`
	OwnerID    string `json:"owner_id"`
	Order      string `json:"order"`
	Limit      int    `json:"limit"`
}

func CursorQueryDigest(collection string, ownerKind string, ownerID string, limit int) (string, error) {
	encoded, err := json.Marshal(cursorQuery{
		Collection: collection,
		OwnerKind:  ownerKind,
		OwnerID:    ownerID,
		Order:      cursorOrder,
		Limit:      limit,
	})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func EncodeCursor(cursor Cursor) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func DecodeCursor(value string) (Cursor, error) {
	if len(value) > maximumEncodedCursorBytes {
		return Cursor{}, errs.New(errs.KindMalformedRequest, "cursor is too long")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return Cursor{}, errs.New(errs.KindMalformedRequest, "cursor is malformed")
	}
	if err := RejectDuplicateFields(decoded); err != nil {
		return Cursor{}, errs.New(errs.KindMalformedRequest, "cursor is malformed")
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	var cursor Cursor
	if err := decoder.Decode(&cursor); err != nil || RequireEOF(decoder) != nil {
		return Cursor{}, errs.New(errs.KindMalformedRequest, "cursor is malformed")
	}
	if cursor.Version != CursorVersion || cursor.Revision <= 0 || cursor.LastID == "" || cursor.Query == "" {
		return Cursor{}, errs.New(errs.KindMalformedRequest, "cursor is malformed")
	}
	return cursor, nil
}
