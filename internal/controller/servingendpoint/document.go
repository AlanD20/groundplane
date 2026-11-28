package servingendpoint

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func decode(payload json.RawMessage) (document, error) {
	var result document
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return document{}, err
	}
	if err := recordcodec.RequireEOF(decoder); err != nil {
		return document{}, err
	}
	return result, nil
}
