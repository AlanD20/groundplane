package componentaction

import (
	"crypto/sha256"
	"io"
)

type ManagedConfigHeader struct {
	ArtifactID string
	MediaType  string
	Length     uint64
	Digest     [sha256.Size]byte
}

type ManagedConfigPayload struct {
	Header ManagedConfigHeader
	Source io.ReadCloser
}
