// Package backupservicefact owns exact byte encodings used by both Controller
// authority sealing and Agent runtime observations.
package backupservicefact

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LabelsDigest commits to a sorted, unique list of complete label pairs.
// Four-byte length prefixes avoid ambiguity in arbitrary label values.
func LabelsDigest(pairs []*agentpb.LabelPair) ([]byte, error) {
	if len(pairs) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "backup Service labels are empty")
	}
	hasher := sha256.New()
	previous := ""
	for _, pair := range pairs {
		if pair == nil || pair.GetKey() <= previous {
			return nil, errs.New(errs.KindValidationFailed, "backup Service labels are not sorted and unique")
		}
		previous = pair.GetKey()
		writePart(hasher, pair.GetKey())
		writePart(hasher, pair.GetValue())
	}
	return hasher.Sum(nil), nil
}

func writePart(hasher hash.Hash, value string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write([]byte(value))
}
