package hierarchydeletion

import (
	"crypto/sha256"
	"github.com/oklog/ulid/v2"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func hierarchyDeletionStablePrivateID(prefix string, values ...string) string {
	digest := HierarchyDeletionFoldDigest("groundplane-deletion-private-id-v1", values...)
	return prefix + "_" + digest[:32]
}

func HierarchyDeletionStableOperationID(values ...string) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte("groundplane-deletion-child-operation-v1"))
	for _, value := range values {
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write([]byte(value))
	}
	value := digest.Sum(nil)
	var identifier ulid.ULID
	copy(identifier[:], value[:len(identifier)])
	clear(value)
	return string(ids.KindOperation) + "_" + identifier.String()
}
