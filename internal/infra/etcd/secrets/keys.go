package secrets

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/core"
)

const (
	secretOwnerIndexPrefix = "/v1/indexes/secrets/by-owner/"
	secretKeyIndexPrefix   = "/v1/indexes/secrets/by-key/"
)

func SecretScopeKey(scope core.SecretScope, projectID string) (string, string) {
	if scope == core.SecretScopeProject {
		return "project", projectID
	}
	return "platform", "-"
}

func SecretOwnerCollectionPrefix(scope core.SecretScope, projectID string) string {
	kind, id := SecretScopeKey(scope, projectID)
	return secretOwnerIndexPrefix + kind + "/" + id + "/"
}

func SecretOwnerKey(secret core.Secret) string {
	return SecretOwnerCollectionPrefix(secret.Scope, secret.ProjectID) + secret.ID
}

func SecretKeyIndexKey(scope core.SecretScope, projectID string, key string) string {
	kind, id := SecretScopeKey(scope, projectID)
	return secretKeyIndexPrefix + kind + "/" + id + "/" + recordcodec.EncodeKeySegment(key)
}

func SecretScopedKey(secret core.Secret) string {
	return SecretKeyIndexKey(secret.Scope, secret.ProjectID, secret.Key)
}
