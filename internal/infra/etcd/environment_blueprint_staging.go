package etcd

import (
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
)

type environmentBlueprintPublicationEvidence struct {
	seal                blueprints.EnvironmentBlueprintSeal
	rootRevision        int64
	descriptorRevision  int64
	descriptorKey       string
	locatorRevision     int64
	locatorKey          string
	publishedDescriptor []byte
}
