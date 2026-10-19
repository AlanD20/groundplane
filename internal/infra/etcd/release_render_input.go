package etcd

import (
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
)

type ReleaseTaskRenderInput struct {
	NativePredecessors []BlueprintNativePredecessor
	PublicationID      string
	Operation          releases.ReleaseOperationHead
	Members            []releaserender.ReleaseTaskRenderMember
}
