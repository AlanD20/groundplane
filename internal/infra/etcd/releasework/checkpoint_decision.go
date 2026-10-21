package releasework

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"time"
)

// ForwardCheckpointDecision distinguishes a new effect grant from recording
// supersession. Revocation is committed without the requested checkpoint, so
// the executor cannot mistake it for permission to run that effect.
type ForwardCheckpointDecision struct {
	Change    workqueue.Change
	RevokedAt *time.Time
}
