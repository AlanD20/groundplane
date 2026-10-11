package localagent

import (
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// acceptedBuild captures the matching authenticated Ready report before its
// lifecycle CAS. A closed readiness subscription alone may outlive its session.
func (manager *Manager) acceptedBuild(record Record) (buildversion.BuildIdentity, error) {
	snapshot, ok := manager.sessions.Snapshot(record.ID)
	if !ok || snapshot.Generation != record.Generation || !snapshot.Online || snapshot.Revoked ||
		snapshot.LastReady.IsZero() || manager.clock.Now().After(snapshot.LastReady.Add(StaleWindow(record.Config.PullIntervalSeconds))) {
		return buildversion.BuildIdentity{}, errs.New(
			errs.KindStateConflict,
			"agent Ready session changed before its build identity was accepted",
		)
	}
	build := buildversion.BuildIdentity{Version: snapshot.Version, Source: snapshot.Source}
	if err := build.Validate(); err != nil {
		return buildversion.BuildIdentity{}, err
	}
	return build, nil
}
