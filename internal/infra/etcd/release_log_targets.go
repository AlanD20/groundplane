package etcd

import (
	"context"
	"errors"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentLogTarget is one Service and serving Release resolved from a
// single Environment MVCC snapshot.
type EnvironmentLogTarget struct {
	ServiceID   string
	ServiceName string
	ReleaseID   string
}

// ResolveEnvironmentLogTargets anchors Environment existence and pages every
// Service at that exact revision before returning only serving Releases.
func (ledger *ReleaseLedger) ResolveEnvironmentLogTargets(
	ctx context.Context,
	environmentID string,
	maximumTargets int,
) ([]EnvironmentLogTarget, error) {
	if ctx == nil || ledger == nil || ledger.store == nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || maximumTargets < 1 {
		return nil, errs.New(errs.KindValidationFailed, "Environment log target snapshot is invalid")
	}
	environmentRead, err := ledger.store.Get(ctx, environmentKey(environmentID))
	if err != nil {
		return nil, err
	}
	if environmentRead == nil || environmentRead.ReadRevision <= 0 {
		return nil, errs.New(errs.KindInternal, "Environment log target anchor read is invalid")
	}
	if environmentRead.Entry == nil {
		return nil, errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	environment, err := decodeEnvironment(environmentRead.Entry.Value)
	if err != nil || environment.ID != environmentID {
		return nil, errs.New(errs.KindInternal, "Environment log target anchor is invalid")
	}

	revision := environmentRead.ReadRevision
	targets := make([]EnvironmentLogTarget, 0)
	projection, found, err := currentEnvironmentProjectionAtRevision(ctx, ledger.store, environmentID, revision)
	if err != nil {
		return nil, err
	}
	if !found {
		return targets, nil
	}
	for _, desired := range projection.Record.DesiredServices {
		if desired.EnvironmentID != environmentID || ids.Validate(ids.KindService, desired.Desired.ID) != nil {
			return nil, errs.New(errs.KindInternal, "Environment applied Service projection is inconsistent")
		}
		release, resolveErr := ledger.ResolveServing(ctx, environmentID, desired.Desired.ID, revision)
		if errorsIsReleaseNotFound(resolveErr) {
			continue
		}
		if resolveErr != nil {
			return nil, resolveErr
		}
		targets = append(targets, EnvironmentLogTarget{
			ServiceID: desired.Desired.ID, ServiceName: desired.Desired.Name, ReleaseID: release.Intent.ID,
		})
		if len(targets) > maximumTargets {
			return nil, errs.New(errs.KindStateConflict, "Environment exceeds the log target limit")
		}
	}
	sort.Slice(targets, func(left, right int) bool {
		return targets[left].ServiceID < targets[right].ServiceID
	})
	return targets, nil
}

func errorsIsReleaseNotFound(err error) bool {
	return errors.Is(err, errs.New(errs.KindReleaseNotFound, ""))
}
