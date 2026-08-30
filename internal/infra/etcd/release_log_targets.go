package etcd

import (
	"context"
	"errors"
	"sort"
	"strings"

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
	prefix := serviceOwnerPrefix(environmentID)
	start := ""
	targets := make([]EnvironmentLogTarget, 0)
	for {
		page, rangeErr := ledger.store.Range(ctx, RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: MaximumPageLimit, Revision: revision,
		})
		if rangeErr != nil {
			return nil, rangeErr
		}
		if page == nil || page.ReadRevision != revision {
			return nil, errs.New(errs.KindInternal, "Environment Service snapshot changed revision")
		}
		serviceIDs := make([]string, len(page.Values))
		primaryKeys := make([]string, len(page.Values))
		for index, value := range page.Values {
			if validateListKey(prefix, value.Key, ids.KindService) != nil {
				return nil, errs.New(errs.KindInternal, "Environment Service index contains an invalid key")
			}
			serviceID := strings.TrimPrefix(value.Key, prefix)
			if string(value.Value) != serviceID {
				return nil, errs.New(errs.KindInternal, "Environment Service index value is invalid")
			}
			serviceIDs[index] = serviceID
			primaryKeys[index] = serviceKey(serviceID)
		}
		if len(primaryKeys) != 0 {
			primaries, readErr := getManyBatchedAtRevision(ctx, ledger.store, primaryKeys, revision)
			if readErr != nil {
				return nil, readErr
			}
			for index, value := range primaries.Values {
				if value == nil {
					return nil, errs.New(errs.KindInternal, "Environment Service index references a missing record")
				}
				record, decodeErr := decodeServiceRecord(value.Value)
				if decodeErr != nil || record.EnvironmentID != environmentID || record.Desired.ID != serviceIDs[index] {
					return nil, errs.New(errs.KindInternal, "Environment Service snapshot is invalid")
				}
				release, resolveErr := ledger.ResolveServing(ctx, environmentID, record.Desired.ID, revision)
				if errorsIsReleaseNotFound(resolveErr) {
					continue
				}
				if resolveErr != nil {
					return nil, resolveErr
				}
				targets = append(targets, EnvironmentLogTarget{
					ServiceID: record.Desired.ID, ServiceName: record.Desired.Name, ReleaseID: release.Intent.ID,
				})
				if len(targets) > maximumTargets {
					return nil, errs.New(errs.KindStateConflict, "Environment exceeds the log target limit")
				}
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return nil, errs.New(errs.KindInternal, "Environment Service snapshot continuation is empty")
		}
		start = page.Values[len(page.Values)-1].Key
	}
	sort.Slice(targets, func(left, right int) bool {
		return targets[left].ServiceID < targets[right].ServiceID
	})
	return targets, nil
}

func errorsIsReleaseNotFound(err error) bool {
	return errors.Is(err, errs.New(errs.KindReleaseNotFound, ""))
}
