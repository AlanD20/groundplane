package monitoring

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/internal/infra/monitoringstore"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type HistoryReader interface {
	ReadHost(context.Context, string, time.Time, time.Time, time.Duration) ([]metrics.HostMeasurement, error)
	ReadService(context.Context, string, time.Time, time.Time, time.Duration) ([]metrics.ServiceMeasurement, error)
	ReadWorkloadAggregate(
		context.Context,
		monitoringstore.AggregateScope,
		time.Time,
		time.Time,
		time.Duration,
	) ([]metrics.AggregatePoint, error)
}

func ReadHostHistory(
	ctx context.Context,
	reader HistoryReader,
	hostID string,
	window Window,
) ([]api.MonitoringBucket, error) {
	if err := validateHistoryRequest(ctx, reader, window); err != nil {
		return nil, err
	}
	if hostID == "" {
		return nil, errs.New(errs.KindValidationFailed, "monitoring Host identity is required")
	}
	values, err := reader.ReadHost(ctx, hostID, window.Public.Start, window.Public.End, window.SourceResolution)
	if err != nil {
		return nil, err
	}
	rows := make([]historyRow, 0, len(values))
	for _, value := range values {
		if value.HostID != hostID || !value.Point.At.Equal(value.Bucket.Start) {
			return nil, errs.New(errs.KindStorageUnavailable, "monitoring Host history owner is invalid")
		}
		rows = append(rows, historyRow{
			Bucket: value.Bucket, Summaries: value.Summaries,
			Coverage: metrics.Coverage{Eligible: 1, Sampled: boolCount(value.Available)},
		})
	}
	return groupHistory(window, rows, hostHistoryFields)
}

func ReadServiceHistory(
	ctx context.Context,
	reader HistoryReader,
	owner metrics.Ownership,
	window Window,
) ([]api.MonitoringBucket, error) {
	if err := validateHistoryRequest(ctx, reader, window); err != nil {
		return nil, err
	}
	if err := validateHistoryOwner(owner); err != nil {
		return nil, err
	}
	values, err := reader.ReadService(
		ctx,
		owner.ServiceID,
		window.Public.Start,
		window.Public.End,
		window.SourceResolution,
	)
	if err != nil {
		return nil, err
	}
	rows := make([]historyRow, 0, len(values))
	for _, value := range values {
		if value.Ownership != owner || !value.Point.At.Equal(value.Bucket.Start) {
			return nil, errs.New(errs.KindStorageUnavailable, "monitoring Service history owner is invalid")
		}
		rows = append(rows, historyRow{
			Bucket: value.Bucket, Summaries: value.Summaries,
			Coverage: metrics.Coverage{Eligible: 1, Sampled: boolCount(value.Available)},
		})
	}
	return groupHistory(window, rows, serviceHistoryFields)
}

func ReadAggregateHistory(
	ctx context.Context,
	reader HistoryReader,
	scope monitoringstore.AggregateScope,
	window Window,
) ([]api.MonitoringBucket, error) {
	if err := validateHistoryRequest(ctx, reader, window); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindTenant, scope.TenantID) != nil ||
		scope.ProjectID != "" && ids.Validate(ids.KindProject, scope.ProjectID) != nil ||
		scope.EnvironmentID != "" && (scope.ProjectID == "" ||
			ids.Validate(ids.KindEnvironment, scope.EnvironmentID) != nil) {
		return nil, errs.New(errs.KindValidationFailed, "monitoring aggregate owner is invalid")
	}
	values, err := reader.ReadWorkloadAggregate(
		ctx,
		scope,
		window.Public.Start,
		window.Public.End,
		window.SourceResolution,
	)
	if err != nil {
		return nil, err
	}
	rows := make([]historyRow, 0, len(values))
	for _, value := range values {
		if !value.Point.At.Equal(value.Bucket.Start) {
			return nil, errs.New(errs.KindStorageUnavailable, "monitoring aggregate history time is invalid")
		}
		rows = append(rows, historyRow{Bucket: value.Bucket, Summaries: value.Summaries, Coverage: value.Coverage})
	}
	return groupHistory(window, rows, aggregateHistoryFields)
}

func validateHistoryRequest(ctx context.Context, reader HistoryReader, window Window) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "monitoring history context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if reader == nil {
		return errs.New(errs.KindStorageUnavailable, "monitoring history is unavailable")
	}
	if window.SourceResolution <= 0 || window.GroupSize < 1 || window.Public.Start.IsZero() ||
		window.Public.End.IsZero() || !window.Public.Start.Before(window.Public.End) ||
		time.Duration(window.Public.BucketSeconds)*time.Second !=
			time.Duration(window.GroupSize)*window.SourceResolution ||
		!window.Public.Start.Equal(window.Public.Start.Truncate(window.SourceResolution)) ||
		!window.Public.End.Equal(window.Public.End.Truncate(window.SourceResolution)) {
		return errs.New(errs.KindValidationFailed, "monitoring history window is invalid")
	}
	return nil
}

func validateHistoryOwner(owner metrics.Ownership) error {
	if ids.Validate(ids.KindProject, owner.ProjectID) != nil ||
		ids.Validate(ids.KindEnvironment, owner.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, owner.ServiceID) != nil ||
		owner.Class == metrics.OwnerWorkload && ids.Validate(ids.KindTenant, owner.TenantID) != nil ||
		owner.Class == metrics.OwnerBacking && owner.TenantID != "" ||
		owner.Class != metrics.OwnerWorkload && owner.Class != metrics.OwnerBacking {
		return errs.New(errs.KindValidationFailed, "monitoring Service owner is invalid")
	}
	return nil
}

func boolCount(value bool) uint32 {
	if value {
		return 1
	}
	return 0
}
