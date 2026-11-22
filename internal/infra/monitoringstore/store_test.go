package monitoringstore

import (
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: monitoring history is derived but must survive Controller restart;
// losing the file or its host/service distinction would silently erase charts.
func TestStorePersistsServiceAndHostAcrossReopen(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "monitoring.sqlite")
	store := openTestStore(t, path, 0)
	at := time.Date(2026, 11, 22, 12, 0, 15, 0, time.UTC)
	cpu, volumeBytes := 1.25, uint64(4_096)
	service := testMeasurement("service-a", monitoring.OwnerWorkload, at, &cpu, "volume-a", &volumeBytes)
	if err := store.Append(ctx, service); err != nil {
		t.Fatal(err)
	}
	ratio, memory, total := 0.5, uint64(4_000), uint64(8_000)
	if err := store.AppendHost(ctx, monitoring.HostMeasurement{
		HostID: "host-a", Available: true,
		Point: monitoring.MetricPoint{
			At: at, CPUCores: &cpu, CPUUtilizationRatio: &ratio,
			MemoryUsedBytes: &memory, MemoryTotalBytes: &total,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, path, 0)
	defer store.Close()
	services, err := store.ReadService(
		ctx,
		"service-a",
		at.Truncate(time.Minute),
		at.Truncate(time.Minute).Add(time.Minute),
		monitoring.SampleInterval,
	)
	if err != nil || len(services) != 1 || services[0].Point.CPUCores == nil || *services[0].Point.CPUCores != cpu ||
		len(services[0].Volumes) != 1 || services[0].Volumes[0].UsedBytes == nil ||
		*services[0].Volumes[0].UsedBytes != volumeBytes ||
		!services[0].Bucket.Start.Equal(at.Truncate(monitoring.SampleInterval)) ||
		!services[0].Bucket.End.Equal(at.Truncate(monitoring.SampleInterval).Add(monitoring.SampleInterval)) ||
		services[0].Summaries.CPUCores.Minimum == nil || *services[0].Summaries.CPUCores.Minimum != cpu ||
		services[0].Summaries.CPUCores.Valid != 1 || services[0].Summaries.CPUCores.Expected != 1 {
		t.Fatalf("service history did not survive reopen: values=%+v err=%v", services, err)
	}
	hosts, err := store.ReadHost(
		ctx,
		"host-a",
		at.Truncate(time.Minute),
		at.Truncate(time.Minute).Add(time.Minute),
		monitoring.SampleInterval,
	)
	if err != nil || len(hosts) != 1 || hosts[0].Point.CPUUtilizationRatio == nil ||
		*hosts[0].Point.CPUUtilizationRatio != ratio || hosts[0].Point.MemoryTotalBytes == nil ||
		*hosts[0].Point.MemoryTotalBytes != total {
		t.Fatalf("Host history did not survive reopen: values=%+v err=%v", hosts, err)
	}
}

// Rationale: tier rollup and expiry must preserve a gauge average while
// bounding old rows; otherwise the 7-day store either grows or changes meaning.
func TestStoreRollsAndExpiresHistory(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t, filepath.Join(t.TempDir(), "monitoring.sqlite"), 0)
	defer store.Close()
	base := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	for index, value := range []float64{1, 2, 3} {
		at := base.Add(time.Duration(index) * monitoring.SampleInterval)
		if err := store.Append(ctx, testMeasurement("service-a", monitoring.OwnerWorkload, at, &value, "", nil)); err != nil {
			t.Fatal(err)
		}
		ratio := value / 10
		if err := store.AppendHost(ctx, monitoring.HostMeasurement{
			HostID: "host-a", Point: monitoring.MetricPoint{At: at, CPUCores: &value, CPUUtilizationRatio: &ratio},
			Available: true,
		}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.Maintain(ctx, base.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	medium, err := store.ReadService(ctx, "service-a", base, base.Add(time.Minute), monitoring.MediumResolution)
	if err != nil || len(medium) != 1 || medium[0].Point.CPUCores == nil || *medium[0].Point.CPUCores != 2 ||
		medium[0].Summaries.CPUCores.Average == nil || *medium[0].Summaries.CPUCores.Average != 2 ||
		medium[0].Summaries.CPUCores.Minimum == nil || *medium[0].Summaries.CPUCores.Minimum != 1 ||
		medium[0].Summaries.CPUCores.Maximum == nil || *medium[0].Summaries.CPUCores.Maximum != 3 ||
		medium[0].Summaries.CPUCores.Valid != 3 || medium[0].Summaries.CPUCores.Expected != 4 ||
		result.RolledServiceRows == 0 {
		t.Fatalf("fine history did not roll up: result=%+v values=%+v err=%v", result, medium, err)
	}
	hosts, err := store.ReadHost(ctx, "host-a", base, base.Add(time.Minute), monitoring.MediumResolution)
	if err != nil || len(hosts) != 1 || hosts[0].Point.CPUUtilizationRatio == nil ||
		math.Abs(*hosts[0].Point.CPUUtilizationRatio-0.2) > 0.000001 || result.RolledHostRows == 0 {
		t.Fatalf("Host history did not roll up: result=%+v values=%+v err=%v", result, hosts, err)
	}
	if _, err = store.Maintain(ctx, base.Add(10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	coarse, err := store.ReadService(
		ctx,
		"service-a",
		base,
		base.Add(monitoring.CoarseResolution),
		monitoring.CoarseResolution,
	)
	if err != nil || len(coarse) != 1 || coarse[0].Point.CPUCores == nil || *coarse[0].Point.CPUCores != 2 {
		t.Fatalf("medium history did not roll up completely: values=%+v err=%v", coarse, err)
	}
	if _, err = store.Maintain(ctx, base.Add(monitoring.FineRetention+30*time.Second)); err != nil {
		t.Fatal(err)
	}
	medium, err = store.ReadService(ctx, "service-a", base, base.Add(time.Minute), monitoring.MediumResolution)
	if err != nil || len(medium) != 1 || medium[0].Point.CPUCores == nil || *medium[0].Point.CPUCores != 2 ||
		medium[0].Summaries.CPUCores.Minimum == nil || *medium[0].Summaries.CPUCores.Minimum != 1 ||
		medium[0].Summaries.CPUCores.Maximum == nil || *medium[0].Summaries.CPUCores.Maximum != 3 {
		t.Fatalf("partial fine expiry rewrote settled medium history: values=%+v err=%v", medium, err)
	}
	if _, err = store.Maintain(ctx, base.Add(monitoring.MediumRetention+10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	coarse, err = store.ReadService(
		ctx,
		"service-a",
		base,
		base.Add(monitoring.CoarseResolution),
		monitoring.CoarseResolution,
	)
	if err != nil || len(coarse) != 1 || coarse[0].Point.CPUCores == nil || *coarse[0].Point.CPUCores != 2 {
		t.Fatalf("partial medium expiry rewrote settled coarse history: values=%+v err=%v", coarse, err)
	}
	if _, err = store.Maintain(ctx, base.Add(8*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	coarse, err = store.ReadService(
		ctx,
		"service-a",
		base,
		base.Add(monitoring.CoarseResolution),
		monitoring.CoarseResolution,
	)
	if err != nil || len(coarse) != 0 {
		t.Fatalf("expired history remains: values=%+v err=%v", coarse, err)
	}
}

// Rationale: parent queries must not count platform-owned backing usage and
// must deduplicate one stable Volume mounted by multiple workload Services.
func TestStoreWorkloadAggregateDeduplicatesVolumeAndExcludesBacking(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t, filepath.Join(t.TempDir(), "monitoring.sqlite"), 0)
	defer store.Close()
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	one, two := 1.0, 2.0
	shared, backing := uint64(100), uint64(10_000)
	measurements := []monitoring.ServiceMeasurement{
		testMeasurement("service-a", monitoring.OwnerWorkload, at, &one, "volume-shared", &shared),
		testMeasurement("service-b", monitoring.OwnerWorkload, at, &two, "volume-shared", &shared),
		testMeasurement("backing-a", monitoring.OwnerBacking, at, &two, "volume-backing", &backing),
	}
	if err := store.AppendBatch(ctx, measurements); err != nil {
		t.Fatal(err)
	}
	aggregates, err := store.ReadWorkloadAggregate(
		ctx,
		AggregateScope{TenantID: "tenant-a"},
		at,
		at.Add(monitoring.SampleInterval),
		monitoring.SampleInterval,
	)
	if err != nil || len(aggregates) != 1 || aggregates[0].Point.CPUCores == nil ||
		*aggregates[0].Point.CPUCores != 3 || aggregates[0].Point.PersistentVolumeUsedBytes == nil ||
		*aggregates[0].Point.PersistentVolumeUsedBytes != shared || aggregates[0].Coverage.Eligible != 2 ||
		aggregates[0].Summaries.CPUCores.Minimum == nil || *aggregates[0].Summaries.CPUCores.Minimum != 3 {
		t.Fatalf("unexpected stored aggregate: values=%+v err=%v", aggregates, err)
	}
}

func TestEnvironmentRollupIsolatedFromSiblingAndBacking(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "monitoring.sqlite")
	store := openTestStore(t, path, 0)
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	prior := at.Add(-monitoring.SampleInterval)
	previousCPU := 5.0
	if err := store.AppendBatch(ctx, []monitoring.ServiceMeasurement{
		testMeasurement("previous-workload", monitoring.OwnerWorkload, prior, &previousCPU, "", nil),
	}); err != nil {
		t.Fatal(err)
	}
	// A previously installed store has the existing scope tables and version,
	// with no Environment rollup table.
	if _, err := store.database.ExecContext(ctx, `DROP TABLE monitoring_environment_samples`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, path, 0)
	defer store.Close()
	project := AggregateScope{TenantID: "tenant-a", ProjectID: "project-a"}
	preserved, err := store.ReadWorkloadAggregate(ctx, project, prior, at, monitoring.SampleInterval)
	if err != nil || len(preserved) != 1 || preserved[0].Point.CPUCores == nil ||
		*preserved[0].Point.CPUCores != previousCPU {
		t.Fatalf("existing Project samples changed: samples=%v error=%v", preserved, err)
	}
	shared, backingStorage := uint64(100), uint64(10_000)
	siblingCPU, backingCPU := 90.0, 100.0
	for index, cpu := range [][2]float64{{9, 1}, {2, 7}} {
		sampled := at.Add(time.Duration(index) * monitoring.SampleInterval)
		sibling := testMeasurement("sibling", monitoring.OwnerWorkload, sampled, &siblingCPU, "", nil)
		sibling.Ownership.EnvironmentID = "environment-b"
		if err := store.AppendBatch(ctx, []monitoring.ServiceMeasurement{
			testMeasurement("service-a", monitoring.OwnerWorkload, sampled, &cpu[0], "shared", &shared),
			testMeasurement("service-b", monitoring.OwnerWorkload, sampled, &cpu[1], "shared", &shared),
			sibling,
			testMeasurement("backing", monitoring.OwnerBacking, sampled, &backingCPU, "backing", &backingStorage),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Maintain(ctx, at.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	scope := AggregateScope{TenantID: "tenant-a", ProjectID: "project-a", EnvironmentID: "environment-a"}
	samples, err := store.ReadWorkloadAggregate(ctx, scope, at, at.Add(time.Minute), monitoring.MediumResolution)
	if err != nil || len(samples) != 1 {
		t.Fatalf("Environment rollup: samples=%v error=%v", samples, err)
	}
	point := samples[0]
	if cpu := point.Summaries.CPUCores; cpu.Average == nil || cpu.Minimum == nil || cpu.Maximum == nil ||
		*cpu.Average != 9.5 || *cpu.Minimum != 9 || *cpu.Maximum != 10 || cpu.ContributorValid != 4 {
		t.Fatalf("Environment CPU must use aligned owned samples: %+v", cpu)
	}
	if point.Point.PersistentVolumeUsedBytes == nil || *point.Point.PersistentVolumeUsedBytes != shared ||
		point.Coverage.VolumeEligible != 2 {
		t.Fatalf("Environment shared Volume was counted incorrectly: %+v", point)
	}
	scope.ProjectID = "other-project"
	if foreign, err := store.ReadWorkloadAggregate(ctx, scope, at, at.Add(time.Minute), monitoring.MediumResolution); err != nil ||
		len(foreign) != 0 {
		t.Fatalf("Environment history escaped its Project: samples=%v error=%v", foreign, err)
	}
}

// Rationale: parent extrema must come from synchronized totals. Summing child
// maxima invents a peak when different Services peak in different samples.
func TestAppendBatchPersistsSynchronizedParentExtremaAndCoverage(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t, filepath.Join(t.TempDir(), "monitoring.sqlite"), 0)
	defer store.Close()
	base := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	shared, backingVolume := uint64(100), uint64(10_000)
	for index, values := range [][2]float64{{10, 1}, {2, 8}} {
		at := base.Add(time.Duration(index) * monitoring.SampleInterval)
		serviceA := testMeasurement("service-a", monitoring.OwnerWorkload, at, &values[0], "shared", &shared)
		serviceB := testMeasurement("service-b", monitoring.OwnerWorkload, at, &values[1], "shared", &shared)
		network := float64(50 + index)
		serviceA.Point.NetworkReceiveBytesPerSecond = &network
		backingCPU := 100.0
		backing := testMeasurement("backing-a", monitoring.OwnerBacking, at, &backingCPU, "backing", &backingVolume)
		if err := store.AppendBatch(ctx, []monitoring.ServiceMeasurement{serviceA, serviceB, backing}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Maintain(ctx, base.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	parents, err := store.ReadWorkloadAggregate(
		ctx,
		AggregateScope{TenantID: "tenant-a"},
		base,
		base.Add(time.Minute),
		monitoring.MediumResolution,
	)
	if err != nil || len(parents) != 1 {
		t.Fatalf("parent history unavailable: values=%+v err=%v", parents, err)
	}
	parent := parents[0]
	if parent.Summaries.CPUCores.Average == nil || *parent.Summaries.CPUCores.Average != 10.5 ||
		parent.Summaries.CPUCores.Minimum == nil || *parent.Summaries.CPUCores.Minimum != 10 ||
		parent.Summaries.CPUCores.Maximum == nil || *parent.Summaries.CPUCores.Maximum != 11 ||
		parent.Summaries.CPUCores.Valid != 2 || parent.Summaries.CPUCores.Expected != 4 ||
		parent.Summaries.CPUCores.ContributorValid != 4 ||
		parent.Summaries.CPUCores.ContributorExpected != 4 {
		t.Fatalf("parent CPU statistics are not synchronized: %+v", parent)
	}
	if parent.Summaries.NetworkReceiveBytesPerSecond.ContributorValid != 2 ||
		parent.Summaries.NetworkReceiveBytesPerSecond.ContributorExpected != 4 ||
		parent.Point.PersistentVolumeUsedBytes == nil || *parent.Point.PersistentVolumeUsedBytes != shared ||
		parent.Summaries.PersistentVolumeUsedBytes.Minimum == nil ||
		*parent.Summaries.PersistentVolumeUsedBytes.Minimum != float64(shared) ||
		parent.Coverage.Eligible != 4 || parent.Coverage.Sampled != 4 {
		t.Fatalf("parent partial or Volume coverage is wrong: %+v", parent)
	}
	serviceA, err := store.ReadService(ctx, "service-a", base, base.Add(time.Minute), monitoring.MediumResolution)
	if err != nil || len(serviceA) != 1 || serviceA[0].Summaries.CPUCores.Maximum == nil {
		t.Fatalf("first child history unavailable: values=%+v err=%v", serviceA, err)
	}
	serviceB, err := store.ReadService(ctx, "service-b", base, base.Add(time.Minute), monitoring.MediumResolution)
	if err != nil || len(serviceB) != 1 || serviceB[0].Summaries.CPUCores.Maximum == nil {
		t.Fatalf("second child history unavailable: values=%+v err=%v", serviceB, err)
	}
	if *parent.Summaries.CPUCores.Maximum ==
		*serviceA[0].Summaries.CPUCores.Maximum+*serviceB[0].Summaries.CPUCores.Maximum {
		t.Fatal("parent maximum was manufactured from child maxima")
	}
	projects, err := store.ReadWorkloadAggregate(
		ctx,
		AggregateScope{TenantID: "tenant-a", ProjectID: "project-a"},
		base,
		base.Add(time.Minute),
		monitoring.MediumResolution,
	)
	if err != nil || len(projects) != 1 || projects[0].Summaries.CPUCores.Maximum == nil ||
		*projects[0].Summaries.CPUCores.Maximum != 11 {
		t.Fatalf("Project synchronized history is wrong: values=%+v err=%v", projects, err)
	}
}

func TestAppendBatchRollsBackEveryRowWhenOneMeasurementIsInvalid(t *testing.T) {
	ctx := t.Context()
	store := openTestStore(t, filepath.Join(t.TempDir(), "monitoring.sqlite"), 0)
	defer store.Close()
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	cpu := 1.0
	valid := testMeasurement("service-a", monitoring.OwnerWorkload, at, &cpu, "", nil)
	tooLarge := ^uint64(0)
	invalid := testMeasurement("service-b", monitoring.OwnerWorkload, at, &cpu, "volume-too-large", &tooLarge)
	if err := store.AppendBatch(ctx, []monitoring.ServiceMeasurement{valid, invalid}); err == nil {
		t.Fatal("batch with an unstoreable metric was accepted")
	}
	values, err := store.ReadService(ctx, "service-a", at, at.Add(monitoring.SampleInterval), monitoring.SampleInterval)
	if err != nil || len(values) != 0 {
		t.Fatalf("invalid batch persisted a prefix: values=%+v err=%v", values, err)
	}
}

// Rationale: the database page ceiling and managed WAL target must fail closed
// before monitoring history can consume unbounded host storage.
func TestStoreHonorsSmallDatabaseCeiling(t *testing.T) {
	ctx := t.Context()
	const maximumBytes = 128 << 10
	store := openTestStore(t, filepath.Join(t.TempDir(), "monitoring.sqlite"), maximumBytes)
	defer store.Close()
	base := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	value := 1.0
	var capacityErr error
	for index := range 1_000 {
		measurement := testMeasurement(
			"service-"+strings.Repeat("x", 512)+time.Duration(index).String(),
			monitoring.OwnerWorkload,
			base.Add(time.Duration(index)*monitoring.SampleInterval),
			&value,
			"",
			nil,
		)
		if capacityErr = store.Append(ctx, measurement); capacityErr != nil {
			break
		}
	}
	if capacityErr == nil || !errors.Is(capacityErr, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("small database ceiling did not fail closed: %v", capacityErr)
	}
	usage, err := store.Usage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if usage.DatabaseBytes > maximumBytes || usage.MaximumDatabaseBytes != maximumBytes {
		t.Fatalf("database exceeded ceiling: %+v", usage)
	}
}

func openTestStore(t *testing.T, path string, maximumBytes int64) *Store {
	t.Helper()
	store, err := Open(t.Context(), Options{
		Path: path, MaximumDatabaseBytes: maximumBytes, WALTargetBytes: 32 << 10, CacheBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testMeasurement(
	serviceID string,
	class monitoring.OwnerClass,
	at time.Time,
	cpu *float64,
	volumeID string,
	used *uint64,
) monitoring.ServiceMeasurement {
	measurement := monitoring.ServiceMeasurement{
		Ownership: monitoring.Ownership{
			TenantID: "tenant-a", ProjectID: "project-a", EnvironmentID: "environment-a",
			ServiceID: serviceID, Class: class,
		},
		Point: monitoring.MetricPoint{At: at, CPUCores: cpu}, Available: true,
	}
	if volumeID != "" {
		measurement.Volumes = []monitoring.VolumeUsage{{VolumeID: volumeID, UsedBytes: used}}
	}
	if class == monitoring.OwnerBacking {
		measurement.Ownership.TenantID = ""
	}
	return measurement
}
