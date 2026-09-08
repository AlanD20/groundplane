package controller

import (
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: an initial Blueprint apply must inject retained Environment
// Entries before Compose starts services; otherwise only later Entry mutations
// reach workloads and a clean environment cannot boot.
func TestProjectEnvironmentEntriesInjectsEnvironmentAndFileEntries(t *testing.T) {
	t.Parallel()
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		apiID         = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		workerID      = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		disabledID    = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	)
	volumeDir := filepath.Join("/var/lib/groundplane/vol", environmentID)
	project := &composetypes.Project{Services: composetypes.Services{
		"api": {
			Name:     "api",
			EnvFiles: []composetypes.EnvFile{{Path: "/authored.env", Required: true}},
		},
		"worker": {Name: "worker"},
	}, DisabledServices: composetypes.Services{"maintenance": {Name: "maintenance"}}}
	entries := []etcd.EntryRecord{
		entryComposeTestRecord(t, environmentID, "APP_ENV", "", []string{"all"}, false, nil, nil),
		entryComposeTestRecord(t, environmentID, "TOKEN", "", []string{"api"}, true, nil, nil),
		entryComposeTestRecord(t, environmentID, "MAINTENANCE", "", []string{"maintenance"}, false, nil, nil),
		entryComposeTestRecord(
			t,
			environmentID,
			"",
			"etc/ssl/app.pem",
			[]string{"api"},
			true,
			uint32Pointer(82),
			uint32Pointer(82),
		),
	}

	projection, err := ProjectEnvironmentEntries(
		project,
		environmentID,
		volumeDir,
		[]ComposeResourceIdentity{
			{ID: apiID, Name: "api"}, {ID: workerID, Name: "worker"}, {ID: disabledID, Name: "maintenance"},
		},
		entries,
	)
	if err != nil {
		t.Fatalf("ProjectEnvironmentEntries() error = %v", err)
	}
	if len(projection.Materializations) != 4 {
		t.Fatalf("materializations = %#v", projection.Materializations)
	}
	byDestination := make(map[string]EnvironmentEntryMaterialization, len(projection.Materializations))
	for _, materialization := range projection.Materializations {
		byDestination[materialization.Destination] = materialization
	}
	canonicalDestination := EnvFileName(environmentID)
	canonical := byDestination[canonicalDestination]
	if canonical.Source.GeneratedEnvironment == nil ||
		len(canonical.Source.GeneratedEnvironment.Values) != 1 ||
		canonical.Source.GeneratedEnvironment.Values[0].Name != "APP_ENV" ||
		canonical.Source.GeneratedEnvironment.Values[0].Value.Storage != etcd.TaskEntryValueStoragePlain {
		t.Fatalf("canonical Environment materialization = %#v", canonical)
	}
	serviceDestination := ServiceEnvFileName(environmentID, "api")
	serviceEnvironment := byDestination[serviceDestination]
	if serviceEnvironment.ServiceID != apiID || serviceEnvironment.ServiceName != "api" ||
		serviceEnvironment.Source.GeneratedEnvironment == nil ||
		len(serviceEnvironment.Source.GeneratedEnvironment.Values) != 1 ||
		serviceEnvironment.Source.GeneratedEnvironment.Values[0].Name != "TOKEN" ||
		serviceEnvironment.Source.GeneratedEnvironment.Values[0].Value.Storage != etcd.TaskEntryValueStorageSecret {
		t.Fatalf("service Environment materialization = %#v", serviceEnvironment)
	}
	file := byDestination["etc/ssl/app.pem"]
	if file.OutputKind != etcd.TaskMaterializationOutputSecretFile ||
		file.Mode != entrymaterialization.ModePrivate || file.UID != 82 || file.GID != 82 ||
		file.Source.EntryValue == nil || file.Source.EntryValue.Storage != etcd.TaskEntryValueStorageSecret {
		t.Fatalf("file Entry materialization = %#v", file)
	}

	canonicalPath := filepath.Join(volumeDir, filepath.FromSlash(canonicalDestination))
	servicePath := filepath.Join(volumeDir, filepath.FromSlash(serviceDestination))
	api := projection.Project.Services["api"]
	if len(api.EnvFiles) != 3 || api.EnvFiles[0].Path != canonicalPath ||
		api.EnvFiles[1].Path != "/authored.env" || api.EnvFiles[2].Path != servicePath {
		t.Fatalf("api env files = %#v", api.EnvFiles)
	}
	if len(api.Volumes) != 1 || api.Volumes[0].Source != filepath.Join(volumeDir, "etc/ssl/app.pem") ||
		api.Volumes[0].Target != "/etc/ssl/app.pem" || !api.Volumes[0].ReadOnly {
		t.Fatalf("api file mounts = %#v", api.Volumes)
	}
	worker := projection.Project.Services["worker"]
	if len(worker.EnvFiles) != 1 || worker.EnvFiles[0].Path != canonicalPath || len(worker.Volumes) != 0 {
		t.Fatalf("worker projection = %#v", worker)
	}
	disabled := projection.Project.DisabledServices["maintenance"]
	disabledPath := filepath.Join(volumeDir, filepath.FromSlash(ServiceEnvFileName(environmentID, "maintenance")))
	if len(disabled.EnvFiles) != 2 || disabled.EnvFiles[0].Path != canonicalPath ||
		disabled.EnvFiles[1].Path != disabledPath {
		t.Fatalf("disabled Service env files = %#v", disabled.EnvFiles)
	}
	if len(project.Services["api"].EnvFiles) != 1 || len(project.Services["api"].Volumes) != 0 {
		t.Fatal("ProjectEnvironmentEntries() mutated its input")
	}
}

func entryComposeTestRecord(
	t *testing.T,
	environmentID string,
	key string,
	path string,
	exposure []string,
	secret bool,
	uid *uint32,
	gid *uint32,
) etcd.EntryRecord {
	t.Helper()
	kind := core.EntryKindEnv
	if path != "" {
		kind = core.EntryKindFile
	}
	record, err := etcd.NewEntryRecord(environmentID, core.EnvEntry{
		ID: ids.New(ids.KindEnvEntry), Kind: kind, Key: key, Path: path,
		UID: uid, GID: gid, Source: core.EntrySource{Kind: core.SourceLiteral},
		Exposure: exposure, Secret: secret,
	}, ids.New(ids.KindConfig))
	if err != nil {
		t.Fatalf("etcd.NewEntryRecord() error = %v", err)
	}
	return record
}
