package blueprint

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testblueprintplanning "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintplanning"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type environmentBlueprintBackupRepositoryStub struct {
	input       testblueprintplanning.EnvironmentBlueprintBackupPolicyInput
	snapshot    environmentBlueprintBackupPolicySnapshot
	validateErr error
	snapshotErr error
}

type environmentBlueprintAttachRepositoryStub struct {
	environmentBlueprintRepository
	backingProject     testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	backingEnvironment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	backingService     testkeyvalue.Versioned[testservices.ServiceRecord]
}

func (stub *environmentBlueprintAttachRepositoryStub) ResolveBackingProject(
	context.Context,
	string,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return stub.backingProject, nil
}

func (stub *environmentBlueprintAttachRepositoryStub) ResolveEnvironment(
	context.Context,
	string,
	string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return stub.backingEnvironment, nil
}

func (stub *environmentBlueprintAttachRepositoryStub) ListServices(
	context.Context,
	string, testkeyvalue.PageRequest,

) (testkeyvalue.Page[testservices.ServiceRecord], error) {
	return testkeyvalue.Page[testservices.ServiceRecord]{
		Items:    []testkeyvalue.Versioned[testservices.ServiceRecord]{stub.backingService},
		Revision: stub.backingService.ReadRevision,
	}, nil
}

func (stub *environmentBlueprintBackupRepositoryStub) ValidateEnvironmentBlueprintBackupPolicy(
	_ context.Context,
	input testblueprintplanning.EnvironmentBlueprintBackupPolicyInput,
) error {
	stub.input = input
	return stub.validateErr
}

func (stub *environmentBlueprintBackupRepositoryStub) PrepareEnvironmentBlueprintBackupPolicy(
	_ context.Context,
	input testblueprintplanning.EnvironmentBlueprintBackupPolicyInput,
) (testblueprintplanning.BlueprintBackupPolicyPreparation, error) {
	stub.input = input
	return testblueprintplanning.BlueprintBackupPolicyPreparation{}, nil
}

func (stub *environmentBlueprintBackupRepositoryStub) GetEnvironmentBlueprintBackupPolicySnapshot(
	_ context.Context,
	_ string,
	_ int64,
) (environmentBlueprintBackupPolicySnapshot, error) {
	return stub.snapshot, stub.snapshotErr
}

// Rationale: omission retains the effective durable policy, while presence
// resolves every authored resource label to a stable candidate identity.
func TestEnvironmentBlueprintBackupOmissionAndPresentLabelResolution(t *testing.T) {
	now := time.Date(2026, 9, 2, 15, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	taskID := ids.NewAt(ids.KindTask, now, 2)
	volumeID := ids.NewAt(ids.KindVolume, now, 3)
	attachID := ids.NewAt(ids.KindAttach, now, 4)
	sourceID := ids.NewAt(ids.KindBackupSource, now, 5)
	stub := &environmentBlueprintBackupRepositoryStub{snapshot: environmentBlueprintBackupPolicySnapshot{
		found: true,
		policy: testbackuppolicy.BackupPolicyRecord{
			EnvironmentID: environmentID, Enabled: false, Frequency: "*-*-* 02:00:00",
			Keep: 2, Encryption: "none", ConnectorID: ids.NewAt(ids.KindConnector, now, 6),
			SourceIDs: []string{sourceID}, UpdatedAt: now,
		},
		sources: []testbackuppolicy.BackupSourceRecord{{
			ID: sourceID, EnvironmentID: environmentID, Kind: core.BackupSourceConfig,
			TargetID: environmentID, CreatedAt: now,
		}},
	}}
	service := &Service{backups: stub}
	omitted, preparation, err := service.prepareEnvironmentBlueprintBackup(
		context.Background(),
		environmentID,
		taskID,
		19,
		nil,
		testenvironmentprojection.EnvironmentComposeProjection{},
		preparedBlueprintAttaches{},
		nil,
		now,
	)
	if err != nil || omitted == nil || !stub.input.Retain ||
		omitted.Sources[0].ID != sourceID || omitted.ConnectorID != stub.snapshot.policy.ConnectorID {
		t.Fatalf("omitted Backup = %#v, preparation zero = %t, error = %v", omitted, preparation.IsZero(), err)
	}
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, RevisionID: taskID,
		Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{ID: volumeID, Slug: "archive", Key: "archive"}},
	}
	attaches := preparedBlueprintAttaches{effective: []testkeyvalue.Versioned[testattachments.Record]{{
		Record: testattachments.Record{
			ID:                 attachID,
			EnvironmentID:      environmentID,
			Name:               "database",
			CredentialAttachID: attachID,
		},
	}}}
	_, _, err = service.prepareEnvironmentBlueprintBackup(
		context.Background(), environmentID, taskID, 19,
		&core.BackupSpec{
			Enabled: true, Frequency: "*-*-* 02:00:00", Keep: 2, Encryption: "age",
			Connector: "archive-store", Sources: []core.BackupSourceSpec{
				{Kind: core.BackupSourceConfig},
				{Kind: core.BackupSourceVolume, Ref: "archive"},
				{Kind: core.BackupSourceAttach, Ref: "database"},
			},
		},
		projection, attaches, func(kind ids.Kind, purpose string) string {
			return ids.DeriveAt(kind, now, taskID, purpose)
		}, now,
	)
	if err != nil {
		t.Fatalf("prepareEnvironmentBlueprintBackup(present) error = %v", err)
	}
	if got := stub.input.Sources; len(got) != 3 || got[0].TargetID != environmentID ||
		got[1].TargetID != volumeID || got[2].TargetID != attachID || stub.input.ConnectorName != "archive-store" {
		t.Fatalf("resolved Backup input = %#v", stub.input)
	}
}

func TestEnvironmentBlueprintAttachPreparationUsesRetainedCredentialOwner(t *testing.T) {
	registerAdapters()
	now := time.Date(2026, 9, 2, 17, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 91)
	consumerID := ids.NewAt(ids.KindService, now, 92)
	backingProjectID := ids.NewAt(ids.KindProject, now, 93)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 94)
	backingServiceID := ids.NewAt(ids.KindService, now, 95)
	backingNetworkID := ids.NewAt(ids.KindNetwork, now, 96)
	taskID := ids.NewAt(ids.KindTask, now, 97)
	ownerID := ids.NewAt(ids.KindAttach, now, 98)
	ownerTaskID := ids.NewAt(ids.KindTask, now, 99)
	candidateID := ids.NewAt(ids.KindAttach, now, 100)
	backingProject := testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
		Record: testhierarchy.ProjectRecord{
			ID:   backingProjectID,
			Slug: "database",
			Kind: testhierarchy.ProjectKindBacking,
		},
		Revision: 21, ReadRevision: 41,
	}
	backingEnvironment := testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
		Record: testhierarchy.EnvironmentRecord{
			ID: backingEnvironmentID, ProjectID: backingProjectID, Name: "main",
			ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		},
		Revision: 22, ReadRevision: 41,
	}
	backingService := testkeyvalue.Versioned[testservices.ServiceRecord]{
		Record: testservices.ServiceRecord{
			EnvironmentID: backingEnvironmentID, BackingNetworkID: backingNetworkID,
			Desired: core.Service{ID: backingServiceID, Name: "postgres", Adapter: "custom"},
			Runtime: core.ServiceRuntime{ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
		},
		Revision: 23, ReadRevision: 41,
	}
	owner, err := testattachments.NewPendingAttachRecord(
		ownerID, environmentID, "database-owner", backingProjectID, backingEnvironmentID,
		backingServiceID, backingNetworkID, consumerID, ownerID, nil, nil, ownerTaskID, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = testattachments.MarkAttachProvisioning(owner, ownerTaskID)
	if err != nil {
		t.Fatal(err)
	}
	owner, err = testattachments.CompleteAttachProvisioning(owner, ownerTaskID, true)
	if err != nil {
		t.Fatal(err)
	}
	repository := &environmentBlueprintAttachRepositoryStub{
		backingProject: backingProject, backingEnvironment: backingEnvironment, backingService: backingService,
	}
	service := &Service{repository: repository}
	prepared, err := service.prepareBlueprintAttaches(
		context.Background(),
		environmentID,
		taskID,
		map[string]core.AttachmentSpec{
			"database-owner": {
				BackingProject: "database", BackingService: "postgres", Service: "worker",
				Credential: core.AttachmentCredentialSpec{Mode: "new"},
			},
			"worker-database": {
				BackingProject: "database", BackingService: "postgres", Service: "worker",
				Credential: core.AttachmentCredentialSpec{Mode: "existing", Attach: "database-owner"},
			},
		},
		[]testblueprints.EnvironmentBlueprintServiceChange{{Record: testservices.ServiceRecord{
			EnvironmentID: environmentID, Desired: core.Service{ID: consumerID, Name: "worker"},
		}}},
		[]testkeyvalue.Versioned[testattachments.Record]{{Record: owner, Revision: 31, ReadRevision: 41}},
		func(ids.Kind, string) string { return candidateID },
		false,
		now,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintAttaches(retained credential owner) error = %v", err)
	}
	defer prepared.clear()
	if len(prepared.publication.Intent.Candidates) != 1 ||
		prepared.publication.Intent.Candidates[0].CredentialAttachID != ownerID ||
		len(prepared.effective) != 2 || prepared.effective[1].Record.ID != candidateID ||
		prepared.effective[0].Revision != 31 || prepared.effective[0].Record.ID != ownerID {
		t.Fatalf("retained credential owner preparation = %#v / %#v", prepared.publication.Intent, prepared.effective)
	}
	joins, err := testtaskplanning.ResolveAttachNetworkJoins(
		environmentID,
		testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: environmentID, DesiredServices: []testservices.EnvironmentServiceProjection{
				{Desired: core.Service{ID: consumerID, Name: "worker"}},
			},
		},
		prepared.effective,
		"",
	)
	if err != nil || len(joins) != 1 || joins[0].NetworkID != backingNetworkID {
		t.Fatalf("cumulative retained Attach network union: %v, %v", joins, err)
	}
}

func TestEnvironmentBlueprintAttachPreparationUsesRetainedGrantTarget(t *testing.T) {
	adapters.Register(attachFactTestAdapter{})
	now := time.Date(2026, 9, 2, 17, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 111)
	apiServiceID := ids.NewAt(ids.KindService, now, 112)
	grantServiceID := ids.NewAt(ids.KindService, now, 113)
	backingProjectID := ids.NewAt(ids.KindProject, now, 114)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 115)
	backingServiceID := ids.NewAt(ids.KindService, now, 116)
	backingNetworkID := ids.NewAt(ids.KindNetwork, now, 117)
	taskID := ids.NewAt(ids.KindTask, now, 118)
	grantID := ids.NewAt(ids.KindAttach, now, 119)
	candidateID := ids.NewAt(ids.KindAttach, now, 120)
	crypt := blueprintTestCrypt{}
	protector, err := secretvalue.NewProtector(crypt, crypt)
	if err != nil {
		t.Fatal(err)
	}
	factRepository := &blueprintAttachFactRepository{
		records: map[string]testkeyvalue.Versioned[testattachments.Record]{},
	}
	facts, err := attachments.NewFactService(factRepository, protector)
	if err != nil {
		t.Fatal(err)
	}
	metadata, encrypted, err := facts.SealFactSets(
		context.Background(),
		grantID,
		attachFactTestAdapter{},
		adapters.Input{
			Host: "postgres", Port: "5432", Database: "reporting", Role: "reporting", Password: []byte("retained-secret"),
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(encrypted.Ciphertext)
	grant := readyAttachRecord(
		t, grantID, environmentID, "reporting", backingProjectID, backingEnvironmentID,
		backingServiceID, backingNetworkID, grantServiceID, nil, metadata, now, 121,
	)
	factRepository.facts = *encrypted
	factRepository.records[grant.Name] = testkeyvalue.Versioned[testattachments.Record]{
		Record: grant, Revision: 31, ReadRevision: 41,
	}
	backingProject := testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
		Record: testhierarchy.ProjectRecord{
			ID:   backingProjectID,
			Slug: "database",
			Kind: testhierarchy.ProjectKindBacking,
		},
		Revision: 21, ReadRevision: 41,
	}
	backingEnvironment := testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
		Record: testhierarchy.EnvironmentRecord{
			ID: backingEnvironmentID, ProjectID: backingProjectID, Name: "main",
			ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		},
		Revision: 22, ReadRevision: 41,
	}
	backingService := testkeyvalue.Versioned[testservices.ServiceRecord]{
		Record: testservices.ServiceRecord{
			EnvironmentID: backingEnvironmentID, BackingNetworkID: backingNetworkID,
			Desired: core.Service{ID: backingServiceID, Name: "postgres", Adapter: "test:1"},
			Runtime: core.ServiceRuntime{ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
		},
		Revision: 23, ReadRevision: 41,
	}
	repository := &environmentBlueprintAttachRepositoryStub{
		backingProject: backingProject, backingEnvironment: backingEnvironment, backingService: backingService,
	}
	service := &Service{
		repository: repository, attachFacts: facts, random: strings.NewReader(strings.Repeat("x", 256)),
	}
	prepared, err := service.prepareBlueprintAttaches(
		context.Background(),
		environmentID,
		taskID,
		map[string]core.AttachmentSpec{
			"api-database": {
				BackingProject: "database", BackingService: "postgres", Service: "api",
				Credential: core.AttachmentCredentialSpec{Mode: "new"}, Grants: []string{"reporting"},
			},
			"reporting": {
				BackingProject: "database", BackingService: "postgres", Service: "reporting-service",
				Credential: core.AttachmentCredentialSpec{Mode: "new"},
			},
		},
		[]testblueprints.EnvironmentBlueprintServiceChange{
			{
				Record: testservices.ServiceRecord{
					EnvironmentID: environmentID,
					Desired:       core.Service{ID: apiServiceID, Name: "api"},
				},
			},
			{
				Record: testservices.ServiceRecord{
					EnvironmentID: environmentID,
					Desired:       core.Service{ID: grantServiceID, Name: "reporting-service"},
				},
			},
		},
		[]testkeyvalue.Versioned[testattachments.Record]{{Record: grant, Revision: 31, ReadRevision: 41}},
		func(ids.Kind, string) string { return candidateID },
		false,
		now,
	)
	if err != nil {
		t.Fatalf("prepareBlueprintAttaches(retained grant) error = %v", err)
	}
	defer prepared.clear()
	if len(prepared.publication.Intent.Candidates) != 1 ||
		len(prepared.publication.Intent.Candidates[0].GrantAttachIDs) != 1 ||
		prepared.publication.Intent.Candidates[0].GrantAttachIDs[0] != grantID ||
		factRepository.factReads != 1 {
		t.Fatalf(
			"retained grant preparation = %#v, fact reads = %d",
			prepared.publication.Intent,
			factRepository.factReads,
		)
	}
}

func TestEnvironmentBlueprintBackupValidationResolvesFixedRevisionDependencies(t *testing.T) {
	now := time.Date(2026, 9, 2, 15, 30, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 21)
	volumeID := ids.NewAt(ids.KindVolume, now, 22)
	attachID := ids.NewAt(ids.KindAttach, now, 23)
	stub := &environmentBlueprintBackupRepositoryStub{}
	service := &Service{backups: stub}
	err := service.validateEnvironmentBlueprintBackup(
		context.Background(), environmentID, 47,
		&core.BackupSpec{
			Enabled: false, Frequency: "*-*-* 02:00:00", Keep: 2, Encryption: "age",
			Sources: []core.BackupSourceSpec{
				{Kind: core.BackupSourceConfig},
				{Kind: core.BackupSourceVolume, Ref: "archive"},
				{Kind: core.BackupSourceAttach, Ref: "database"},
			},
		}, testenvironmentprojection.EnvironmentComposeProjection{Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{ID: volumeID, Slug: "archive"}}}, []testkeyvalue.Versioned[testattachments.Record]{{Record: testattachments.Record{
			ID: attachID, EnvironmentID: environmentID, Name: "database", CredentialAttachID: attachID,
		}}},
	)
	if err != nil || stub.input.ReadRevision != 47 || len(stub.input.Sources) != 3 ||
		stub.input.Sources[0].TargetID != environmentID || stub.input.Sources[1].TargetID != volumeID ||
		stub.input.Sources[2].TargetID != attachID {
		t.Fatalf("validation input = %#v, error = %v", stub.input, err)
	}
	changes := environmentBlueprintChanges(
		blueprintparser.AuthoringDocument{Backup: &core.BackupSpec{}},
		blueprintparser.Result{Project: &composetypes.Project{}, Extensions: blueprintparser.Extensions{}},
		true, testenvironmentprojection.EnvironmentComposeProjection{},
	)
	found := false
	for _, change := range changes {
		found = found || change.Resource == "backup" && change.Key == "policy" &&
			change.Action == apiTypes.BlueprintChangeRetain
	}
	if !found {
		t.Fatalf("omitted Backup validation changes = %#v", changes)
	}
}

// Rationale: canonical authoring maps stable target ids to current labels and
// omits only a deleted Connector from a valid disabled policy.
func TestEnvironmentBlueprintBackupAuthoringUsesLabelsAndHidesDeletedDisabledConnector(t *testing.T) {
	now := time.Date(2026, 9, 2, 16, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 11)
	volumeID := ids.NewAt(ids.KindVolume, now, 12)
	attachID := ids.NewAt(ids.KindAttach, now, 13)
	connectorID := ids.NewAt(ids.KindConnector, now, 14)
	stub := &environmentBlueprintBackupRepositoryStub{snapshot: environmentBlueprintBackupPolicySnapshot{
		found: true, connectorFound: true, connectorName: "current-store",
		policy: testbackuppolicy.BackupPolicyRecord{
			EnvironmentID: environmentID, Enabled: true, Frequency: "*-*-* 02:00:00",
			Keep: 4, Encryption: "age", ConnectorID: connectorID,
		},
		sources: []testbackuppolicy.BackupSourceRecord{
			{EnvironmentID: environmentID, Kind: core.BackupSourceVolume, TargetID: volumeID},
			{EnvironmentID: environmentID, Kind: core.BackupSourceAttach, TargetID: attachID},
		},
	}}
	service := &Service{backups: stub}
	snapshot := environmentBlueprintSnapshot{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{ID: environmentID},
		},
		hasHead: true,
		projection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: testenvironmentprojection.EnvironmentComposeProjection{
				Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{ID: volumeID, Slug: "archive"}},
			},
		},
	}
	authored, err := service.environmentBlueprintAuthoringBackup(
		context.Background(),
		snapshot,
		[]testkeyvalue.Versioned[testattachments.Record]{
			{Record: testattachments.Record{ID: attachID, Name: "database"}},
		},
	)
	if err != nil || authored.Connector != "current-store" || authored.Sources[0].Ref != "archive" ||
		authored.Sources[1].Ref != "database" {
		t.Fatalf("authoring Backup = %#v, %v", authored, err)
	}
	stub.snapshot.policy.Enabled = false
	stub.snapshot.connectorFound = false
	authored, err = service.environmentBlueprintAuthoringBackup(
		context.Background(),
		snapshot,
		[]testkeyvalue.Versioned[testattachments.Record]{
			{Record: testattachments.Record{ID: attachID, Name: "database"}},
		},
	)
	if err != nil || authored == nil || authored.Enabled || authored.Connector != "" || authored.Keep != 4 ||
		authored.Encryption != "age" || authored.Frequency != "*-*-* 02:00:00" || len(authored.Sources) != 2 {
		t.Fatalf("deleted disabled Connector authoring = %#v, %v", authored, err)
	}
	document, err := blueprintparser.MarshalAuthoringDocument(blueprintparser.AuthoringDocument{
		Envelope: core.Envelope{Kind: core.KindDocEnvironment, Schema: core.EnvelopeSchema,
			Metadata: core.EnvelopeMetadata{Tenant: "acme", Project: "console", Environment: "production"}},
		NetworkPool: "10.40.0.0/16", Compose: []byte("services: {}\n"), Backup: authored,
	})
	if err != nil || strings.Contains(string(document), connectorID) || strings.Contains(string(document), volumeID) ||
		strings.Contains(string(document), attachID) {
		t.Fatalf("canonical disabled Backup leaked stable ids: %q, %v", document, err)
	}
	parsed, err := blueprintparser.Parse(context.Background(), blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID, Tenant: "acme", Project: "console", Environment: "production",
	}, core.BlueprintBundle{
		RootPath: "compose.yaml", ComposeSources: []string{"compose.yaml"},
		Files: []core.BlueprintFile{{Path: "compose.yaml", Content: document}},
	})
	if err != nil || parsed.Extensions.Backup == nil || parsed.Extensions.Backup.Keep != 4 ||
		len(parsed.Extensions.Backup.Sources) != 2 {
		t.Fatalf("canonical disabled Backup reparse = %#v, %v", parsed.Extensions.Backup, err)
	}
	stub.snapshot.policy.Enabled = true
	if _, err := service.environmentBlueprintAuthoringBackup(context.Background(), snapshot, nil); err == nil {
		t.Fatal("enabled missing Connector authoring succeeded")
	}
	stub.snapshotErr = errs.Wrap(errs.KindInternal, errors.New("storage unavailable"))
	if _, err := service.environmentBlueprintAuthoringBackup(context.Background(), snapshot, nil); err == nil ||
		!strings.Contains(err.Error(), "storage unavailable") {
		t.Fatalf("authoring snapshot error = %v", err)
	}
}
