package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/backup"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-159: the actual native removal producer must commit desired state,
// replacement Backup policy, scheduling and visible work together. A changed
// policy/source fence cannot be hidden by staging or an automatic replan.
func TestNativeVolumeRemovalJoinsBackupPolicyAndRejectsRacingAuthority(t *testing.T) {
	for _, test := range []struct {
		name  string
		count int
		race  string
	}{
		{"last source", 1, ""}, {"maximum selection", api.MaximumBackupPolicySources, ""},
		{"policy race", 1, "policy"}, {"coordination race", 1, "coordination"},
		{"epoch race", 1, "epoch"}, {"source race", 1, "source"}, {"Connector reference race", 1, "Connector reference"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newNativeVolumeFixture(t)
			finishNativeVolumeFixture(t, f, f.created.TaskID)
			policy := configureNativeVolumeBackupPolicy(t, f, test.count)
			policyKey := backuppolicy.BackupPolicyKey(f.environmentID)
			coordinationKey := environmentcoordination.Key(f.environmentID)
			headKey := blueprints.EnvironmentBlueprintHeadKey(f.environmentID)
			baseline, err := f.store.Get(t.Context(), headKey)
			if err != nil || baseline.Entry == nil {
				t.Fatalf("baseline desired state: %v", err)
			}
			impact, err := f.reads.GetVolumeDeletionImpact(t.Context(), f.created.Volume.ID, "", 40)
			if err != nil || !impact.Complete {
				t.Fatalf("policy-aware Volume impact: %v", err)
			}
			injected := false
			staleRefused := false
			if test.race != "" {
				f.store.afterBlueprintCommit = func(result keyvalue.TransactionResult) {
					if injected && !result.Succeeded {
						staleRefused = true
					}
				}
				key := map[string]string{"policy": policyKey, "coordination": coordinationKey,
					"epoch":               hierarchy.EnvironmentMutationEpochKey(f.environmentID),
					"source":              backuppolicy.BackupSourceKey(policy.Sources[0].ID),
					"Connector reference": backuppolicy.BackupPolicyConnectorReferenceKey(policy.ConnectorID, f.environmentID)}[test.race]
				f.store.beforeCommit = func(mutations []keyvalue.Mutation) {
					for _, mutation := range mutations {
						task, err := workqueue.DecodeTask(mutation.Value)
						if err != nil || mutation.Key != workqueue.TaskKey(task.ID) ||
							task.Action != workexecution.ActionRemove {
							continue
						}
						f.store.beforeCommit = nil
						value, err := f.store.Get(t.Context(), key)
						if err != nil || value.Entry == nil {
							t.Fatalf("race authority missing: %v", err)
						}
						if _, err := f.store.Put(t.Context(), key, value.Entry.Value); err != nil {
							t.Fatal(err)
						}
						injected = true
						return
					}
				}
			}
			response, err := f.mutations.RemoveVolume(
				t.Context(),
				f.created.Volume.ID,
				impact.ImpactToken,
				"data",
				"native-volume-policy-remove",
			)
			if test.race != "" && (!injected || !staleRefused) {
				t.Fatalf(
					"racing %s bypassed the stale comparison: %v; injected=%t refused=%t",
					test.race,
					err,
					injected,
					staleRefused,
				)
			}
			if test.race == "policy" || test.race == "source" {
				if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
					t.Fatalf("changed removal impact accepted: %v", err)
				}
				current, readErr := f.store.Get(t.Context(), headKey)
				if readErr != nil || current.Entry == nil || current.Entry.ModRevision != baseline.Entry.ModRevision {
					t.Fatalf("rejected removal changed desired state: %v", readErr)
				}
				page, readErr := f.queue.ListTasks(t.Context(), f.environmentID, "", 100, 0)
				if readErr != nil {
					t.Fatal(readErr)
				}
				for _, task := range page.Tasks {
					if task.Task.Action == workexecution.ActionRemove {
						t.Fatal("racing policy published a removal Task")
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var accepted api.TaskAccepted
			if err := json.Unmarshal(response.Body, &accepted); err != nil {
				t.Fatal(err)
			}
			task, _, err := f.queue.ReadTask(t.Context(), accepted.TaskID, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{headKey, policyKey, coordinationKey} {
				value, err := f.store.Get(t.Context(), key)
				if err != nil || value.Entry == nil || value.Entry.ModRevision != task.Revision {
					t.Fatalf("policy/desired state did not join Task: %s: %v", key, err)
				}
			}
			policies, err := etcd.NewBackupPolicyRepository(f.store)
			if err != nil {
				t.Fatal(err)
			}
			current, err := policies.GetBackupPolicyProjection(t.Context(), f.environmentID)
			if err != nil || len(current.Sources) != test.count-1 || current.Enabled != (test.count > 1) {
				t.Fatalf("removal lost remaining Backup selection: %#v, %v", current, err)
			}
			for index, source := range current.Sources {
				if source.ID != policy.Sources[index+1].ID || source.TargetID != policy.Sources[index+1].TargetID {
					t.Fatal("Volume removal changed another Backup source or its order")
				}
			}
			projection, found, err := f.blueprint.GetEnvironmentComposeProjection(t.Context(), f.environmentID)
			if err != nil || !found || projection.Record.Backup == nil ||
				len(projection.Record.Backup.Sources) != test.count-1 {
				t.Fatalf("desired Backup differs from effective replacement: %v", err)
			}
			for _, volume := range projection.Record.Volumes {
				if volume.ID == f.created.Volume.ID {
					t.Fatal("desired removal retained Volume")
				}
			}
			for _, source := range policy.Sources {
				value, err := f.store.Get(t.Context(), backuppolicy.BackupSourceKey(source.ID))
				if err != nil || value.Entry == nil {
					t.Fatalf("removal erased historical source authority: %v", err)
				}
			}
			applied, err := f.inputs.ReadApplied(t.Context(), f.created.Volume.ID, 0)
			if err != nil || applied == nil {
				t.Fatalf("acceptance erased applied storage before absence proof: %v", err)
			}
			before := f.store.revision
			again, err := f.mutations.RemoveVolume(
				t.Context(),
				f.created.Volume.ID,
				impact.ImpactToken,
				"data",
				"native-volume-policy-remove",
			)
			if err != nil || !bytes.Equal(again.Body, response.Body) || f.store.revision != before {
				t.Fatalf("native policy removal replay rewrote state: %v", err)
			}
		})
	}
}

func finishNativeVolumeFixture(t *testing.T, f *nativeVolumeFixture, taskID string) {
	t.Helper()
	view := f.start(t, taskID)
	progress := f.progress(t, view)
	for progress.Stage != volumeexecution.Complete {
		progress = f.advance(t, view, progress)
	}
	f.complete(t, view, progress)
}

func configureNativeVolumeBackupPolicy(t *testing.T, f *nativeVolumeFixture, count int) api.BackupPolicy {
	t.Helper()
	sources := []api.BackupSourceInput{{Kind: api.BackupSourceVolume, TargetID: f.created.Volume.ID}}
	for index := 1; index < count; index++ {
		response, err := f.mutations.CreateVolume(
			t.Context(),
			api.VolumeCreate{EnvironmentID: f.environmentID, Slug: "data-" + strconv.Itoa(index)},
			"policy-volume-create-"+strconv.Itoa(index),
		)
		if err != nil {
			t.Fatal(err)
		}
		var created api.VolumeMutationResponse
		if err := json.Unmarshal(response.Body, &created); err != nil {
			t.Fatal(err)
		}
		finishNativeVolumeFixture(t, f, created.TaskID)
		sources = append(sources, api.BackupSourceInput{Kind: api.BackupSourceVolume, TargetID: created.Volume.ID})
	}
	connector, err := connectors.NewRecord(
		core.Connector{ID: ids.New(ids.KindConnector), EnvironmentID: f.environmentID,
			Name: "volume-backups", Kind: core.ConnectorKindS3Compatible, Endpoint: "https://objects.example.test", Bucket: "volume-backups", Prefix: "qa/", Region: "auto", PathStyle: true,
			Credentials: map[core.ConnectorCredentialName]core.ConnectorCredential{
				core.ConnectorCredentialAccessKey: {Kind: core.ConnectorCredentialDirect},
				core.ConnectorCredentialSecretKey: {Kind: core.ConnectorCredentialDirect},
			}},
	)
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := connectors.NewEncryptedCredentials(
		connector.Connector.ID,
		[]byte("supplied-storage-credentials"),
	)
	if err != nil {
		t.Fatal(err)
	}
	connectorRepository, err := etcd.NewConnectorRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	environment, err := f.blueprint.GetEnvironment(t.Context(), f.environmentID)
	if err != nil {
		t.Fatal(err)
	}
	project, err := f.blueprint.GetProject(t.Context(), environment.Record.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connectorRepository.CreateConnector(t.Context(), environment, project, connector, credentials); err != nil {
		t.Fatal(err)
	}
	key, err := ageinfra.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}
	cipher := nativeVolumeCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := requestidempotency.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := etcd.NewIdempotencyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	protected, err := backup.NewDurableBackupPolicyIdempotency(coordinator, replay)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := backup.NewAgeBackupPolicyKeyFactory(protector)
	if err != nil {
		t.Fatal(err)
	}
	policies, err := etcd.NewBackupPolicyRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	policyRepository, err := backup.NewDurableBackupPolicyRepository(policies)
	if err != nil {
		t.Fatal(err)
	}
	service, err := backup.NewBackupPolicyService(policyRepository, keys, protected)
	if err != nil {
		t.Fatal(err)
	}
	configured, err := service.SetBackupPolicy(
		t.Context(),
		f.environmentID,
		api.BackupPolicyReplacementRequest{
			Enabled:     true,
			Frequency:   "0 2 * * *",
			Keep:        7,
			Encryption:  api.BackupEncryptionNone,
			ConnectorID: connector.Connector.ID,
			Sources:     sources,
		},
		"native-volume-backup-policy",
	)
	if err != nil {
		t.Fatal(err)
	}
	return configured.Policy
}
