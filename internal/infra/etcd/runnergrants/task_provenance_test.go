package runnergrants

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func TestRunnerTaskProvenanceBindsNativeTask(t *testing.T) {
	image := imagefetch.RegistryAuthority + "/approved@sha256:" + strings.Repeat("e", 64)
	scope := workexecution.Scope{
		TenantID: ids.New(
			ids.KindTenant,
		), ProjectID: ids.New(ids.KindProject), EnvironmentID: ids.New(ids.KindEnvironment),
	}
	authority := TaskProvenance{
		RunnerID: ids.New(ids.KindRunner), RunnerEpoch: 2, GrantRevision: 4, CredentialRevision: 6,
		ExpiresAt: time.Now().
			UTC().
			Add(time.Hour),
		TokenDigest: strings.Repeat("a", 64), Scope: scope, ServiceID: ids.New(ids.KindService),
		Action: workexecution.ActionFetch, Image: image,
	}
	if err := authority.ValidateFetch(authority.ServiceID, image); err != nil {
		t.Fatal(err)
	}
	if err := authority.ValidateFetch(ids.New(ids.KindService), image); err == nil {
		t.Fatal("accepted a different Service")
	}
	if err := authority.ValidateFetch(authority.ServiceID, imagefetch.RegistryAuthority+"/approved@sha256:"+strings.Repeat("b", 64)); err == nil {
		t.Fatal("accepted a different image")
	}
	if err := authority.ValidateDeploy(authority.ServiceID, image); err == nil {
		t.Fatal("accepted Deploy using a Fetch grant")
	}
	intent := workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("f", 64)}
	task := workexecution.Task{
		ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor, Scope: scope,
		TargetID: ids.New(ids.KindHost), TargetName: image, Action: workexecution.ActionFetch, AcceptedIntent: &intent,
	}
	change, err := authority.PublicationForTask(task)
	if err != nil {
		t.Fatal(err)
	}
	record, err := recordcodec.Decode[TaskProvenanceRecord](change.Mutations[0].Value, taskProvenanceKind)
	if err != nil || !record.MatchesTask(task) || record.Authority != authority {
		t.Fatalf("record does not bind accepted Task: %v", err)
	}
	replaced := intent
	replaced.SHA256 = strings.Repeat("c", 64)
	task.AcceptedIntent = &replaced
	if record.MatchesTask(task) {
		t.Fatal("accepted a substituted Task input")
	}
	task.AcceptedIntent = &intent
	task.Scope.EnvironmentID = ids.New(ids.KindEnvironment)
	if _, err = authority.PublicationForTask(task); err == nil {
		t.Fatal("published provenance for a different Environment")
	}
}
