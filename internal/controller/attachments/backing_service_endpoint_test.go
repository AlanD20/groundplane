package attachments

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// QA: ATT-12; local fact-rendering regression, not live Docker DNS or authentication proof.
// Rationale: distinct backing instances can share the adapter Service name, so their HOST and URL facts must use
// distinct stable aliases that select only the intended instance while both networks remain attached.
func TestPrepareAttachFactsUsesStableBackingEndpoint(t *testing.T) {
	registerAdapters()
	adapter, registered := adapters.Get("postgres:16")
	if !registered {
		t.Fatal("postgres:16 adapter is not registered")
	}
	tests := []struct {
		serviceID string
		wantHost  string
	}{
		{"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", "gp-svc-01arz3ndektsv4rrffq69g5fav"},
		{"svc_01ARZ3NDEKTSV4RRFFQ69G5FAW", "gp-svc-01arz3ndektsv4rrffq69g5faw"},
	}
	for _, test := range tests {
		t.Run(test.serviceID, func(t *testing.T) {
			capture := &backingEndpointFactCapture{}
			defer capture.clear()
			service := &MutationService{facts: capture, random: strings.NewReader(strings.Repeat("x", 64))}
			identity, _, _, _, err := service.prepareAttachFacts(
				context.Background(),
				"att_01ARZ3NDEKTSV4RRFFQ69G5FAX",
				"op_01ARZ3NDEKTSV4RRFFQ69G5FAZ",
				testkeyvalue.Versioned[testservices.ServiceRecord]{
					Record: testservices.ServiceRecord{Desired: core.Service{Name: "api"}},
				},
				etcd.AttachCreateScope{
					BackingService: testkeyvalue.Versioned[testservices.ServiceRecord]{
						Record: testservices.ServiceRecord{Desired: core.Service{
							ID: test.serviceID, Name: "postgres", Adapter: "postgres:16",
						}},
					},
					Grants: []testkeyvalue.Versioned[testattachments.Record]{{Record: testattachments.Record{
						ID: "att_01ARZ3NDEKTSV4RRFFQ69G5FAY",
					}}},
				},
				adapter,
			)
			if err != nil {
				t.Fatalf("prepareAttachFacts() error = %v", err)
			}
			defer identity.Clear()
			if len(capture.grants) != 1 {
				t.Fatalf("captured grant fact sets = %d, want 1", len(capture.grants))
			}
			for name, params := range map[string]adapters.Input{
				"owner": capture.own,
				"grant": capture.grants[0].Params,
			} {
				assertBackingEndpointFacts(t, name, adapter, params, test.wantHost)
			}
		})
	}
}

type backingEndpointFactCapture struct {
	attachMutationFacts
	own    adapters.Input
	grants []GrantInput
}

func (capture *backingEndpointFactCapture) SealFactSets(
	_ context.Context,
	_ string,
	_ adapters.Adapter,
	own adapters.Input,
	grants []GrantInput,
) ([]testattachments.FactSetMetadata, *testattachments.EncryptedFacts, error) {
	own.Password = append([]byte(nil), own.Password...)
	capture.own = own
	capture.grants = append([]GrantInput(nil), grants...)
	for index := range capture.grants {
		capture.grants[index].Params.Password = append([]byte(nil), capture.grants[index].Params.Password...)
	}
	return nil, nil, nil
}

func (capture *backingEndpointFactCapture) ResolveReadyDatabase(
	_ context.Context,
	_ testkeyvalue.Versioned[testattachments.Record],
	consume func(string) error,
) error {
	return consume("shared")
}

func (capture *backingEndpointFactCapture) clear() {
	clear(capture.own.Password)
	for index := range capture.grants {
		clear(capture.grants[index].Params.Password)
	}
}

func assertBackingEndpointFacts(
	t *testing.T,
	set string,
	adapter adapters.Adapter,
	params adapters.Input,
	wantHost string,
) {
	t.Helper()
	facts, err := adapters.BuildFacts(adapter, params)
	if err != nil {
		t.Fatalf("BuildFacts(%s) error = %v", set, err)
	}
	defer adapters.ClearFacts(facts)
	values := make(map[string]string, len(facts))
	for _, fact := range facts {
		values[fact.Key] = string(fact.Value)
	}
	if values["pg16_HOST"] != wantHost {
		t.Fatalf("%s HOST = %q, want endpoint %q", set, values["pg16_HOST"], wantHost)
	}
	if !strings.Contains(values["pg16_URL"], "@"+wantHost+":5432/") {
		t.Fatalf("%s URL does not use endpoint %q", set, wantHost)
	}
}
