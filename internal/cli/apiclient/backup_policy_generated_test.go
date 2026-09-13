package apiclient

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
)

// Rationale: generated consumers must retain exact operation identities,
// JSON mutation headers, non-null sources, enums, and Volume scope.
// QA: BAK-01, VOL-01; generated HTTP model only, not policy or Volume effects.
func TestGeneratedBackupPolicyAndVolumeOperations(t *testing.T) {
	doer := backupPolicyGeneratedDoer(func(request *http.Request) (*http.Response, error) {
		body := ""
		switch request.Method + " " + request.URL.Path {
		case "GET /environments/env_01AAAAAAAAAAAAAAAAAAAAAAAA/backup-policy":
			body = `{"enabled":false,"sources":[]}`
		case "PUT /environments/env_01AAAAAAAAAAAAAAAAAAAAAAAA/backup-policy":
			if got := request.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
			if got := request.Header.Get("Idempotency-Key"); got != "0123456789abcdef" {
				t.Errorf("Idempotency-Key = %q", got)
			}
			body = `{"enabled":true,"frequency":"0 0 * * *","keep":3,"encryption":"age","connector_id":"con_01AAAAAAAAAAAAAAAAAAAAAAAA","sources":[{"id":"bps_01AAAAAAAAAAAAAAAAAAAAAAAA","kind":"config","target_id":"env_01AAAAAAAAAAAAAAAAAAAAAAAA"}]}`
		case "GET /volumes":
			if got := request.URL.Query().Get("environment"); got != "env_01AAAAAAAAAAAAAAAAAAAAAAAA" {
				t.Errorf("environment = %q", got)
			}
			body = `{"items":[]}`
		default:
			t.Errorf("unexpected generated request: %s %s", request.Method, request.URL)
			body = `{}`
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    request,
		}, nil
	})
	client, err := generated.NewClientWithResponses("http://groundplane.test", generated.WithHTTPClient(doer))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	show, err := client.BackupPolicyShowWithResponse(ctx, "env_01AAAAAAAAAAAAAAAAAAAAAAAA")
	if err != nil || show.JSON200 == nil || show.JSON200.Sources == nil {
		t.Fatalf("BackupPolicyShowWithResponse() = %#v, %v", show, err)
	}
	frequency := "0 0 * * *"
	keep := int64(3)
	encryption := generated.BackupPolicyReplacementRequestEncryptionAge
	connectorID := "con_01AAAAAAAAAAAAAAAAAAAAAAAA"
	sources := []generated.BackupSourceInput{{
		Kind:     generated.BackupSourceInputKindConfig,
		TargetId: "env_01AAAAAAAAAAAAAAAAAAAAAAAA",
	}}
	set, err := client.BackupPolicySetWithResponse(
		ctx,
		"env_01AAAAAAAAAAAAAAAAAAAAAAAA",
		&generated.BackupPolicySetParams{IdempotencyKey: "0123456789abcdef"},
		generated.BackupPolicyReplacementRequest{
			Enabled: true, Frequency: &frequency, Keep: &keep, Encryption: &encryption,
			ConnectorId: &connectorID, Sources: sources,
		},
	)
	if err != nil || set.JSON200 == nil || set.JSON200.Sources == nil {
		t.Fatalf("BackupPolicySetWithResponse() = %#v, %v", set, err)
	}
	if set.JSON200.Encryption == nil || *set.JSON200.Encryption != generated.BackupPolicyEncryptionAge {
		t.Fatalf("BackupPolicySetWithResponse().Encryption = %#v", set.JSON200.Encryption)
	}
	volumes, err := client.VolumeListWithResponse(ctx, &generated.VolumeListParams{
		Environment: "env_01AAAAAAAAAAAAAAAAAAAAAAAA",
	})
	if err != nil || volumes.JSON200 == nil || volumes.JSON200.Items == nil || len(*volumes.JSON200.Items) != 0 {
		t.Fatalf("VolumeListWithResponse() = %#v, %v", volumes, err)
	}
}

type backupPolicyGeneratedDoer func(*http.Request) (*http.Response, error)

func (doer backupPolicyGeneratedDoer) Do(request *http.Request) (*http.Response, error) {
	return doer(request)
}
