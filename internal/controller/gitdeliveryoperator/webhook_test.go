package gitdeliveryoperator

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/githubconnection"
)

func TestWebhookSignatureBindsExactRequestBytes(t *testing.T) {
	secret := []byte("synthetic signing credential for tests")
	body := []byte(`{"ref":"refs/heads/staging","installation":{"id":42}}`)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	digest, valid := webhookDigest("sha256=" + hex.EncodeToString(mac.Sum(nil)))
	if !valid || !verifyWebhookSignature(secret, body, digest) {
		t.Fatal("correctly signed webhook rejected")
	}
	if verifyWebhookSignature(secret, append(body, '\n'), digest) ||
		verifyWebhookSignature([]byte("rotated signing credential"), body, digest) ||
		verifyWebhookSignature(nil, body, digest) {
		t.Fatal("changed body or key authenticated")
	}
}

func TestWebhookListenerRejectsOtherRoutesAndUnsignedRequests(t *testing.T) {
	handler := (&Operator{}).GitHubWebhook()
	path := GitHubWebhookPrefix + "tenant/" + ids.New(ids.KindTenant) + "/" + ids.New(githubconnection.IDKind)
	requests := []struct {
		method string
		path   string
		status int
	}{
		{http.MethodPost, "/api/v1/services", http.StatusNotFound},
		{http.MethodPost, path + "/", http.StatusNotFound},
		{http.MethodPost, path + "?unsigned=true", http.StatusNotFound},
		{http.MethodPost, strings.Replace(path, "/tenant/", "/%74enant/", 1), http.StatusNotFound},
		{http.MethodGet, path, http.StatusMethodNotAllowed},
		{http.MethodPost, path, http.StatusUnauthorized},
	}
	for _, example := range requests {
		writer := httptest.NewRecorder()
		handler.ServeHTTP(writer, httptest.NewRequest(example.method, example.path, nil))
		if writer.Code != example.status {
			t.Fatalf("%s %s returned %d, want %d", example.method, example.path, writer.Code, example.status)
		}
	}
}
