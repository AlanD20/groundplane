package s3compatible

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Rationale: SDK input assertions do not prove the signed HTTP request. This
// server observes immutable conditionals, explicit MD5, checksum policy,
// three-attempt retries, replayed bodies, and both addressing modes on wire.
func TestWirePutObjectContractAndAddressing(t *testing.T) {
	for _, pathStyle := range []bool{true, false} {
		t.Run(fmt.Sprintf("path_style_%t", pathStyle), func(t *testing.T) {
			body := []byte("wire artifact")
			artifact := testArtifact(body, backupobject.EncryptionNone)
			var attempts atomic.Int32
			var server *httptest.Server
			server = newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				attempt := attempts.Add(1)
				wireBody, err := io.ReadAll(request.Body)
				if err != nil {
					t.Errorf("read request body: %v", err)
				}
				if request.Method != http.MethodPut || request.Header.Get("If-None-Match") != "*" ||
					request.Header.Get("Content-MD5") != md5Base64(body) || !bytes.Equal(wireBody, body) {
					t.Errorf("PUT method/conditional/md5/body = %s/%q/%q/%q",
						request.Method, request.Header.Get("If-None-Match"), request.Header.Get("Content-MD5"), wireBody)
				}
				if request.Header.Get("X-Amz-Sdk-Checksum-Algorithm") != "" ||
					request.Header.Get("X-Amz-Checksum-Crc32") != "" {
					t.Errorf("unexpected optional checksum headers: %#v", request.Header)
				}
				if request.Header.Get("X-Amz-Meta-Groundplane-Format-Version") != "1" {
					t.Errorf("format metadata = %q", request.Header.Get("X-Amz-Meta-Groundplane-Format-Version"))
				}
				if pathStyle {
					if request.Host != wireEndpointHost(server) || request.URL.Path != "/groundplane-backups/"+artifact.Key {
						t.Errorf("path-style host/path = %q/%q", request.Host, request.URL.Path)
					}
				} else if request.Host != "groundplane-backups."+wireEndpointHost(server) ||
					request.URL.Path != "/"+artifact.Key {
					t.Errorf("virtual-host host/path = %q/%q", request.Host, request.URL.Path)
				}
				if attempt < 3 {
					response.Header().Set("Content-Type", "application/xml")
					response.WriteHeader(http.StatusInternalServerError)
					_, _ = io.WriteString(response, `<Error><Code>InternalError</Code><Message>private</Message></Error>`)
					return
				}
				response.Header().Set("ETag", `"wire-etag"`)
			}))
			defer server.Close()

			adapter := newWireAdapter(t, server, pathStyle)
			object, err := adapter.PutExact(context.Background(), artifact, bytes.NewReader(body))
			if err != nil {
				t.Fatalf("PutExact() error = %v", err)
			}
			if attempts.Load() != 3 || object.Discriminator != (backupobject.Discriminator{
				Kind: backupobject.DiscriminatorETag, Value: `"wire-etag"`,
			}) {
				t.Fatalf("attempts/discriminator = %d/%#v", attempts.Load(), object.Discriminator)
			}
		})
	}
}

// Rationale: redirects must never forward signed S3 requests to another
// authority and the caller-visible error must remain the fixed taxonomy text.
func TestWireRedirectIsRejectedAndRedacted(t *testing.T) {
	body := []byte("wire artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	var redirected atomic.Int32
	target := newIPv4Server(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Add(1)
	}))
	defer target.Close()
	source := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Location", target.URL+"/signed-material")
		response.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	config := testConfig()
	config.Endpoint = source.URL
	adapter, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = adapter.PutExact(context.Background(), artifact, bytes.NewReader(body))
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindValidationFailed || redirected.Load() != 0 ||
		!strings.Contains(err.Error(), "provider contract was rejected") ||
		strings.Contains(err.Error(), source.URL) || strings.Contains(err.Error(), target.URL) {
		t.Fatalf("redirect error = %v, kind = %v/%t, target hits = %d", err, kind, ok, redirected.Load())
	}
}

// Rationale: restore and delete correctness depends on the actual ETag
// conditionals and response body bytes emitted by the pinned SDK transport.
func TestWireGetAndDeleteExactConditionalsAndBody(t *testing.T) {
	body := []byte("wire artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	object := backupobject.Object{Artifact: artifact, Discriminator: backupobject.Discriminator{
		Kind: backupobject.DiscriminatorETag, Value: `"wire-etag"`,
	}}
	var headCalls atomic.Int32
	var deleted atomic.Bool
	server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			if request.Header.Get("If-Match") != object.Discriminator.Value {
				t.Errorf("GET If-Match = %q", request.Header.Get("If-Match"))
			}
			writeObjectHeaders(response, artifact, object.Discriminator.Value)
			_, _ = response.Write(body)
		case http.MethodHead:
			headCalls.Add(1)
			if deleted.Load() {
				response.WriteHeader(http.StatusNotFound)
				return
			}
			if request.Header.Get("If-Match") != object.Discriminator.Value {
				t.Errorf("HEAD If-Match = %q", request.Header.Get("If-Match"))
			}
			writeObjectHeaders(response, artifact, object.Discriminator.Value)
		case http.MethodDelete:
			if request.Header.Get("If-Match") != object.Discriminator.Value {
				t.Errorf("DELETE If-Match = %q", request.Header.Get("If-Match"))
			}
			deleted.Store(true)
			response.WriteHeader(http.StatusNoContent)
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	adapter := newWireAdapter(t, server, true)
	var restored bytes.Buffer
	if err := adapter.GetExact(context.Background(), object, &restored); err != nil {
		t.Fatalf("GetExact() error = %v", err)
	}
	if !bytes.Equal(restored.Bytes(), body) {
		t.Fatalf("restored body = %q", restored.Bytes())
	}
	if err := adapter.DeleteExact(context.Background(), object); err != nil {
		t.Fatalf("DeleteExact() error = %v", err)
	}
	if !deleted.Load() || headCalls.Load() != 2 {
		t.Fatalf("delete/head state = %t/%d", deleted.Load(), headCalls.Load())
	}
}

// Rationale: multipart immutability is carried by the Complete request itself,
// so its conditional and size must be confirmed on the actual HTTP wire.
func TestWireCompleteMultipartConditional(t *testing.T) {
	body := []byte("wire artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Query().Get("uploadId") != "upload-1" ||
			request.Header.Get("If-None-Match") != "*" ||
			request.Header.Get("X-Amz-Mp-Object-Size") != strconv.Itoa(len(body)) {
			t.Errorf("complete method/query/conditional/size = %s/%q/%q/%q", request.Method,
				request.URL.RawQuery, request.Header.Get("If-None-Match"), request.Header.Get("X-Amz-Mp-Object-Size"))
		}
		response.Header().Set("Content-Type", "application/xml")
		response.Header().Set("X-Amz-Version-Id", "null")
		_, _ = io.WriteString(response, `<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>groundplane-backups</Bucket><Key>`+
			artifact.Key+`</Key><ETag>"wire-etag"</ETag></CompleteMultipartUploadResult>`)
	}))
	defer server.Close()
	adapter := newWireAdapter(t, server, true)
	object, restart, err := adapter.completeMultipart(context.Background(), artifact, bytes.NewReader(body), "upload-1", []types.CompletedPart{{
		ETag: aws.String(`"part-etag"`), PartNumber: aws.Int32(1),
	}})
	if err != nil || restart || object.Discriminator != (backupobject.Discriminator{
		Kind: backupobject.DiscriminatorVersionID, Value: "null",
	}) {
		t.Fatalf("completeMultipart() = %#v, restart = %t, error = %v", object, restart, err)
	}
}

// Rationale: per-part integrity is an HTTP contract, not merely an SDK input;
// the actual UploadPart request must carry the explicit MD5 and exact body.
func TestWireUploadPartContentMD5(t *testing.T) {
	body := []byte("wire multipart part")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	var calls atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		wireBody, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read UploadPart body: %v", err)
		}
		if request.Method != http.MethodPut || request.URL.Query().Get("uploadId") != "upload-1" ||
			request.URL.Query().Get("partNumber") != "1" || request.Header.Get("Content-MD5") != md5Base64(body) ||
			!bytes.Equal(wireBody, body) {
			t.Errorf("UploadPart request = %s %q md5=%q body=%q", request.Method, request.URL.RawQuery,
				request.Header.Get("Content-MD5"), wireBody)
		}
		response.Header().Set("ETag", `"part-etag"`)
	}))
	defer server.Close()
	adapter := newWireAdapter(t, server, true)
	parts, err := adapter.uploadParts(context.Background(), artifact, bytes.NewReader(body), "upload-1")
	if err != nil || calls.Load() != 1 || len(parts) != 1 || aws.ToString(parts[0].ETag) != `"part-etag"` {
		t.Fatalf("uploadParts() = %#v, error = %v, calls = %d", parts, err, calls.Load())
	}
}

// Rationale: reserved-key cleanup depends on the SDK's real marker spelling,
// full pagination, exact-key filtering, abort query, and final empty re-list.
func TestWireCleanupMultipartPaginationAndAbortMarkers(t *testing.T) {
	artifact := testArtifact(nil, backupobject.EncryptionNone)
	var listCalls atomic.Int32
	var abortOne atomic.Int32
	var abortTwo atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		if request.Method == http.MethodGet && query.Has("uploads") {
			call := listCalls.Add(1)
			if query.Get("prefix") != artifact.Key {
				t.Errorf("list prefix = %q", query.Get("prefix"))
			}
			switch call {
			case 1:
				if query.Get("key-marker") != "" || query.Get("upload-id-marker") != "" {
					t.Errorf("first list markers = %q/%q", query.Get("key-marker"), query.Get("upload-id-marker"))
				}
				writeMultipartList(response, artifact.Key, true, artifact.Key, "upload-1",
					[2]string{artifact.Key, "upload-1"}, [2]string{artifact.Key + ".operator", "leave"})
			case 2:
				if query.Get("key-marker") != artifact.Key || query.Get("upload-id-marker") != "upload-1" {
					t.Errorf("second list markers = %q/%q", query.Get("key-marker"), query.Get("upload-id-marker"))
				}
				writeMultipartList(response, artifact.Key, false, "", "", [2]string{artifact.Key, "upload-2"})
			case 3:
				if query.Get("key-marker") != "" || query.Get("upload-id-marker") != "" {
					t.Errorf("verification list retained markers = %q/%q",
						query.Get("key-marker"), query.Get("upload-id-marker"))
				}
				writeMultipartList(response, artifact.Key, false, "", "")
			default:
				t.Errorf("unexpected list call %d", call)
			}
			return
		}
		if request.Method == http.MethodDelete && query.Get("uploadId") != "" {
			switch query.Get("uploadId") {
			case "upload-1":
				abortOne.Add(1)
			case "upload-2":
				abortTwo.Add(1)
			default:
				t.Errorf("unexpected abort upload id %q", query.Get("uploadId"))
			}
			if request.URL.Path != "/groundplane-backups/"+artifact.Key {
				t.Errorf("abort path = %q", request.URL.Path)
			}
			response.WriteHeader(http.StatusNoContent)
			return
		}
		response.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer server.Close()
	if err := newWireAdapter(t, server, true).cleanupExactUploads(context.Background(), artifact.Key); err != nil {
		t.Fatalf("cleanupExactUploads() error = %v", err)
	}
	if listCalls.Load() != 3 || abortOne.Load() != 1 || abortTwo.Load() != 1 {
		t.Fatalf("wire calls = list %d abort %d/%d", listCalls.Load(), abortOne.Load(), abortTwo.Load())
	}
}

// Rationale: mutation retry ownership is split between SDK and adapter. Each
// Create call must be one wire attempt while the adapter performs exactly one
// reconciled retry and proves no upload remains after both ambiguities.
func TestWireCreateMultipartUsesOneAttemptPerMutation(t *testing.T) {
	artifact := testArtifact(nil, backupobject.EncryptionNone)
	var createCalls atomic.Int32
	var listCalls atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		if request.Method == http.MethodPost && query.Has("uploads") {
			createCalls.Add(1)
			writeProviderError(response, http.StatusInternalServerError, "InternalError")
			return
		}
		if request.Method == http.MethodGet && query.Has("uploads") {
			listCalls.Add(1)
			writeMultipartList(response, artifact.Key, false, "", "")
			return
		}
		response.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer server.Close()
	_, err := newWireAdapter(t, server, true).createMultipart(context.Background(), artifact)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStorageUnavailable {
		t.Fatalf("createMultipart() error = %v, kind = %v, %t", err, kind, ok)
	}
	if createCalls.Load() != 2 || listCalls.Load() != 4 {
		t.Fatalf("wire calls = create %d list %d, want 2/4", createCalls.Load(), listCalls.Load())
	}
}

// Rationale: each ambiguous Complete mutation is one wire attempt; the sole
// repeat is adapter-owned and terminal return follows Head plus abort/re-list.
func TestWireCompleteMultipartUsesOneAttemptPerMutation(t *testing.T) {
	body := []byte("wire artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	var completeCalls atomic.Int32
	var headCalls atomic.Int32
	var listCalls atomic.Int32
	var abortCalls atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		switch {
		case request.Method == http.MethodPost && query.Get("uploadId") == "upload-1":
			completeCalls.Add(1)
			writeProviderError(response, http.StatusInternalServerError, "InternalError")
		case request.Method == http.MethodHead:
			headCalls.Add(1)
			response.WriteHeader(http.StatusNotFound)
		case request.Method == http.MethodGet && query.Has("uploads"):
			call := listCalls.Add(1)
			if call < 3 {
				writeMultipartList(response, artifact.Key, false, "", "", [2]string{artifact.Key, "upload-1"})
			} else {
				writeMultipartList(response, artifact.Key, false, "", "")
			}
		case request.Method == http.MethodDelete && query.Get("uploadId") == "upload-1":
			abortCalls.Add(1)
			response.WriteHeader(http.StatusNoContent)
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	_, restart, err := newWireAdapter(t, server, true).completeMultipart(
		context.Background(), artifact, bytes.NewReader(body), "upload-1",
		[]types.CompletedPart{{ETag: aws.String(`"part-etag"`), PartNumber: aws.Int32(1)}},
	)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStorageUnavailable || restart {
		t.Fatalf("completeMultipart() restart = %t, error = %v, kind = %v, %t", restart, err, kind, ok)
	}
	if completeCalls.Load() != 2 || headCalls.Load() != 2 || listCalls.Load() != 3 || abortCalls.Load() != 1 {
		t.Fatalf("wire calls = complete %d head %d list %d abort %d",
			completeCalls.Load(), headCalls.Load(), listCalls.Load(), abortCalls.Load())
	}
}

// Rationale: Delete retries are adapter-owned. Each mutation is one wire
// attempt and a second attempt occurs only after exact Head reconciliation.
func TestWireDeleteUsesOneAttemptPerMutation(t *testing.T) {
	body := []byte("wire artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	object := backupobject.Object{Artifact: artifact, Discriminator: backupobject.Discriminator{
		Kind: backupobject.DiscriminatorETag, Value: `"wire-etag"`,
	}}
	var deleteCalls atomic.Int32
	var headCalls atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodHead:
			headCalls.Add(1)
			writeObjectHeaders(response, artifact, object.Discriminator.Value)
		case http.MethodDelete:
			deleteCalls.Add(1)
			writeProviderError(response, http.StatusInternalServerError, "InternalError")
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	err := newWireAdapter(t, server, true).DeleteExact(context.Background(), object)
	if kind, ok := errs.KindOf(err); !ok || kind != errs.KindStorageUnavailable {
		t.Fatalf("DeleteExact() error = %v, kind = %v, %t", err, kind, ok)
	}
	if deleteCalls.Load() != 2 || headCalls.Load() != 3 {
		t.Fatalf("wire calls = delete %d head %d, want 2/3", deleteCalls.Load(), headCalls.Load())
	}
}

// Rationale: VersionId identity is defined by header/query presence. An empty
// provider value must outrank ETag and remain an explicitly present empty
// versionId query on every exact version-addressed operation.
func TestWirePresentEmptyVersionIDRoundTrips(t *testing.T) {
	body := []byte("empty version artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	var deleted atomic.Bool
	var versionedRequests atomic.Int32
	server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut {
			query := request.URL.Query()
			if !query.Has("versionId") || query.Get("versionId") != "" {
				t.Errorf("%s versionId query presence/value = %t/%q; raw = %q",
					request.Method, query.Has("versionId"), query.Get("versionId"), request.URL.RawQuery)
			}
			versionedRequests.Add(1)
		}
		switch request.Method {
		case http.MethodPut:
			response.Header()["X-Amz-Version-Id"] = []string{""}
			response.Header().Set("ETag", `"must-not-win"`)
		case http.MethodHead:
			if deleted.Load() {
				response.WriteHeader(http.StatusNotFound)
				return
			}
			writeObjectHeaders(response, artifact, `"must-not-win"`)
			response.Header()["X-Amz-Version-Id"] = []string{""}
		case http.MethodGet:
			writeObjectHeaders(response, artifact, `"must-not-win"`)
			response.Header()["X-Amz-Version-Id"] = []string{""}
			_, _ = response.Write(body)
		case http.MethodDelete:
			deleted.Store(true)
			response.WriteHeader(http.StatusNoContent)
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	adapter := newWireAdapter(t, server, true)
	object, err := adapter.PutExact(context.Background(), artifact, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("PutExact() error = %v", err)
	}
	want := backupobject.Discriminator{Kind: backupobject.DiscriminatorVersionID, Value: ""}
	if object.Discriminator != want {
		t.Fatalf("PutExact() discriminator = %#v, want %#v", object.Discriminator, want)
	}
	head, err := adapter.HeadExact(context.Background(), artifact, &want)
	if err != nil || !head.Present || head.Object.Discriminator != want {
		t.Fatalf("HeadExact() = %#v, error = %v", head, err)
	}
	var restored bytes.Buffer
	if err := adapter.GetExact(context.Background(), object, &restored); err != nil {
		t.Fatalf("GetExact() error = %v", err)
	}
	if !bytes.Equal(restored.Bytes(), body) {
		t.Fatalf("restored body = %q", restored.Bytes())
	}
	if err := adapter.DeleteExact(context.Background(), object); err != nil {
		t.Fatalf("DeleteExact() error = %v", err)
	}
	if !deleted.Load() || versionedRequests.Load() != 5 {
		t.Fatalf("empty VersionId replay = requests %d, deleted %t", versionedRequests.Load(), deleted.Load())
	}
}

// Rationale: provider discriminator bounds must be enforced after real SDK
// header decoding, with a fixed rejection and no ETag fallback for a present
// oversized VersionId.
func TestWireRejectsOversizedProviderDiscriminatorEvidence(t *testing.T) {
	body := []byte("oversized discriminator artifact")
	for _, test := range []struct {
		name      string
		versionID *string
		etag      string
	}{
		{"VersionId", aws.String(strings.Repeat("v", 1025)), `"must-not-win"`},
		{"ETag", nil, strings.Repeat("e", 1025)},
	} {
		t.Run(test.name, func(t *testing.T) {
			artifact := testArtifact(body, backupobject.EncryptionNone)
			server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				_, _ = io.Copy(io.Discard, request.Body)
				if test.versionID != nil {
					response.Header().Set("X-Amz-Version-Id", *test.versionID)
				}
				response.Header().Set("ETag", test.etag)
			}))
			defer server.Close()
			_, err := newWireAdapter(t, server, true).PutExact(
				context.Background(), artifact, bytes.NewReader(body),
			)
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindValidationFailed ||
				!strings.Contains(err.Error(), "provider contract was rejected") ||
				strings.Contains(err.Error(), strings.Repeat("v", 64)) ||
				strings.Contains(err.Error(), strings.Repeat("e", 64)) {
				t.Fatalf("PutExact() error = %v, kind = %v, %t", err, kind, ok)
			}
		})
	}
}

func newWireAdapter(t *testing.T, server *httptest.Server, pathStyle bool) *Adapter {
	t.Helper()
	endpoint, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	config := testConfig()
	config.Endpoint = "http://s3.test:" + endpoint.Port()
	config.PathStyle = pathStyle
	awsConfig := newAWSConfig(config)
	httpClient := awsConfig.HTTPClient.(*http.Client)
	transport := httpClient.Transport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	httpClient.Transport = transport
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(config.Endpoint)
		options.UsePathStyle = pathStyle
	})
	return &Adapter{config: config, client: client}
}

func newIPv4Server(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on IPv4 loopback: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	return server
}

func wireEndpointHost(server *httptest.Server) string {
	parsed, _ := url.Parse(server.URL)
	return "s3.test:" + parsed.Port()
}

func md5Base64(body []byte) string {
	digest := md5.Sum(body)
	return base64.StdEncoding.EncodeToString(digest[:])
}

func writeObjectHeaders(response http.ResponseWriter, artifact backupobject.Artifact, etag string) {
	response.Header().Set("Content-Length", strconv.FormatUint(artifact.Evidence.StoredSizeBytes, 10))
	response.Header().Set("ETag", etag)
	for key, value := range artifact.Metadata() {
		response.Header().Set("X-Amz-Meta-"+key, value)
	}
}

func writeProviderError(response http.ResponseWriter, status int, code string) {
	response.Header().Set("Content-Type", "application/xml")
	response.WriteHeader(status)
	_, _ = io.WriteString(response, "<Error><Code>"+code+"</Code><Message>private</Message></Error>")
}

func writeMultipartList(
	response http.ResponseWriter,
	prefix string,
	truncated bool,
	nextKeyMarker string,
	nextUploadIDMarker string,
	uploads ...[2]string,
) {
	response.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprintf(response,
		`<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>groundplane-backups</Bucket><Prefix>%s</Prefix><KeyMarker></KeyMarker><UploadIdMarker></UploadIdMarker><NextKeyMarker>%s</NextKeyMarker><NextUploadIdMarker>%s</NextUploadIdMarker><MaxUploads>1000</MaxUploads><IsTruncated>%t</IsTruncated>`,
		prefix, nextKeyMarker, nextUploadIDMarker, truncated,
	)
	for _, upload := range uploads {
		_, _ = fmt.Fprintf(response, "<Upload><Key>%s</Key><UploadId>%s</UploadId></Upload>", upload[0], upload[1])
	}
	_, _ = io.WriteString(response, "</ListMultipartUploadsResult>")
}
