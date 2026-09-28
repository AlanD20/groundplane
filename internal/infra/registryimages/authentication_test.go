package registryimages

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type registryTransport func(*http.Request) (*http.Response, error)

func (transport registryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

// IMG-01: exercise actual challenge, manifest/config selection and blob redirect
// handling. Public auth must not expose GP credentials, even to token/CDN hosts.
func TestPublicResolutionPinsContentWithoutLeakingManagedCredentials(t *testing.T) {
	config := []byte(`{"architecture":"amd64","os":"linux"}`)
	manifest, err := json.Marshal(ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    ocispec.Descriptor{Digest: digest.FromBytes(config), Size: int64(len(config))},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Include schemaVersion independently of the resolver's selected output.
	manifest = []byte(strings.Replace(string(manifest), `"schemaVersion":0`, `"schemaVersion":2`, 1))
	for _, requested := range []string{"nginx:latest", "ghcr.io/example/app:release"} {
		t.Run(requested, func(t *testing.T) {
			tokens, cdn := 0, 0
			transport := registryTransport(func(request *http.Request) (*http.Response, error) {
				status, body, headers := http.StatusOK, "", make(http.Header)
				switch request.URL.Host {
				case "token.example":
					tokens++
					if request.Header.Get("Authorization") != "" ||
						!strings.HasSuffix(request.URL.Query().Get("scope"), ":pull") {
						t.Fatal("token request received credentials or excess scope")
					}
					body = `{"token":"anonymous-pull-token"}`
				case "cdn.example":
					cdn++
					if request.Header.Get("Authorization") != "" {
						t.Fatal("CDN received registry credentials")
					}
					body = string(config)
				case "registry-1.docker.io", "ghcr.io":
					if strings.HasPrefix(request.Header.Get("Authorization"), "Basic ") {
						t.Fatal("public registry received GP credentials")
					}
					if request.Header.Get("Authorization") == "" {
						status = http.StatusUnauthorized
						headers.Set(
							"WWW-Authenticate",
							`Bearer realm="https://token.example/token",service="registry",scope="repository:example/app:pull,push"`,
						)
					} else if strings.Contains(request.URL.Path, "/manifests/") {
						body = string(manifest)
					} else {
						status = http.StatusTemporaryRedirect
						headers.Set("Location", "https://cdn.example/blob")
					}
				default:
					t.Fatalf("unexpected request host %s", request.URL.Host)
				}
				return &http.Response{
					StatusCode: status,
					Header:     headers,
					Body:       io.NopCloser(strings.NewReader(body)),
					Request:    request,
				}, nil
			})
			client := &Client{
				http: &http.Client{
					Transport:     transport,
					CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
				},
				username:     "gp",
				password:     "private",
				architecture: "amd64",
			}
			plan, err := client.Resolve(context.Background(), requested)
			if err != nil {
				t.Fatal(err)
			}
			if plan.ConfigDigest != digest.FromBytes(config).String() ||
				plan.ManifestDigest != digest.FromBytes(manifest).String() ||
				tokens != 2 ||
				cdn != 1 {
				t.Fatalf("wrong selected bytes or auth journey: %#v tokens=%d cdn=%d", plan, tokens, cdn)
			}
		})
	}
}

// IMG-01: a managed registry must not redirect its credentials, and public
// challenges must not downgrade transport or gain access to private auth.
func TestRegistryRejectsAuthDowngradeAndManagedRedirect(t *testing.T) {
	for _, challenge := range []string{`Basic realm="private"`, `Bearer realm="http://token.example/token"`, `Bearer realm="https://user:pass@token.example/token"`} {
		client := &Client{}
		request, _ := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			"https://public.example/v2/app/manifests/one",
			nil,
		)
		if _, err := client.anonymousToken(request, challenge, "app"); err == nil {
			t.Fatalf("unsafe challenge accepted: %s", challenge)
		}
	}
	client := &Client{
		username: "gp",
		password: "private",
		http: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			Transport: registryTransport(func(request *http.Request) (*http.Response, error) {
				if request.URL.Host != imagefetch.RegistryAuthority {
					t.Fatal("managed credentials followed a redirect")
				}
				return &http.Response{
					StatusCode: http.StatusTemporaryRedirect,
					Header:     http.Header{"Location": []string{"https://public.example/blob"}},
					Body:       io.NopCloser(strings.NewReader("")),
					Request:    request,
				}, nil
			}),
		},
	}
	named, err := imagefetch.ParseRequested(imagefetch.RegistryAuthority + "/app:one")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.document(context.Background(), named, "manifests", "one"); err == nil {
		t.Fatal("managed redirect accepted")
	}
}
