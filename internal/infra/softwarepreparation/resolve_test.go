package softwarepreparation

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestResolveRejectsConsoleSourceRefBeforeRemoteResolution(t *testing.T) {
	t.Parallel()
	preparer := &Preparer{}
	_, err := preparer.Resolve(context.Background(), ResolveInput{
		Selection: SelectionConsole, SourceKind: SourceRef, OriginalRef: "main", Platform: HostPlatform(),
	})
	if err == nil || !strings.Contains(err.Error(), "published release") {
		t.Fatalf("Resolve Console-only source ref error = %v", err)
	}
}

// All captures one immutable commit for the three outputs, with no independent
// release lookups that can observe different source revisions.
func TestResolveAllCapturesOneSourceCommit(t *testing.T) {
	t.Parallel()
	commit := strings.Repeat("a", 40)
	requests := 0
	preparer := &Preparer{
		http: &http.Client{Transport: sourceRoundTrip(func(request *http.Request) (*http.Response, error) {
			requests++
			if request.URL.String() != "https://api.github.com/repos/AlanD20/groundplane/commits/main" {
				t.Fatalf("unexpected source authority lookup %s", request.URL)
			}
			payload := fmt.Sprintf(`{"sha":%q,"commit":{"committer":{"date":"2026-10-10T12:00:00Z"}}}`, commit)
			return &http.Response{
				StatusCode: http.StatusOK,
				Request:    request,
				Body:       io.NopCloser(strings.NewReader(payload)),
			}, nil
		})},
	}
	source, err := preparer.Resolve(context.Background(), ResolveInput{
		Selection: SelectionAll, SourceKind: SourceRef, OriginalRef: "main", Platform: HostPlatform(),
	})
	if err != nil || source.ResolvedSHA != commit || requests != 1 {
		t.Fatalf("All source = %v, requests = %d, err = %v", source.ResolvedSHA, requests, err)
	}
}

type sourceRoundTrip func(*http.Request) (*http.Response, error)

func (transport sourceRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}
