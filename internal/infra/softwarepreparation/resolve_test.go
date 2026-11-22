package softwarepreparation

import (
	"context"
	"strings"
	"testing"
)

func TestResolveRejectsConsoleSourceRefBeforeRemoteResolution(t *testing.T) {
	t.Parallel()
	preparer := &Preparer{}
	for _, selection := range []Selection{SelectionConsole, SelectionAll} {
		_, err := preparer.Resolve(context.Background(), ResolveInput{
			Selection: selection, SourceKind: SourceRef, OriginalRef: "main", Platform: HostPlatform(),
		})
		if err == nil || !strings.Contains(err.Error(), "published release") {
			t.Fatalf("Resolve(%s) error = %v", selection, err)
		}
	}
}
