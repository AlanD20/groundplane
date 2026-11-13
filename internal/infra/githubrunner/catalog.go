// Package githubrunner resolves only official, checksummed upstream releases.
package githubrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnersoftware"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Catalog struct{ client *http.Client }

func NewCatalog() *Catalog { return &Catalog{client: &http.Client{Timeout: 30 * time.Second}} }

type releaseRecord struct {
	Tag        string `json:"tag_name"`
	Draft      bool   `json:"draft"`
	Prerelease bool   `json:"prerelease"`
	Assets     []struct {
		Name   string `json:"name"`
		URL    string `json:"browser_download_url"`
		Digest string `json:"digest"`
		Size   int64  `json:"size"`
	} `json:"assets"`
}

func (catalog *Catalog) List(ctx context.Context) ([]runnersoftware.Release, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/repos/actions/runner/releases?per_page=50", nil)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "Groundplane-Runner-Updates")
	response, err := catalog.client.Do(req)
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errs.Newf(errs.KindStorageUnavailable, "GitHub Runner release lookup returned HTTP %d; retry after checking GitHub connectivity and API rate limits", response.StatusCode)
	}
	var records []releaseRecord
	if err := json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&records); err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	result := []runnersoftware.Release{}
	arch := map[string]string{"amd64": "x64", "arm64": "arm64"}[runtime.GOARCH]
	for _, record := range records {
		v := strings.TrimPrefix(record.Tag, "v")
		if record.Draft || record.Prerelease || !runnersoftware.ValidVersion(v) {
			continue
		}
		name := fmt.Sprintf("actions-runner-linux-%s-%s.tar.gz", arch, v)
		for _, asset := range record.Assets {
			if asset.Name != name {
				continue
			}
			r := runnersoftware.Release{Version: v, Architecture: runtime.GOARCH, URL: asset.URL, SHA256: strings.TrimPrefix(asset.Digest, "sha256:"), Size: asset.Size}
			if r.Validate() == nil {
				result = append(result, r)
			}
		}
	}
	return result, nil
}
func (catalog *Catalog) Resolve(ctx context.Context, version string) (runnersoftware.Release, error) {
	if !runnersoftware.ValidVersion(version) {
		return runnersoftware.Release{}, errs.New(errs.KindValidationFailed, "Select an exact GitHub Runner version, for example 2.337.0")
	}
	list, err := catalog.List(ctx)
	if err != nil {
		return runnersoftware.Release{}, err
	}
	for _, release := range list {
		if release.Version == version {
			return release, nil
		}
	}
	return runnersoftware.Release{}, errs.New(errs.KindValidationFailed, "Selected GitHub Runner version has no verified release for this host architecture")
}
