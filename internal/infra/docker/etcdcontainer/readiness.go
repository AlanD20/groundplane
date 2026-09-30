package etcdcontainer

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// An open TCP listener alone does not prove that the store can serve requests.
func probeReadiness(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+endpoint+"/health", nil)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}).Do(
		request,
	)
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	defer response.Body.Close()
	var result struct {
		Health string `json:"health"`
	}
	if response.StatusCode != http.StatusOK ||
		json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result) != nil ||
		result.Health != "true" {
		return errs.New(errs.KindStorageUnavailable, "etcd container: health endpoint is not ready")
	}
	return nil
}
