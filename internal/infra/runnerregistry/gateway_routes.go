package runnerregistry

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

type registryPath uint8

const (
	invalidPath registryPath = iota
	registryRoot
	manifestPath
	blobPath
	beginUpload
	blobUpload
)

var (
	registryDigest      = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	registryTag         = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
	registryUploadID    = regexp.MustCompile(`^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$`)
	registryUploadState = regexp.MustCompile(`^[A-Za-z0-9_=-]+$`)
)

func canonicalRegistryPath(location *url.URL) bool {
	return location != nil && location.User == nil && location.Opaque == "" && !location.ForceQuery &&
		strings.HasPrefix(
			location.Path,
			"/v2/",
		) && len(location.Path) <= 2048 && location.EscapedPath() == location.Path
}

func scopedRegistryPath(path, repository string) registryPath {
	if path == "/v2/" {
		return registryRoot
	}
	remainder, ok := strings.CutPrefix(path, "/v2/"+repository+"/")
	if !ok {
		return invalidPath
	}
	kind, name, found := strings.Cut(remainder, "/")
	if !found {
		return invalidPath
	}
	switch kind {
	case "manifests":
		if registryDigest.MatchString(name) || registryTag.MatchString(name) {
			return manifestPath
		}
	case "blobs":
		if name == "uploads/" {
			return beginUpload
		}
		if id, found := strings.CutPrefix(name, "uploads/"); found && registryUploadID.MatchString(id) {
			return blobUpload
		}
		if registryDigest.MatchString(name) {
			return blobPath
		}
	}
	return invalidPath
}

func registryRequest(request *http.Request, grant Grant) (url.Values, bool) {
	if request.URL.IsAbs() || request.URL.Host != "" || request.URL.Fragment != "" {
		return nil, false
	}
	query, err := url.ParseQuery(request.URL.RawQuery)
	if err != nil {
		return nil, false
	}
	read := request.Method == http.MethodGet || request.Method == http.MethodHead
	if read && (request.ContentLength != 0 || len(request.TransferEncoding) != 0) {
		return nil, false
	}
	pull, push := grant.Actions&Pull != 0, grant.Actions&Push != 0
	switch scopedRegistryPath(request.URL.Path, grant.Repository) {
	case registryRoot:
		return query, read && len(query) == 0
	case manifestPath:
		return query, (read && pull || request.Method == http.MethodPut && push) && len(query) == 0
	case blobPath:
		return query, read && pull && len(query) == 0
	case beginUpload:
		return query, request.Method == http.MethodPost && push && beginUploadQuery(query, grant.Repository, pull)
	case blobUpload:
		return query, push && uploadRequest(request.Method, query)
	default:
		return nil, false
	}
}

func beginUploadQuery(query url.Values, repository string, pull bool) bool {
	switch len(query) {
	case 0:
		return true
	case 1:
		return singleMatching(query, "digest", registryDigest)
	case 2:
		from := query["from"]
		return pull && len(from) == 1 && from[0] == repository && singleMatching(query, "mount", registryDigest)
	default:
		return false
	}
}

func uploadRequest(method string, query url.Values) bool {
	stateCount := 0
	if query.Has("_state") {
		if !singleMatching(query, "_state", registryUploadState) {
			return false
		}
		stateCount++
	}
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPatch:
		return len(query) == stateCount
	case http.MethodPut:
		return len(query) == stateCount+1 && singleMatching(query, "digest", registryDigest)
	default:
		return false
	}
}

func registryLocation(kind registryPath, query url.Values) bool {
	switch kind {
	case blobUpload:
		return uploadRequest(http.MethodGet, query)
	case blobPath, manifestPath:
		return len(query) == 0
	default:
		return false
	}
}

func singleMatching(query url.Values, key string, pattern *regexp.Regexp) bool {
	values := query[key]
	return len(values) == 1 && pattern.MatchString(values[0])
}
