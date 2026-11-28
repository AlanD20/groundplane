package githubapp

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maxArtifactBytes = 128 << 10
	maxResultBytes   = 8 << 10
	resultMember     = "build-result.json"
)

type BuildResult struct {
	RequestID    string `json:"request_id"`
	SourceCommit string `json:"source_commit"`
	ImageDigest  string `json:"image_digest"`
	RunID        int64  `json:"run_id"`
	RunAttempt   int64  `json:"run_attempt"`
}

type artifactRecord struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Expired     bool   `json:"expired"`
	Size        int64  `json:"size_in_bytes"`
	Digest      string `json:"digest"`
	WorkflowRun struct {
		ID               int64 `json:"id"`
		RepositoryID     int64 `json:"repository_id"`
		HeadRepositoryID int64 `json:"head_repository_id"`
	} `json:"workflow_run"`
}

// BuildResult fetches the run's uniquely named digest-verified result archive.
// The Controller must still match the request and source commit to its intent.
func (client *Client) BuildResult(
	ctx context.Context,
	scope Scope,
	runID, attempt int64,
	artifactName string,
) (BuildResult, error) {
	if runID <= 0 || attempt <= 0 || !requestPattern.MatchString(artifactName) {
		return BuildResult{}, errs.New(errs.KindValidationFailed, "GitHub build result identity is invalid")
	}
	access, err := client.installation(ctx, scope)
	if err != nil {
		return BuildResult{}, err
	}
	artifact, err := client.findArtifact(ctx, access, runID, artifactName)
	if err != nil {
		return BuildResult{}, err
	}
	data, err := client.downloadArtifact(ctx, access, artifact)
	if err != nil {
		return BuildResult{}, err
	}
	defer clear(data)
	return readBuildResult(data, runID, attempt)
}

func (client *Client) findArtifact(
	ctx context.Context,
	access installation,
	runID int64,
	artifactName string,
) (artifactRecord, error) {
	query := url.Values{"name": {artifactName}, "per_page": {"2"}}
	path := access.path() + "/actions/runs/" + id(runID) + "/artifacts?" + query.Encode()
	var result struct {
		Total     int              `json:"total_count"`
		Artifacts []artifactRecord `json:"artifacts"`
	}
	if err := client.requestJSON(ctx, http.MethodGet, path, access.token, nil, http.StatusOK, &result); err != nil {
		return artifactRecord{}, err
	}
	if result.Total == 0 && len(result.Artifacts) == 0 {
		return artifactRecord{}, errs.New(errs.KindRequestNotFound, "GitHub build result artifact was not found")
	}
	if result.Total != 1 || len(result.Artifacts) != 1 {
		return artifactRecord{}, errs.New(errs.KindStateConflict, "GitHub build result artifact is ambiguous")
	}
	item := result.Artifacts[0]
	if item.ID <= 0 || item.Name != artifactName || item.Expired || item.Size <= 0 || item.Size > maxArtifactBytes ||
		!digestPattern.MatchString(item.Digest) || item.WorkflowRun.ID != runID ||
		item.WorkflowRun.RepositoryID != access.repo.ID || item.WorkflowRun.HeadRepositoryID != access.repo.ID {
		return artifactRecord{}, errs.New(
			errs.KindStateConflict,
			"GitHub build result artifact lacks valid run identity or digest",
		)
	}
	return item, nil
}

func (client *Client) downloadArtifact(
	ctx context.Context,
	access installation,
	artifact artifactRecord,
) ([]byte, error) {
	path := access.path() + "/actions/artifacts/" + id(artifact.ID) + "/zip"
	response, err := client.request(ctx, http.MethodGet, path, access.token, nil)
	if err != nil {
		return nil, err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusFound {
		return nil, responseError(response)
	}
	location := response.Header.Get("Location")
	if !validArtifactLocation(location) {
		return nil, errs.New(errs.KindStateConflict, "GitHub provided an unrecognized artifact download location")
	}
	req, err := client.newRequest(ctx, http.MethodGet, location, nil)
	if err != nil {
		return nil, err
	}
	// The API's bearer token stays at api.github.com. This separate request
	// uses only the short-lived download URL and does not follow redirects.
	response, err = client.send(ctx, req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, responseError(response)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxArtifactBytes+1))
	if err != nil || len(data) > maxArtifactBytes {
		clear(data)
		return nil, errs.New(errs.KindRequestUnavailable, "GitHub build result archive is unreadable or oversized")
	}
	digest := sha256.Sum256(data)
	if int64(len(data)) != artifact.Size || "sha256:"+hex.EncodeToString(digest[:]) != artifact.Digest {
		clear(data)
		return nil, errs.New(
			errs.KindStateConflict,
			"GitHub build result archive differs from its recorded digest or size",
		)
	}
	return data, nil
}

func validArtifactLocation(location string) bool {
	if len(location) == 0 || len(location) > 8192 {
		return false
	}
	parsed, err := url.Parse(location)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Opaque != "" || parsed.Fragment != "" ||
		parsed.Path == "" || (parsed.Port() != "" && parsed.Port() != "443") {
		return false
	}
	host := strings.ToLower(parsed.Hostname())
	return strings.HasSuffix(host, ".blob.core.windows.net") ||
		strings.HasSuffix(host, ".actions.githubusercontent.com")
}

func readBuildResult(data []byte, runID, attempt int64) (BuildResult, error) {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil || len(archive.File) != 1 {
		return BuildResult{}, errs.New(
			errs.KindStateConflict,
			"GitHub build result must contain exactly one metadata file",
		)
	}
	file := archive.File[0]
	if file.Name != resultMember || !file.Mode().IsRegular() || file.UncompressedSize64 == 0 ||
		file.UncompressedSize64 > maxResultBytes {
		return BuildResult{}, errs.New(errs.KindStateConflict, "GitHub build result metadata file is invalid")
	}
	reader, err := file.Open()
	if err != nil {
		return BuildResult{}, errs.New(errs.KindStateConflict, "GitHub build result metadata cannot be opened")
	}
	defer reader.Close()
	var result BuildResult
	if err := decodeBoundedJSON(reader, maxResultBytes, &result); err != nil {
		return BuildResult{}, err
	}
	if result.RunID != runID || result.RunAttempt != attempt || !requestPattern.MatchString(result.RequestID) ||
		!commitPattern.MatchString(result.SourceCommit) || !digestPattern.MatchString(result.ImageDigest) {
		return BuildResult{}, errs.New(
			errs.KindStateConflict,
			"GitHub build result differs from the selected run attempt or has invalid metadata",
		)
	}
	return result, nil
}
