// Package softwarepreparation resolves and prepares immutable Groundplane
// Controller bundles and Agent images without activating either component.
package softwarepreparation

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	githubRepository = "AlanD20/groundplane"
	githubSourceURL  = "https://github.com/AlanD20/groundplane"

	maximumGitHubJSONBytes  = 2 << 20
	maximumSourceBytes      = 200 << 20
	maximumUnpackedBytes    = 512 << 20
	maximumBundleBytes      = 512 << 20
	maximumBundleMemberSize = 256 << 20
	maximumCatalogBytes     = 16 << 10
	minimumFreeBytes        = 10 << 30
	TaskTimeoutSeconds      = int64(3 * 60 * 60)

	commandTimeout = 2 * time.Minute
	buildTimeout   = 45 * time.Minute
	cleanupTimeout = 2 * time.Minute
	maximumOutput  = 16 << 20
)

var (
	commitPattern    = regexp.MustCompile(`^[0-9a-f]{40}$`)
	digestPattern    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	operationPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{7,63}$`)
	versionPattern   = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
)

type Selection string

const (
	SelectionController Selection = "controller"
	SelectionAgent      Selection = "agent"
	SelectionBoth       Selection = "both"
)

func (selection Selection) valid() bool {
	return selection == SelectionController || selection == SelectionAgent || selection == SelectionBoth
}

func (selection Selection) includesController() bool {
	return selection == SelectionController || selection == SelectionBoth
}

func (selection Selection) IncludesController() bool { return selection.includesController() }

func (selection Selection) IncludesAgent() bool { return selection.includesAgent() }

func (selection Selection) includesAgent() bool {
	return selection == SelectionAgent || selection == SelectionBoth
}

type SourceKind string

const (
	SourceRef SourceKind = "source_ref"
	Release   SourceKind = "release"
)

type Platform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
}

func HostPlatform() Platform {
	return Platform{OS: runtime.GOOS, Architecture: runtime.GOARCH}
}

func (platform Platform) valid() bool {
	return platform.OS == "linux" && (platform.Architecture == "amd64" || platform.Architecture == "arm64")
}

type ResolveInput struct {
	Selection   Selection  `json:"selection"`
	SourceKind  SourceKind `json:"source_kind"`
	OriginalRef string     `json:"original_ref"`
	Platform    Platform   `json:"platform"`
}

type ReleaseAsset struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// ResolvedRelease freezes one component-scoped published release. A combined
// preparation carries two of these records so later execution never consults
// the mutable component tags again.
type ResolvedRelease struct {
	OriginalRef string         `json:"original_ref"`
	ResolvedSHA string         `json:"resolved_sha"`
	CommitTime  time.Time      `json:"commit_time"`
	Assets      []ReleaseAsset `json:"assets"`
}

// ResolvedSource is safe to persist as Task authority. Prepare never resolves
// OriginalRef again; source builds use ResolvedSHA and release downloads use
// the exact asset identities captured here.
type ResolvedSource struct {
	Selection         Selection        `json:"selection"`
	SourceKind        SourceKind       `json:"source_kind"`
	OriginalRef       string           `json:"original_ref"`
	ResolvedSHA       string           `json:"resolved_sha,omitempty"`
	CommitTime        time.Time        `json:"commit_time,omitempty"`
	Platform          Platform         `json:"platform"`
	ControllerRelease *ResolvedRelease `json:"controller_release,omitempty"`
	AgentRelease      *ResolvedRelease `json:"agent_release,omitempty"`
}

func (source ResolvedSource) Equal(other ResolvedSource) bool {
	return source.Selection == other.Selection && source.SourceKind == other.SourceKind &&
		source.OriginalRef == other.OriginalRef && source.ResolvedSHA == other.ResolvedSHA &&
		source.CommitTime.Equal(other.CommitTime) && source.Platform == other.Platform &&
		equalResolvedRelease(source.ControllerRelease, other.ControllerRelease) &&
		equalResolvedRelease(source.AgentRelease, other.AgentRelease)
}

func ValidateResolvedSource(source ResolvedSource) error { return source.validate() }

func equalResolvedRelease(left, right *ResolvedRelease) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.OriginalRef == right.OriginalRef && left.ResolvedSHA == right.ResolvedSHA &&
		left.CommitTime.Equal(right.CommitTime) && slices.Equal(left.Assets, right.Assets)
}

type Input struct {
	OperationID             string         `json:"operation_id"`
	Source                  ResolvedSource `json:"source"`
	ControllerToolCatalog   []byte         `json:"controller_tool_catalog,omitempty"`
	ControllerCatalogSHA256 string         `json:"controller_catalog_sha256,omitempty"`
}

type Artifact struct {
	Reference      string   `json:"reference"`
	ManifestDigest string   `json:"manifest_digest"`
	ConfigDigest   string   `json:"config_digest"`
	Platform       Platform `json:"platform"`
}

type ControllerResult struct {
	Artifact       Artifact `json:"artifact"`
	BinarySHA256   string   `json:"binary_sha256"`
	MetadataSHA256 string   `json:"metadata_sha256"`
	CLISHA256      string   `json:"cli_sha256"`
}

type AgentResult struct {
	Artifact          Artifact `json:"artifact"`
	UpstreamReference string   `json:"upstream_reference,omitempty"`
}

type Result struct {
	Source     ResolvedSource    `json:"source"`
	Controller *ControllerResult `json:"controller,omitempty"`
	Agent      *AgentResult      `json:"agent,omitempty"`
}

func (result Result) Equal(other Result) bool {
	return result.Source.Equal(other.Source) && equalControllerResult(result.Controller, other.Controller) &&
		equalAgentResult(result.Agent, other.Agent)
}

func equalControllerResult(left, right *ControllerResult) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func equalAgentResult(left, right *AgentResult) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

type Phase string

const (
	PhaseAccepted            Phase = "accepted"
	PhasePreparing           Phase = "preparing"
	PhaseAgentPublished      Phase = "agent_published"
	PhaseControllerPublished Phase = "controller_published"
	PhaseVerified            Phase = "verified"
	PhaseFailed              Phase = "failed"
)

type Progress struct {
	Phase       Phase  `json:"phase"`
	Result      Result `json:"result"`
	ErrorCode   string `json:"error_code,omitempty"`
	ErrorDetail string `json:"error_detail,omitempty"`
}

func (progress Progress) Equal(other Progress) bool {
	return progress.Phase == other.Phase && progress.Result.Equal(other.Result) &&
		progress.ErrorCode == other.ErrorCode && progress.ErrorDetail == other.ErrorDetail
}

type Reporter func(context.Context, Progress) error

// Preparer hides fixed-repository retrieval, archive validation, containerized
// builds, managed-registry publication and operation-owned cleanup behind two
// methods. Durable Task ownership and retention stay with its caller.
type Preparer struct {
	commands      runner.Runner
	workspaceRoot string
	http          *http.Client
}

func NewPreparer(commands runner.Runner, workspaceRoot string) (*Preparer, error) {
	if commands == nil {
		return nil, errs.New(errs.KindInternal, "software preparation runner is required")
	}
	clean := filepath.Clean(workspaceRoot)
	if !filepath.IsAbs(clean) || clean != workspaceRoot {
		return nil, errs.New(errs.KindInternal, "software preparation workspace must be an absolute clean path")
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	status, statusOK := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 ||
		!statusOK || status.Uid != uint32(os.Geteuid()) {
		return nil, errs.New(errs.KindStateConflict, "software preparation workspace ownership is unsafe")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.IdleConnTimeout = time.Minute
	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Minute,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 5 || request.URL.Scheme != "https" || !trustedGitHubHost(request.URL.Hostname()) {
				return errs.New(errs.KindStateConflict, "software preparation download redirect is untrusted")
			}
			request.Header.Del("Authorization")
			return nil
		},
	}
	return &Preparer{commands: commands, workspaceRoot: clean, http: client}, nil
}

func trustedGitHubHost(host string) bool {
	return host == "api.github.com" || host == "github.com" || host == "codeload.github.com" ||
		strings.HasSuffix(host, ".githubusercontent.com")
}

func (source ResolvedSource) validate() error {
	if !source.Selection.valid() || (source.SourceKind != SourceRef && source.SourceKind != Release) ||
		!validOriginalRef(source.OriginalRef) || !source.Platform.valid() {
		return errs.New(errs.KindValidationFailed, "resolved software source is invalid")
	}
	if source.Platform != HostPlatform() {
		return errs.New(errs.KindValidationFailed, "software preparation platform does not match this host")
	}
	if source.SourceKind == SourceRef {
		if !commitPattern.MatchString(source.ResolvedSHA) || source.CommitTime.Before(time.Unix(0, 0)) ||
			source.CommitTime.Nanosecond() != 0 || source.ControllerRelease != nil || source.AgentRelease != nil {
			return errs.New(errs.KindValidationFailed, "resolved source-ref authority is invalid")
		}
		return nil
	}
	if source.ResolvedSHA != "" || !source.CommitTime.IsZero() ||
		!validReleaseTag(source.Selection, source.OriginalRef) {
		return errs.New(errs.KindValidationFailed, "published release tag is outside its component namespace")
	}
	if source.Selection.includesController() != (source.ControllerRelease != nil) ||
		source.Selection.includesAgent() != (source.AgentRelease != nil) {
		return errs.New(errs.KindValidationFailed, "published release component authority is incomplete")
	}
	if source.ControllerRelease != nil {
		if err := source.ControllerRelease.validate(SelectionController); err != nil {
			return err
		}
	}
	if source.AgentRelease != nil {
		if err := source.AgentRelease.validate(SelectionAgent); err != nil {
			return err
		}
	}
	return nil
}

func (release ResolvedRelease) validate(selection Selection) error {
	if !validReleaseTag(selection, release.OriginalRef) || !commitPattern.MatchString(release.ResolvedSHA) ||
		release.CommitTime.Before(time.Unix(0, 0)) || release.CommitTime.Nanosecond() != 0 || len(release.Assets) == 0 {
		return errs.New(errs.KindValidationFailed, "published release authority is invalid")
	}
	for _, asset := range release.Assets {
		if asset.Name == "" || asset.Size <= 0 || !digestPattern.MatchString(asset.SHA256) {
			return errs.New(errs.KindValidationFailed, "published release asset identity is invalid")
		}
	}
	return nil
}

func validOriginalRef(value string) bool {
	if value == "" || len(value) > 255 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range []byte(value) {
		if character < 0x21 || character == 0x7f {
			return false
		}
	}
	return true
}

func validReleaseTag(selection Selection, value string) bool {
	prefix := "v"
	if selection != SelectionBoth {
		prefix = string(selection) + "/v"
	}
	return strings.HasPrefix(value, prefix) && versionPattern.MatchString(strings.TrimPrefix(value, prefix))
}

func releaseVersion(tag string) string {
	value := tag[strings.LastIndexByte(tag, '/')+1:]
	return strings.TrimPrefix(value, "v")
}

func sha256Value(value []byte) string {
	digest := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", digest)
}

func checkContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
