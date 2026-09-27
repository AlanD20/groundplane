package runnerproxy

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
)

// buildEngine is the dedicated, ownership-attested daemon, never host Docker.
// The caller must hold its current-epoch socket identity across all operations.
type buildEngine interface {
	Info(context.Context, client.InfoOptions) (client.SystemInfoResult, error)
	ImageList(context.Context, client.ImageListOptions) (client.ImageListResult, error)
	ImageBuild(context.Context, io.Reader, client.ImageBuildOptions) (client.ImageBuildResult, error)
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
}

// BuildExecutor serializes the complete build transaction. Other mutating
// proxy routes must share this same mutation lock rather than parallelize calls
// to the private daemon and invalidate the before/after ownership inventory.
type BuildExecutor struct {
	engine   buildEngine
	policy   *BuildPolicy
	ledger   *BuildLedger
	mutation *sync.Mutex
	daemonID string
}

func NewBuildExecutor(engine buildEngine, policy *BuildPolicy, ledger *BuildLedger,
	mutation *sync.Mutex, daemonID string,
) (*BuildExecutor, error) {
	if engine == nil || policy == nil || ledger == nil || mutation == nil || daemonID == "" ||
		len(daemonID) > 128 || policy.runnerID != ledger.runnerID || policy.epoch != ledger.epoch {
		return nil, buildEvidenceConflict()
	}
	return &BuildExecutor{engine: engine, policy: policy, ledger: ledger, mutation: mutation, daemonID: daemonID}, nil
}

// Build emits the daemon's bounded JSON stream but derives its successful image
// from independent post-build inventory and inspection. On transport loss or an
// ambiguous outcome the issued record remains unresolved; no resubmission occurs.
func (executor *BuildExecutor) Build(ctx context.Context, operationID, rawQuery string,
	prepared *BuildContext, output io.Writer,
) (BuildReceipt, error) {
	if ctx == nil || output == nil {
		return BuildReceipt{}, buildRequestDenied()
	}
	options, err := executor.policy.Prepare(operationID, rawQuery, prepared)
	if err != nil {
		return BuildReceipt{}, err
	}
	executor.mutation.Lock()
	defer executor.mutation.Unlock()
	if err := ctx.Err(); err != nil {
		return BuildReceipt{}, err
	}
	before, err := executor.inventory(ctx)
	if err != nil {
		return BuildReceipt{}, err
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		return BuildReceipt{}, errs.Wrap(errs.KindInternal, err)
	}
	optionsHash := sha256.Sum256(encoded)
	clear(encoded)
	intent := BuildIntent{
		OperationID: operationID, RunnerID: executor.policy.runnerID, RuntimeEpoch: executor.policy.epoch,
		DaemonID: executor.daemonID, ContextDigest: prepared.Digest(),
		OptionsDigest: "sha256:" + hex.EncodeToString(optionsHash[:]), BeforeImages: inventoryIDs(before),
	}
	if err := executor.ledger.Issue(ctx, intent); err != nil {
		return BuildReceipt{}, err
	}
	response, err := executor.engine.ImageBuild(ctx, prepared, options)
	if err != nil {
		return BuildReceipt{}, errs.Wrap(errs.KindInternal, err)
	}
	if response.Body == nil {
		return BuildReceipt{}, buildEvidenceConflict()
	}
	failed, streamErr := forwardBuildOutput(response.Body, output)
	closeErr := response.Body.Close()
	if streamErr != nil {
		return BuildReceipt{}, streamErr
	}
	if closeErr != nil {
		return BuildReceipt{}, errs.Wrap(errs.KindInternal, closeErr)
	}
	after, err := executor.inventory(ctx)
	if err != nil {
		return BuildReceipt{}, err
	}
	receipt := BuildReceipt{
		Intent:       intent,
		IntentDigest: buildRecordDigest(intent),
		NewImages:    []string{},
		Succeeded:    !failed,
	}
	for _, current := range after {
		if _, existing := slices.BinarySearch(intent.BeforeImages, current.ID); !existing {
			receipt.NewImages = append(receipt.NewImages, current.ID)
		}
	}
	slices.Sort(receipt.NewImages)
	if !failed {
		receipt.ImageID, err = executor.resolveBuiltImage(ctx, intent, after)
		if err != nil {
			return BuildReceipt{}, err
		}
	}
	if err := executor.ledger.Complete(ctx, receipt); err != nil {
		return BuildReceipt{}, err
	}
	if failed {
		return receipt, errs.New(errs.KindStateConflict, "Runner image build failed")
	}
	return receipt, nil
}

func (executor *BuildExecutor) inventory(ctx context.Context) ([]image.Summary, error) {
	info, err := executor.engine.Info(ctx, client.InfoOptions{})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if info.Info.ID != executor.daemonID {
		return nil, buildEvidenceConflict()
	}
	listed, err := executor.engine.ImageList(ctx, client.ImageListOptions{All: true})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(listed.Items) > maximumBuildInventory {
		return nil, buildEvidenceConflict()
	}
	if !validImageInventory(inventoryIDs(listed.Items)) {
		return nil, buildEvidenceConflict()
	}
	return listed.Items, nil
}

func inventoryIDs(images []image.Summary) []string {
	identities := make([]string, len(images))
	for index, current := range images {
		identities[index] = current.ID
	}
	slices.Sort(identities)
	return identities
}

func (executor *BuildExecutor) resolveBuiltImage(
	ctx context.Context,
	intent BuildIntent,
	inventory []image.Summary,
) (string, error) {
	imageID := ""
	for _, current := range inventory {
		if !buildLabelsMatch(current.Labels, intent) {
			continue
		}
		if imageID != "" {
			return "", buildEvidenceConflict()
		}
		imageID = current.ID
	}
	if imageID == "" {
		return "", buildEvidenceConflict()
	}
	if _, existed := slices.BinarySearch(intent.BeforeImages, imageID); existed {
		return "", buildEvidenceConflict()
	}
	inspected, err := executor.engine.ImageInspect(ctx, imageID)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	if inspected.ID != imageID || inspected.Config == nil || !buildLabelsMatch(inspected.Config.Labels, intent) {
		return "", buildEvidenceConflict()
	}
	return imageID, nil
}

func buildLabelsMatch(labels map[string]string, intent BuildIntent) bool {
	return labels[runnerOwnerLabel] == intent.RunnerID &&
		labels[runnerEpochLabel] == strconv.FormatUint(intent.RuntimeEpoch, 10) &&
		labels[buildOperationLabel] == intent.OperationID &&
		labels[buildInputLabel] == intent.ContextDigest
}

// Docker emits newline-delimited JSON. Validate each bounded frame before
// forwarding it, and require EOF so a success-looking prefix cannot seal proof.
func forwardBuildOutput(source io.Reader, output io.Writer) (bool, error) {
	reader := bufio.NewScanner(source)
	reader.Buffer(make([]byte, 32<<10), 1<<20)
	failed := false
	frames := 0
	for reader.Scan() {
		frame := reader.Bytes()
		if len(strings.TrimSpace(string(frame))) == 0 {
			continue
		}
		var message struct {
			Error       string `json:"error"`
			ErrorDetail *struct {
				Message string `json:"message"`
			} `json:"errorDetail"`
		}
		if err := json.Unmarshal(frame, &message); err != nil || len(frame) == 0 || frame[0] != '{' {
			return false, buildEvidenceConflict()
		}
		frames++
		failed = failed || message.Error != "" || message.ErrorDetail != nil
		if _, err := output.Write(append(frame, '\n')); err != nil {
			return false, errs.Wrap(errs.KindInternal, err)
		}
	}
	if err := reader.Err(); err != nil {
		return false, errs.Wrap(errs.KindInternal, err)
	}
	if frames == 0 {
		return false, buildEvidenceConflict()
	}
	return failed, nil
}
