// Package publicingress applies captured ingress configuration to an owned edge
// runtime and retains the last verified startup configuration across restarts.
package publicingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumConfigBytes = 256 << 10

// Configuration is the immutable, renderer-produced data authorized by the
// current Agent attempt. RuntimeID is the exact immutable container ID.
type Configuration struct {
	RuntimeID string
	Revision  int64
	JSON      []byte
}

type Receipt struct {
	RuntimeID string `json:"runtime_id"`
	Revision  int64  `json:"revision"`
	Digest    string `json:"digest"`
}

// Runtime must operate on the sealed runtime identity, never an arbitrary
// Docker name or destination. Quiescent must independently prove that the
// exact interrupted mutation can no longer take effect.
type Runtime interface {
	Validate(context.Context, string, []byte) error
	Reload(context.Context, string, []byte) error
	Active(context.Context, string) ([]byte, error)
	Ready(context.Context, Receipt) error
	Quiescent(context.Context, string) error
}

type Store struct {
	stateRoot string
	runtime   Runtime
}

func New(stateRoot string, runtime Runtime) (*Store, error) {
	if !filepath.IsAbs(stateRoot) || filepath.Clean(stateRoot) != stateRoot ||
		stateRoot == string(filepath.Separator) || runtime == nil {
		return nil, errs.New(errs.KindValidationFailed, "public ingress state root or runtime is invalid")
	}
	return &Store{stateRoot: stateRoot, runtime: runtime}, nil
}

// Bootstrap records the startup configuration of a newly provisioned runtime.
// The provisioner supplies and starts the trusted initial configuration, and
// Bootstrap refuses to adopt any other configuration observed at runtime.
func (store *Store) Bootstrap(ctx context.Context, initial Configuration) (Receipt, error) {
	wanted, err := capture(initial)
	if err != nil {
		return Receipt{}, err
	}
	dir, err := store.open(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer dir.close()
	previous, found, err := dir.load()
	if err != nil {
		return Receipt{}, err
	}
	if found {
		if previous.Candidate != nil || previous.Current.Receipt != wanted.Receipt {
			return Receipt{}, conflict("public ingress startup ownership already exists")
		}
		return store.verify(ctx, previous.Current)
	}
	if _, err := store.verify(ctx, wanted); err != nil {
		return Receipt{}, err
	}
	if err := dir.commit(wanted); err != nil {
		return Receipt{}, err
	}
	return wanted.Receipt, nil
}

func (store *Store) Apply(ctx context.Context, expected Receipt, candidate Configuration) (Receipt, error) {
	next, err := capture(candidate)
	if err != nil {
		return Receipt{}, err
	}
	dir, err := store.open(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer dir.close()
	state, err := dir.requireState()
	if err != nil {
		return Receipt{}, err
	}
	if state.Current.Receipt != expected || next.RuntimeID != expected.RuntimeID || state.Candidate != nil {
		return Receipt{}, conflict("public ingress activation ownership changed or recovery is required")
	}
	if next.Revision == expected.Revision && next.Digest != expected.Digest {
		return Receipt{}, conflict("public ingress revision differs from its captured configuration")
	}
	if next.Revision < expected.Revision {
		return Receipt{}, conflict("public ingress candidate revision is stale")
	}
	// The captured predecessor can be active while one of its routes is
	// unhealthy. Keep an authorized repair eligible if ownership still matches.
	if err := store.requireActive(ctx, state.Current); err != nil {
		return Receipt{}, err
	}
	if next.Digest == expected.Digest {
		if err := store.runtime.Ready(ctx, next.Receipt); err != nil {
			return Receipt{}, err
		}
		if next.Receipt == expected {
			return expected, nil
		}
		if err := dir.commit(next); err != nil {
			return Receipt{}, err
		}
		return next.Receipt, nil
	}
	if err := store.runtime.Validate(ctx, next.RuntimeID, next.JSON); err != nil {
		return Receipt{}, err
	}
	state.Candidate = &next
	if err := dir.save(state); err != nil {
		return Receipt{}, err
	}
	if err := store.runtime.Reload(ctx, next.RuntimeID, next.JSON); err != nil {
		// The exact candidate may have reached the edge. Leave the intention
		// in place so recovery can observe the actual configuration.
		return Receipt{}, err
	}
	if err := store.requireActive(ctx, next); err != nil {
		return Receipt{}, err
	}
	return store.qualify(ctx, dir, state)
}

// Recover fences stale recovery attempts with both captured receipts. It can
// settle a witnessed activation or restore the predecessor and never replays
// candidate application.
func (store *Store) Recover(ctx context.Context, predecessor, candidate Receipt) (Receipt, error) {
	dir, err := store.open(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer dir.close()
	state, err := dir.requireState()
	if err != nil {
		return Receipt{}, err
	}
	if state.Candidate == nil {
		if state.Current.Receipt != predecessor && state.Current.Receipt != candidate {
			return Receipt{}, conflict("public ingress recovery belongs to an older activation")
		}
		return store.verify(ctx, state.Current)
	}
	if state.Current.Receipt != predecessor || state.Candidate.Receipt != candidate {
		return Receipt{}, conflict("public ingress recovery does not match the pending activation")
	}
	if err := store.runtime.Quiescent(ctx, predecessor.RuntimeID); err != nil {
		return Receipt{}, err
	}
	active, err := store.active(ctx, predecessor.RuntimeID)
	if err != nil {
		return Receipt{}, err
	}
	switch {
	case bytes.Equal(active, state.Current.JSON):
		if err := store.runtime.Ready(ctx, predecessor); err != nil {
			return Receipt{}, err
		}
		if err := dir.commit(state.Current); err != nil {
			return Receipt{}, err
		}
		return predecessor, nil
	case !bytes.Equal(active, state.Candidate.JSON):
		return Receipt{}, conflict("public ingress active configuration has unknown ownership")
	case state.Restoring:
		return store.restore(ctx, dir, state)
	default:
		return store.qualify(ctx, dir, state)
	}
}

// Observe verifies that the live runtime serves one of the exact expected publications.
func (store *Store) Observe(ctx context.Context, expected Receipt, alternatives ...Receipt) (Receipt, error) {
	dir, err := store.open(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer dir.close()
	state, err := dir.requireState()
	if err != nil {
		return Receipt{}, err
	}
	if state.Candidate != nil {
		return Receipt{}, conflict("public ingress activation is awaiting recovery")
	}
	if state.Current.Receipt != expected && !slices.Contains(alternatives, state.Current.Receipt) {
		return Receipt{}, conflict("public ingress settled receipt differs from the expected publication")
	}
	_, pending, err := dir.read(reloadMarker, 512)
	if err != nil {
		return Receipt{}, err
	}
	if pending {
		return Receipt{}, conflict("public ingress has an unresolved reload")
	}
	return store.verify(ctx, state.Current)
}

func (store *Store) qualify(ctx context.Context, dir *stateDirectory, state activation) (Receipt, error) {
	candidate := *state.Candidate
	if err := store.runtime.Ready(ctx, candidate.Receipt); err != nil {
		if ctx.Err() != nil {
			return Receipt{}, ctx.Err()
		}
		if _, restoreErr := store.restore(ctx, dir, state); restoreErr != nil {
			return Receipt{}, restoreErr
		}
		return Receipt{}, errs.New(
			errs.KindRequestFailed,
			"public ingress candidate was unhealthy and predecessor was restored",
		)
	}
	if err := dir.commit(candidate); err != nil {
		return Receipt{}, err
	}
	return candidate.Receipt, nil
}

func (store *Store) restore(ctx context.Context, dir *stateDirectory, state activation) (Receipt, error) {
	if !state.Restoring {
		state.Restoring = true
		if err := dir.save(state); err != nil {
			return Receipt{}, err
		}
	}
	previous := state.Current
	// A crash after candidate startup was written can leave both receipts
	// pending. Once restoration is chosen, make restarts use the predecessor.
	if err := dir.replace(StartupFilename, previous.JSON); err != nil {
		return Receipt{}, err
	}
	if err := store.runtime.Reload(ctx, previous.RuntimeID, previous.JSON); err != nil {
		return Receipt{}, err
	}
	if _, err := store.verify(ctx, previous); err != nil {
		return Receipt{}, err
	}
	if err := dir.commit(previous); err != nil {
		return Receipt{}, err
	}
	return previous.Receipt, nil
}

// Describe derives the receipt for an immutable captured configuration.
func Describe(cfg Configuration) (Receipt, error) {
	record, err := capture(cfg)
	return record.Receipt, err
}

func (store *Store) verify(ctx context.Context, cfg recordedConfig) (Receipt, error) {
	if err := store.requireActive(ctx, cfg); err != nil {
		return Receipt{}, err
	}
	if err := store.runtime.Ready(ctx, cfg.Receipt); err != nil {
		return Receipt{}, err
	}
	return cfg.Receipt, nil
}

func (store *Store) requireActive(ctx context.Context, cfg recordedConfig) error {
	active, err := store.active(ctx, cfg.RuntimeID)
	if err != nil {
		return err
	}
	if !bytes.Equal(active, cfg.JSON) {
		return conflict("public ingress active configuration differs from the captured configuration")
	}
	return nil
}

func (store *Store) active(ctx context.Context, runtimeID string) ([]byte, error) {
	raw, err := store.runtime.Active(ctx, runtimeID)
	if err != nil {
		return nil, err
	}
	return canonicalObject(raw)
}

func capture(cfg Configuration) (recordedConfig, error) {
	if !validRuntimeID(cfg.RuntimeID) || cfg.Revision <= 0 {
		return recordedConfig{}, errs.New(errs.KindValidationFailed, "public ingress captured identity is invalid")
	}
	raw, err := canonicalObject(cfg.JSON)
	if err != nil {
		return recordedConfig{}, err
	}
	sum := sha256.Sum256(raw)
	return recordedConfig{
		Receipt: Receipt{RuntimeID: cfg.RuntimeID, Revision: cfg.Revision, Digest: fmt.Sprintf("sha256:%x", sum)},
		JSON:    raw,
	}, nil
}

func canonicalObject(raw []byte) ([]byte, error) {
	if len(raw) == 0 || len(raw) > maximumConfigBytes {
		return nil, errs.New(errs.KindValidationFailed, "public ingress configuration size is invalid")
	}
	data, err := jcs.Canonicalize(raw)
	if err != nil || len(data) > maximumConfigBytes || data[0] != '{' {
		return nil, errs.New(errs.KindValidationFailed, "public ingress configuration is not a valid JSON object")
	}
	return data, nil
}

func validRuntimeID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !strings.ContainsRune("0123456789abcdef", char) {
			return false
		}
	}
	return true
}

func conflict(detail string) error {
	return errs.New(errs.KindStateConflict, detail)
}
