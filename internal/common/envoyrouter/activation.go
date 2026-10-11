package envoyrouter

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/envoyadmin"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Activation is a bounded, resumable publication sequence built exclusively from
// the Task's captured before/after files. An interrupted invocation can recognize
// its exact intermediate restart files; it cannot adopt an unrelated live state
// as its predecessor. The infrastructure caller retains the original deadline.
type Activation struct {
	transition Transition
	desired    Snapshot
	states     []activationState
	identity   [sha256.Size]byte
}

type activationState struct {
	files   [4][]byte
	source  []byte
	phase   TransitionPhase
	target  string
	content []byte
}

type ActivationProgress struct {
	identity [sha256.Size]byte
	index    int
	active   bool
	complete bool
}

func (progress ActivationProgress) Complete() bool { return progress.complete }
func (progress ActivationProgress) ReadyToPublish() bool {
	return progress.active && !progress.complete
}

type Publication struct {
	command []string
	input   []byte
}

func (publication Publication) Command() []string {
	return append([]string(nil), publication.command...)
}
func (publication Publication) Input() []byte { return bytes.Clone(publication.input) }

// A deliberately absent predecessor means a new router: its startup must already
// expose the captured desired files and active resources. This function never
// invents a predecessor from whichever files happen to exist in the container.
func PrepareActivation(previousSource, desiredSource []byte) (Activation, error) {
	desired, err := Read(desiredSource)
	if err != nil {
		return Activation{}, err
	}
	identityInput := append(bytes.Clone(previousSource), 0)
	identityInput = append(identityInput, desiredSource...)
	activation := Activation{desired: desired, identity: sha256.Sum256(identityInput)}
	if len(previousSource) == 0 {
		activation.states = []activationState{snapshotState(desired, DesiredSnapshotActive)}
		return activation, nil
	}
	previous, err := Read(previousSource)
	if err != nil {
		return Activation{}, err
	}
	transition, err := PrepareTransition(previous, desired)
	if err != nil {
		return Activation{}, err
	}
	activation.transition = transition
	activation.states = []activationState{snapshotState(previous, "")}
	activation.appendPublication(1, ClustersTarget, transition.BridgeClusters(), BridgeClustersActive)
	activation.appendPublication(2, RoutesTarget, desired.Routes(), DesiredRoutesActive)
	activation.appendPublication(1, ClustersTarget, desired.Clusters(), DesiredSnapshotActive)
	// The listener is unchanged semantically. Publish its captured representation
	// and the validation manifest only after desired traffic resources are proven.
	activation.appendPublication(0, BootstrapTarget, desired.Bootstrap(), DesiredSnapshotActive)
	activation.appendPublication(3, ManifestTarget, desired.Manifest(), DesiredSnapshotActive)
	activation.states[len(activation.states)-1].phase = DesiredSnapshotActive
	return activation, nil
}

func snapshotState(snapshot Snapshot, phase TransitionPhase) activationState {
	return activationState{
		files:  [4][]byte{snapshot.Bootstrap(), snapshot.Clusters(), snapshot.Routes(), snapshot.Manifest()},
		source: snapshot.Source(), phase: phase,
	}
}

func (activation *Activation) appendPublication(index int, target string, content []byte, phase TransitionPhase) {
	previous := activation.states[len(activation.states)-1]
	if bytes.Equal(previous.files[index], content) {
		return
	}
	next := activationState{files: previous.files, phase: phase, target: target, content: bytes.Clone(content)}
	next.files[index] = bytes.Clone(content)
	next.source = bytes.Join(next.files[:], []byte{'\n'})
	next.source = append(next.source, '\n')
	activation.states = append(activation.states, next)
}

// Observe accepts only exact reachable restart-file states, then proves the full
// active listener/CDS/RDS configuration for that state. File publication and
// discovery acknowledgement are distinct; a successful rename is not completion.
func (activation Activation) Observe(value []byte) (ActivationProgress, error) {
	if len(activation.states) == 0 || len(value) == 0 || len(value) > MaximumObservationBytes {
		return ActivationProgress{}, invalidObservation()
	}
	parts := bytes.SplitN(value, []byte{'\n'}, 5)
	if len(parts) != 5 {
		return ActivationProgress{}, invalidObservation()
	}
	source := bytes.Join(parts[:4], []byte{'\n'})
	source = append(source, '\n')
	index := -1
	for candidate := len(activation.states) - 1; candidate >= 0; candidate-- {
		if bytes.Equal(source, activation.states[candidate].source) {
			index = candidate
			break
		}
	}
	if index < 0 {
		return ActivationProgress{}, errs.New(errs.KindStateConflict,
			"HTTP router files do not match this Task's previous, intermediate or desired configuration")
	}
	body, err := envoyadmin.ReadResponse(parts[4], MaximumObservationBytes)
	if err != nil {
		return ActivationProgress{}, err
	}
	state := activation.states[index]
	var active bool
	if state.phase == "" {
		active, err = activation.transition.previous.VerifyActiveConfiguration(body)
	} else if state.phase == DesiredSnapshotActive {
		active, err = activation.desired.VerifyActiveConfiguration(body)
	} else {
		active, err = activation.transition.VerifyActiveConfiguration(body, state.phase)
	}
	if err != nil {
		return ActivationProgress{}, err
	}
	return ActivationProgress{identity: activation.identity, index: index, active: active,
		complete: active && index == len(activation.states)-1}, nil
}

// Publication uses fixed paths and transfers configuration only through stdin.
// Hash comparisons cover every restart file both before staging and immediately
// before rename. Unexpected state or an interrupted staging file is not adopted.
func (activation Activation) Publication(progress ActivationProgress) (Publication, error) {
	if progress.identity != activation.identity || !progress.ReadyToPublish() || progress.index < 0 ||
		progress.index+1 >= len(activation.states) {
		return Publication{}, errs.New(errs.KindInternal, "HTTP router publication has no acknowledged predecessor")
	}
	current, next := activation.states[progress.index], activation.states[progress.index+1]
	return preparePublication(current, next), nil
}

func preparePublication(current, next activationState) Publication {
	checks := "test ! -L " + RuntimeDirectory + "; test -d " + RuntimeDirectory + "; "
	for index, target := range []string{BootstrapTarget, ClustersTarget, RoutesTarget, ManifestTarget} {
		digest := sha256.Sum256(append(bytes.Clone(current.files[index]), '\n'))
		checks += "test ! -L " + target + "; test -f " + target + "; " +
			"test \"$(sha256sum " + target + ")\" = '" + hex.EncodeToString(digest[:]) + "  " + target + "'; "
	}
	input := append(bytes.Clone(next.content), '\n')
	digest := sha256.Sum256(input)
	command := "umask 077; " + checks +
		"gp_publication=$(mktemp " + RuntimeDirectory + "/.publish.XXXXXX); " +
		"trap 'rm -f -- \"$gp_publication\"' EXIT; cat > \"$gp_publication\"; " +
		"test \"$(sha256sum \"$gp_publication\" | cut -d ' ' -f 1)\" = '" + hex.EncodeToString(digest[:]) + "'; " +
		"sync; " + checks + "mv -f -- \"$gp_publication\" " + next.target + "; sync"
	return Publication{command: []string{"sh", "-ec", strings.TrimSpace(command)}, input: input}
}
