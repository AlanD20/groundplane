package envoyrouter

// Decision separates a completed proof, an asynchronous acknowledgement wait,
// and one authorized publication. Executors do not choose recovery targets or
// derive commands from the observed runtime themselves.
type Decision struct {
	complete    bool
	publication *Publication
}

func (decision Decision) Complete() bool { return decision.complete }
func (decision Decision) Publication() (Publication, bool) {
	if decision.publication == nil {
		return Publication{}, false
	}
	return Publication{command: decision.publication.Command(), input: decision.publication.Input()}, true
}

func (activation Activation) Advance(value []byte) (Decision, error) {
	progress, err := activation.Observe(value)
	if err != nil {
		return Decision{}, err
	}
	if progress.Complete() {
		return Decision{complete: true}, nil
	}
	if !progress.ReadyToPublish() {
		return Decision{}, nil
	}
	publication, err := activation.Publication(progress)
	if err != nil {
		return Decision{}, err
	}
	return Decision{publication: &publication}, nil
}

func (restoration Restoration) Advance(value []byte) (Decision, error) {
	progress, err := restoration.Observe(value)
	if err != nil {
		return Decision{}, err
	}
	if progress.Restored() {
		return Decision{complete: true}, nil
	}
	if !progress.ReadyToPublish() {
		return Decision{}, nil
	}
	publication, err := restoration.Publication(progress)
	if err != nil {
		return Decision{}, err
	}
	return Decision{publication: &publication}, nil
}
