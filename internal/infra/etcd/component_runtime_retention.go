package etcd

// AppliedComponentRuntime returns the exact captured applied artifact whose
// key revision the Component publication already compares. The caller owns
// the returned bytes; mutation cannot alter the publication's private evidence.
func (preparation ComponentTaskPreparation) AppliedComponentRuntime() []byte {
	return append([]byte(nil), preparation.appliedComponentRuntime...)
}
