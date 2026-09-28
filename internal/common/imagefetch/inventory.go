package imagefetch

// LocalImage describes Docker observation, not a successful GP deployment.
type LocalImage struct {
	ID             string
	Tags           []string
	Digests        []string
	SizeBytes      int64
	Created        int64
	Containers     int
	ContentIDs     []string
	RemovalBlocked string
}
