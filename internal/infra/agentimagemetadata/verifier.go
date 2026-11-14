package agentimagemetadata

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Verifier serializes native-update preflight reads. An image-derived owner
// makes a copy helper left by an interrupted preflight reclaimable next time.
type Verifier struct{ mu sync.Mutex }

func (verifier *Verifier) Verify(ctx context.Context, image string, channel int) error {
	verifier.mu.Lock()
	defer verifier.mu.Unlock()
	owner := ids.DeriveAt(
		ids.KindOperation,
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		"agent-build-inspection",
		image,
	)
	metadata, err := Read(ctx, image, owner)
	if err != nil {
		return err
	}
	if metadata.ChannelSchema != channel {
		return errs.New(
			errs.KindSoftwareProtocolIncompatible,
			fmt.Sprintf(
				"Selected Agent channel format %d does not match Controller format %d; prepare Both with matching builds",
				metadata.ChannelSchema,
				channel,
			),
		)
	}
	return nil
}
