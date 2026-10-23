package kairos

import (
	"context"
	"time"
)

// ReferenceClock reports an independent estimate of the server's clock offset.
type ReferenceClock interface {
	Offset(ctx context.Context) (time.Duration, error)
}
