package kairos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ReferenceClock reports an independent estimate of the server's clock offset.
type ReferenceClock interface {
	Offset(ctx context.Context) (time.Duration, error) // true time minus local time
}

// SystemClock trusts the local clock: its offset is always zero.
type SystemClock struct{}

// Offset returns 0, nil.
func (SystemClock) Offset(context.Context) (time.Duration, error) { return 0, nil }

// HTTPReference derives the offset from the time reported by an HTTP endpoint.
type HTTPReference struct {
	URL     string
	Extract TimeExtractor
	Samples int          // 0 = 5
	Client  *http.Client // nil = &http.Client{}
}

// ErrCoarseReference is returned when the reference's time precision is too coarse.
var ErrCoarseReference = errors.New("kairos: reference clock precision too coarse")

// referenceTimeout is the per-request timeout applied when ctx has no deadline.
var referenceTimeout = 5 * time.Second

// Offset returns median(ref.Time - (sent + rtt/2)) over the samples.
func (h HTTPReference) Offset(ctx context.Context) (time.Duration, error) {
	n := h.Samples
	if n <= 0 {
		n = 5
	}
	client := h.Client
	if client == nil {
		client = &http.Client{}
	}
	offs := make([]time.Duration, 0, n)
	for i := range n {
		off, err := h.sample(ctx, client)
		if err != nil {
			return 0, fmt.Errorf("kairos: reference sample %d: %w", i, err)
		}
		offs = append(offs, off)
	}
	return quantile(offs, 0.5), nil
}

func (h HTTPReference) sample(ctx context.Context, client *http.Client) (time.Duration, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, referenceTimeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, nil)
	if err != nil {
		return 0, err
	}
	sent := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	received := time.Now()
	cerr := resp.Body.Close()
	if err != nil {
		return 0, err
	}
	if cerr != nil {
		return 0, fmt.Errorf("close body: %w", cerr)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	st, err := h.Extract(resp, body)
	if err != nil {
		return 0, err
	}
	if st.Precision > preciseThreshold {
		return 0, ErrCoarseReference
	}
	rtt := received.Sub(sent)
	return st.Time.Sub(sent.Add(rtt / 2)), nil
}
