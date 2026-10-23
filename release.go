package kairos

import (
	"context"
	"runtime"
	"sort"
	"time"
)

// sleepUntil waits until t. It sleeps on a timer until t-spin, then spins
// (yielding with runtime.Gosched) for the remainder. spin <= 0 means
// timer-only. It returns ctx.Err() if ctx is cancelled first.
func sleepUntil(ctx context.Context, t time.Time, spin time.Duration) error {
	if spin < 0 {
		spin = 0
	}
	if d := time.Until(t.Add(-spin)); d > 0 {
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if spin == 0 {
		return ctx.Err()
	}
	for time.Now().Before(t) {
		if err := ctx.Err(); err != nil {
			return err
		}
		runtime.Gosched()
	}
	return ctx.Err()
}

// release closes ch at (or just before, by the wake latency) time at.
type release struct {
	at time.Time
	ch chan struct{}
}

// runReleases sorts rs by at, then for each waits until at-wake and closes
// its channel. All sleeping and spinning happens on the caller's goroutine, so
// at most one goroutine spins. It returns on ctx cancellation without closing
// the remaining channels.
func runReleases(ctx context.Context, rs []release, spin, wake time.Duration) {
	sort.SliceStable(rs, func(i, j int) bool { return rs[i].at.Before(rs[j].at) })
	for _, r := range rs {
		if err := sleepUntil(ctx, r.at.Add(-wake), spin); err != nil {
			return
		}
		close(r.ch)
	}
}

// measureWakeLatency returns the median delay between closing a channel and a
// waiting goroutine observing the close, over n trials.
func measureWakeLatency(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	lat := make([]time.Duration, 0, n)
	for range n {
		ch := make(chan struct{})
		got := make(chan time.Time, 1)
		ready := make(chan struct{})
		go func() {
			close(ready)
			<-ch
			got <- time.Now()
		}()
		<-ready
		time.Sleep(200 * time.Microsecond) // let the goroutine park
		closedAt := time.Now()
		close(ch)
		lat = append(lat, (<-got).Sub(closedAt))
	}
	return quantile(lat, 0.5)
}
