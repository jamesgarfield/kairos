package kairos

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// defaultHit reports a successful (2xx, error-free) attempt processed at or
// after target on the server's clock.
func defaultHit(target time.Time) func(Attempt) bool {
	return func(a Attempt) bool {
		return a.Err == nil && a.StatusCode >= 200 && a.StatusCode < 300 && !a.Estimated.Before(target)
	}
}

// Fire schedules the workers from cal, rewarms each worker's connection, has a
// single releaser release each worker at its fire time, and retries each
// worker immediately until it gets a hit or the deadline passes. It returns
// every attempt and the best hit. It returns ErrTooLate, without sending
// anything, if the first worker's fire time has already passed.
func (s *Session) Fire(ctx context.Context, cal Calibration) (Result, error) {
	start := time.Now()
	cfg := s.cfg
	lead := cfg.Lead
	if lead == 0 {
		lead = 3 * cal.RTT
	}
	sched := plan(cfg.Target, cal.Final, cfg.Workers, cfg.Spacing, lead, cfg.GiveUp)
	res := Result{
		Target:         cfg.Target,
		Calibration:    cal,
		CalibrationAge: start.Sub(cal.TakenAt),
		Schedule:       sched,
	}
	if now := time.Now(); !sched.FireAt[0].After(now) {
		return res, fmt.Errorf("%w: first worker would fire %v ago", ErrTooLate, now.Sub(sched.FireAt[0]))
	}

	sendCtx, cancelSend := context.WithDeadline(ctx, sched.Deadline)
	defer cancelSend()
	reqCtx, cancelReq := context.WithDeadline(ctx, sched.Deadline.Add(cfg.InFlightGrace))
	defer cancelReq()

	isHit := cfg.IsHit
	if isHit == nil {
		isHit = defaultHit(cfg.Target)
	}

	// runReleases sorts rs in place, so workers take their channels from chans.
	rs := make([]release, cfg.Workers)
	chans := make([]chan struct{}, cfg.Workers)
	for i := range rs {
		chans[i] = make(chan struct{})
		rs[i] = release{at: sched.FireAt[i], ch: chans[i]}
	}

	res.Attempts = make([][]Attempt, cfg.Workers)
	var wg sync.WaitGroup
	wg.Go(func() { runReleases(sendCtx, rs, cfg.Spin, cal.WakeLatency) })
	for w := range cfg.Workers {
		wg.Go(func() {
			res.Attempts[w] = s.runWorker(ctx, sendCtx, reqCtx, cancelSend, w, sched.FireAt[w], sched.Deadline, chans[w], isHit, cal.Final.Point)
		})
	}
	wg.Wait()

	for w := range res.Attempts {
		for i := range res.Attempts[w] {
			a := &res.Attempts[w][i]
			if a.Hit && (res.Best == nil || a.Estimated.Before(res.Best.Estimated)) {
				res.Best = a
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return res, err
	}
	if res.Best == nil {
		return res, ErrNoHit
	}
	b := res.Best
	cfg.Logger.Logf("kairos: best hit worker %d attempt %d: status %d, estimated server time %v (%v after target)",
		b.Worker, b.N, b.StatusCode, b.Estimated, b.Estimated.Sub(cfg.Target))
	return res, nil
}

// runWorker rewarms worker w's connection, waits for its release, then sends
// and retries immediately until a hit, the deadline, or sendCtx cancellation.
// Nothing is logged between release and the end of the send loop.
func (s *Session) runWorker(ctx, sendCtx, reqCtx context.Context, cancelSend context.CancelFunc,
	w int, fireAt, deadline time.Time, release <-chan struct{}, isHit func(Attempt) bool, stampDelay time.Duration,
) []Attempt {
	cfg := s.cfg
	c := s.clients[w]
	attempts := make([]Attempt, 0, 64)

	if err := sleepUntil(sendCtx, fireAt.Add(-cfg.RewarmBefore), 0); err != nil {
		return attempts
	}
	if err := s.rewarm(ctx, c); err != nil {
		cfg.Logger.Logf("kairos: worker %d rewarm: %v", w, err)
	}
	req, buildErr := cfg.Request(reqCtx)

	select {
	case <-release:
	case <-sendCtx.Done():
		return attempts
	}

	for n := 1; ; n++ {
		if sendCtx.Err() != nil || !time.Now().Before(deadline) {
			break
		}
		if buildErr != nil {
			// Never sent; a build error is not retried, so the loop cannot spin on it.
			attempts = append(attempts, Attempt{Worker: w, N: n, Err: fmt.Errorf("build request: %w", buildErr)})
			break
		}
		a := s.send(c, req, cfg.ServerTime)
		a.N, a.Worker = n, w
		a.Estimated = estimateServerTime(a, stampDelay)
		a.Hit = isHit(a)
		attempts = append(attempts, a)
		if a.Hit {
			if cfg.StopOnFirstHit {
				cancelSend()
			}
			break
		}
		req, buildErr = cfg.Request(reqCtx)
	}
	return attempts
}

// rewarm sends one real request on c so its connection is warm at release.
func (s *Session) rewarm(ctx context.Context, c *http.Client) error {
	rctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()
	req, err := s.cfg.Request(rctx)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	return s.send(c, req, s.cfg.ServerTime).Err
}
