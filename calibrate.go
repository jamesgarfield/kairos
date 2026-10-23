package kairos

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"
)

// sweepLeadIn is how far ahead of now the first sweep sample is scheduled,
// leaving time to build requests and start the sample goroutines.
const sweepLeadIn = 50 * time.Millisecond

// sweepCoarseSamples is the number of samples in the coarse sweep pass.
const sweepCoarseSamples = 10

// sweepMaxHalf caps the fine pass at 2*sweepMaxHalf+1 samples.
const sweepMaxHalf = 25

// Calibrate measures the stamp delay (server clock minus local send time,
// including one-way latency) against the calibration endpoint, optionally
// combines it with the reference clock, and warms every worker connection.
func (s *Session) Calibrate(ctx context.Context) (Calibration, error) {
	req, cancel, err := s.calibrationRequest(ctx)
	if err != nil {
		return Calibration{}, fmt.Errorf("calibration probe: %w", err)
	}
	probe := s.send(s.clients[0], req, s.cfg.CalibrationServerTime)
	cancel()
	if probe.Err != nil {
		return Calibration{}, fmt.Errorf("calibration probe: %w", probe.Err)
	}

	var target Estimate
	var rtts []time.Duration
	if probe.Server.Precision <= preciseThreshold {
		target, rtts, err = s.calibratePrecise(ctx)
	} else {
		target, rtts, err = s.calibrateSweep(ctx, probe.Server.Precision)
	}
	if err != nil {
		return Calibration{}, err
	}

	cal := Calibration{Target: target, RTT: quantile(rtts, 0.5)}
	if s.cfg.Reference != nil {
		off, err := s.cfg.Reference.Offset(ctx)
		if err != nil {
			return Calibration{}, fmt.Errorf("calibration reference: %w", err)
		}
		ref := referenceEstimate(off, rtts)
		cal.Reference = &ref
	}
	cal.Final, cal.Disagreement = combine(cal.Target, cal.Reference, s.cfg.Policy)
	if cal.Disagreement != 0 {
		s.cfg.Logger.Logf("kairos: calibration disagreement %v: target %+v, reference %+v, final %+v",
			cal.Disagreement, cal.Target, *cal.Reference, cal.Final)
	}
	cal.WakeLatency = measureWakeLatency(20)
	cal.TakenAt = time.Now()
	return cal, nil
}

// calibrationRequest builds a calibration request bounded by RequestTimeout.
// The caller must call the returned cancel func once the request is done.
func (s *Session) calibrationRequest(ctx context.Context) (*http.Request, context.CancelFunc, error) {
	rctx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	req, err := s.cfg.Calibration(rctx)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("build request: %w", err)
	}
	if req == nil {
		cancel()
		return nil, nil, errors.New("build request: Calibration returned nil request")
	}
	return req, cancel, nil
}

// calibratePrecise runs CalibrationRounds rounds in which every client sends
// at once, and returns the precise estimate and all RTTs.
func (s *Session) calibratePrecise(ctx context.Context) (Estimate, []time.Duration, error) {
	var delays, rtts []time.Duration
	for r := range s.cfg.CalibrationRounds {
		atts := make([]Attempt, len(s.clients))
		var wg sync.WaitGroup
		for i, c := range s.clients {
			wg.Go(func() {
				req, cancel, err := s.calibrationRequest(ctx)
				if err != nil {
					atts[i].Err = err
					return
				}
				defer cancel()
				atts[i] = s.send(c, req, s.cfg.CalibrationServerTime)
			})
		}
		wg.Wait()
		for _, a := range atts {
			if a.Err != nil {
				return Estimate{}, nil, fmt.Errorf("calibration round %d: %w", r, a.Err)
			}
		}
		for _, a := range atts {
			delays = append(delays, a.Server.Time.Sub(a.Sent))
			rtts = append(rtts, a.RTT())
		}
	}
	if len(delays) == 0 {
		return Estimate{}, nil, errors.New("calibration: no precise samples")
	}
	return preciseEstimate(delays), rtts, nil
}

// calibrateSweep finds the stamp delay from a server clock of the given
// precision by a coarse pass across one period, then a fine pass straddling
// the predicted tick. It returns the estimate and the filtered RTTs.
func (s *Session) calibrateSweep(ctx context.Context, precision time.Duration) (Estimate, []time.Duration, error) {
	start := time.Now().Add(sweepLeadIn)
	coarseAt := make([]time.Time, sweepCoarseSamples)
	for i := range coarseAt {
		coarseAt[i] = start.Add(time.Duration(i) * precision / sweepCoarseSamples)
	}
	coarse, err := s.sweepPass(ctx, coarseAt, 0)
	if err != nil {
		return Estimate{}, nil, err
	}
	e1, _, err := sweepResult(coarse, precision)
	if err != nil {
		return Estimate{}, nil, err
	}

	width := e1.Hi - e1.Lo
	step := max(s.cfg.SweepStep, width/50)
	m := min(sweepMaxHalf, int(math.Ceil(float64(width/2)/float64(step)))+1)
	d := e1.Point
	earliest := time.Now().Add(sweepLeadIn)
	serverEarliest := earliest.Add(d)
	boundary := serverEarliest.Truncate(precision).Add(precision)
	// Derive the local instant from earliest so it keeps its monotonic reading.
	localCenter := earliest.Add(boundary.Sub(serverEarliest))
	if localCenter.Add(-time.Duration(m) * step).Before(earliest) {
		localCenter = localCenter.Add(precision)
	}
	fineAt := make([]time.Time, 0, 2*m+1)
	for j := -m; j <= m; j++ {
		fineAt = append(fineAt, localCenter.Add(time.Duration(j)*step))
	}
	fine, err := s.sweepPass(ctx, fineAt, len(coarse))
	if err != nil {
		return Estimate{}, nil, err
	}
	return sweepResult(append(coarse, fine...), precision)
}

// sweepResult returns sweepEstimate(samples) and the RTTs that survive the
// same 2x-median filter sweepEstimate applies. It errors if none survive.
func sweepResult(samples []sweepSample, precision time.Duration) (Estimate, []time.Duration, error) {
	all := make([]time.Duration, len(samples))
	for i, sm := range samples {
		all[i] = sm.RTT
	}
	cutoff := 2 * quantile(all, 0.5)
	var kept []time.Duration
	for _, r := range all {
		if r <= cutoff {
			kept = append(kept, r)
		}
	}
	if len(kept) == 0 {
		return Estimate{}, nil, errors.New("calibration: no usable sweep samples")
	}
	return sweepEstimate(samples, precision), kept, nil
}

// sweepPass sends one sample at each local instant in at. Requests are built
// up front; each sample waits on its own goroutine for a release from a single
// releaser, then sends on client i%Workers. base offsets sample indices in
// error messages.
func (s *Session) sweepPass(ctx context.Context, at []time.Time, base int) ([]sweepSample, error) {
	n := len(at)
	reqs := make([]*http.Request, n)
	for i := range n {
		req, cancel, err := s.calibrationRequest(ctx)
		if err != nil {
			return nil, fmt.Errorf("calibration sweep sample %d: %w", base+i, err)
		}
		defer cancel()
		reqs[i] = req
	}

	rs := make([]release, n)
	atts := make([]Attempt, n)
	var wg sync.WaitGroup
	for i := range n {
		ch := make(chan struct{})
		rs[i] = release{at: at[i], ch: ch}
		c := s.clients[i%len(s.clients)]
		wg.Go(func() {
			select {
			case <-ch:
			case <-ctx.Done():
				atts[i].Err = ctx.Err()
				return
			}
			atts[i] = s.send(c, reqs[i], s.cfg.CalibrationServerTime)
		})
	}
	runReleases(ctx, rs, s.cfg.Spin, 0)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("calibration sweep: %w", err)
	}

	samples := make([]sweepSample, n)
	for i, a := range atts {
		if a.Err != nil {
			return nil, fmt.Errorf("calibration sweep sample %d: %w", base+i, a.Err)
		}
		samples[i] = sweepSample{Sent: a.Sent, Stamp: a.Server.Time, RTT: a.RTT()}
	}
	return samples, nil
}
