package kairos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// calibrated starts a fake server from opts, builds a session for it from cfg
// (cfg.Target must be set), and calibrates it.
func calibrated(t *testing.T, opts fakeOpts, cfg Config) (*Session, Calibration) {
	t.Helper()
	srv := newFakeServer(t, opts)
	cfg.Request = reqFor(srv.URL)
	if cfg.CalibrationRounds == 0 {
		cfg.CalibrationRounds = 3
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(s.Close)
	cal, err := s.Calibrate(context.Background())
	if err != nil {
		t.Fatalf("Calibrate: %v", err)
	}
	return s, cal
}

func checkNoSendAtOrAfterDeadline(t *testing.T, res Result) {
	t.Helper()
	for _, as := range res.Attempts {
		for _, a := range as {
			if !a.Sent.Before(res.Schedule.Deadline) {
				t.Errorf("worker %d attempt %d Sent %v, at or after Deadline %v", a.Worker, a.N, a.Sent, res.Schedule.Deadline)
			}
		}
	}
}

func TestFirePrecise(t *testing.T) {
	target := time.Now().Add(200*ms + 1500*ms)
	s, cal := calibrated(t, fakeOpts{Skew: 200 * ms, Precise: true},
		Config{Target: target, ServerTime: preciseExtract, CalibrationServerTime: preciseExtract})
	res, err := s.Fire(context.Background(), cal)
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if d := res.Best.Estimated.Sub(target); d < 0 || d > 20*ms {
		t.Errorf("Best.Estimated - Target = %v, want [0, 20ms]", d)
	}
	for _, as := range res.Attempts {
		for _, a := range as {
			if a.Hit && a.Estimated.Before(target) {
				t.Errorf("worker %d attempt %d is a hit with Estimated %v before Target %v", a.Worker, a.N, a.Estimated, target)
			}
		}
	}
	checkNoSendAtOrAfterDeadline(t, res)
}

func TestFireDateOnly(t *testing.T) {
	target := time.Now().Add(300*ms + 5*time.Second).Truncate(time.Second)
	s, cal := calibrated(t, fakeOpts{Skew: 300 * ms}, Config{Target: target})
	res, err := s.Fire(context.Background(), cal)
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if !res.Best.Server.Time.Equal(target) {
		t.Errorf("Best.Server.Time = %v, want %v", res.Best.Server.Time, target)
	}
	if d := res.Best.Estimated.Sub(target); d >= 30*ms {
		t.Errorf("Best.Estimated - Target = %v, want < 30ms", d)
	}
}

func TestFireGate(t *testing.T) {
	target := time.Now().Add(200*ms + 1500*ms)
	gate := func(serverNow time.Time) int {
		if serverNow.Before(target) {
			return 403
		}
		return 200
	}
	s, cal := calibrated(t, fakeOpts{Skew: 200 * ms, Precise: true, Status: gate},
		Config{Target: target, ServerTime: preciseExtract, CalibrationServerTime: preciseExtract})
	res, err := s.Fire(context.Background(), cal)
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if res.Best.StatusCode != 200 {
		t.Errorf("Best.StatusCode = %d, want 200", res.Best.StatusCode)
	}
	forbidden := 0
	for _, as := range res.Attempts {
		for _, a := range as {
			if a.StatusCode == 403 {
				forbidden++
				if a.Hit {
					t.Errorf("worker %d attempt %d: 403 recorded as hit", a.Worker, a.N)
				}
			}
		}
	}
	if forbidden == 0 {
		t.Error("no 403 attempts recorded")
	}
}

func TestFireStopOnFirstHit(t *testing.T) {
	target := time.Now().Add(200*ms + 1500*ms)
	s, cal := calibrated(t, fakeOpts{Skew: 200 * ms, Precise: true},
		Config{Target: target, ServerTime: preciseExtract, CalibrationServerTime: preciseExtract, StopOnFirstHit: true})
	res, err := s.Fire(context.Background(), cal)
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	limit := res.Best.Received.Add(5 * ms)
	for _, as := range res.Attempts {
		for _, a := range as {
			if a.Sent.After(limit) {
				t.Errorf("worker %d attempt %d Sent %v, after Best.Received+5ms %v", a.Worker, a.N, a.Sent, limit)
			}
		}
	}
}

func TestFireTooLate(t *testing.T) {
	var count atomic.Int64
	target := time.Now().Add(-time.Second)
	s, cal := calibrated(t, fakeOpts{Precise: true, Count: &count},
		Config{Target: target, ServerTime: preciseExtract, CalibrationServerTime: preciseExtract})
	before := count.Load()
	res, err := s.Fire(context.Background(), cal)
	if !errors.Is(err, ErrTooLate) {
		t.Fatalf("err = %v, want ErrTooLate", err)
	}
	if after := count.Load(); after != before {
		t.Errorf("server saw %d requests during Fire, want 0", after-before)
	}
	if !res.Target.Equal(target) || len(res.Schedule.FireAt) == 0 || res.Calibration.TakenAt.IsZero() {
		t.Errorf("Result not filled: Target %v, FireAt %v, Calibration.TakenAt %v", res.Target, res.Schedule.FireAt, res.Calibration.TakenAt)
	}
}

func TestFireCancel(t *testing.T) {
	target := time.Now().Add(200*ms + 3*time.Second)
	s, cal := calibrated(t, fakeOpts{Skew: 200 * ms, Precise: true, Delay: 100 * ms},
		Config{Target: target, ServerTime: preciseExtract, CalibrationServerTime: preciseExtract})
	sched := plan(target, cal.Final, s.cfg.Workers, s.cfg.Spacing, 3*cal.RTT, s.cfg.GiveUp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelAt := sched.FireAt[0].Add(50 * ms)
	time.AfterFunc(time.Until(cancelAt), cancel)

	res, err := s.Fire(ctx, cal)
	returned := time.Now()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if d := returned.Sub(cancelAt); d > 200*ms {
		t.Errorf("Fire returned %v after cancel, want <= 200ms", d)
	}
	if res.Attempts == nil {
		t.Error("Attempts = nil")
	}
}

func TestFireInFlightAtDeadline(t *testing.T) {
	target := time.Now().Add(200*ms + 3*time.Second)
	forbid := func(time.Time) int { return 403 }
	s, cal := calibrated(t, fakeOpts{Skew: 200 * ms, Precise: true, Status: forbid, Delay: 300 * ms},
		Config{Target: target, ServerTime: preciseExtract, CalibrationServerTime: preciseExtract, GiveUp: 100 * ms})
	res, err := s.Fire(context.Background(), cal)
	if !errors.Is(err, ErrNoHit) {
		t.Fatalf("err = %v, want ErrNoHit", err)
	}
	straddled := false
	for _, as := range res.Attempts {
		for _, a := range as {
			if a.Sent.Before(res.Schedule.Deadline) && a.Received.After(res.Schedule.Deadline) && a.Err == nil {
				straddled = true
			}
		}
	}
	if !straddled {
		t.Error("no attempt sent before Deadline and received after it without error")
	}
	checkNoSendAtOrAfterDeadline(t, res)
}

func TestFireSlowRewarmBoundedByGrace(t *testing.T) {
	// Calibrate against a fast server; fire (and so rewarm) against one whose
	// Delay far exceeds GiveUp+InFlightGrace.
	fast := newFakeServer(t, fakeOpts{Skew: 200 * ms, Precise: true})
	target := time.Now().Add(200*ms + 1500*ms)
	const grace = 50 * ms
	s, cal := calibrated(t, fakeOpts{Skew: 200 * ms, Precise: true, Delay: 2 * time.Second},
		Config{Target: target, Calibration: reqFor(fast.URL), ServerTime: preciseExtract,
			CalibrationServerTime: preciseExtract, GiveUp: 50 * ms, InFlightGrace: grace})
	res, err := s.Fire(context.Background(), cal)
	returned := time.Now()
	if !errors.Is(err, ErrNoHit) {
		t.Fatalf("err = %v, want ErrNoHit", err)
	}
	if limit := res.Schedule.Deadline.Add(grace + 100*ms); returned.After(limit) {
		t.Errorf("Fire returned %v after Deadline, want <= grace+100ms (%v)", returned.Sub(res.Schedule.Deadline), grace+100*ms)
	}
}
