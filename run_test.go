package kairos

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func runPrecise(t *testing.T, calibrateBefore, targetIn time.Duration) (Result, error) {
	t.Helper()
	srv := newFakeServer(t, fakeOpts{Skew: 100 * ms, Precise: true})
	return Run(context.Background(), Config{
		Target:                time.Now().Add(100*ms + targetIn),
		CalibrateBefore:       calibrateBefore,
		CalibrationRounds:     3,
		Request:               reqFor(srv.URL),
		ServerTime:            preciseExtract,
		CalibrationServerTime: preciseExtract,
	})
}

func TestRun(t *testing.T) {
	res, err := runPrecise(t, time.Second, 2500*ms)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Best == nil {
		t.Fatal("Best is nil")
	}
}

func TestRunCalibrateBeforePassed(t *testing.T) {
	res, err := runPrecise(t, 30*time.Second, 1500*ms)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Best == nil {
		t.Fatal("Best is nil")
	}
}

func TestRunResultComplete(t *testing.T) {
	res, err := runPrecise(t, time.Second, 2500*ms)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Schedule.FireAt) != len(res.Attempts) {
		t.Fatalf("len(FireAt) = %d, len(Attempts) = %d", len(res.Schedule.FireAt), len(res.Attempts))
	}
	pre := false
	for w, as := range res.Attempts {
		for i, a := range as {
			if a.Worker != w || a.N != i+1 {
				t.Errorf("Attempts[%d][%d] has Worker %d, N %d", w, i, a.Worker, a.N)
			}
			if a.Sent.IsZero() || a.Received.IsZero() {
				t.Errorf("Attempts[%d][%d] has zero Sent or Received", w, i)
			}
			if a.Estimated.Before(res.Target) {
				pre = true
			}
		}
	}
	if !pre {
		t.Error("no attempt with Estimated before Target; pre-target attempts were dropped")
	}
}

func TestRunCancelledDuringWait(t *testing.T) {
	srv := newFakeServer(t, fakeOpts{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*ms)
	defer cancel()
	_, err := Run(ctx, Config{Target: time.Now().Add(time.Hour), Request: reqFor(srv.URL)})
	if err != context.DeadlineExceeded {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}

// flipLogger records messages and calls onLog after each one.
type flipLogger struct {
	recordingLogger
	onLog func()
}

func (l *flipLogger) Logf(format string, args ...any) {
	l.recordingLogger.Logf(format, args...)
	l.onLog()
}

// retries counts the logged calibration retries.
func retries(l *flipLogger) int {
	n := 0
	for _, m := range l.messages() {
		if strings.Contains(m, "retrying") {
			n++
		}
	}
	return n
}

// runSplit runs a Date sweep against a server that, while split is set,
// alternates per request between two clocks 90s apart, so any subset of the
// sweep's samples is inconsistent. Each logged message calls onLog.
func runSplit(t *testing.T, split *atomic.Bool, onLog func(), targetIn time.Duration, attempts int) (*flipLogger, Result, error) {
	t.Helper()
	var n atomic.Int64
	srv := newFakeServer(t, fakeOpts{Date: func(now time.Time) time.Time {
		if split.Load() && n.Add(1)%2 == 0 {
			return now.Add(-90 * time.Second)
		}
		return now
	}})
	log := &flipLogger{onLog: onLog}
	res, err := Run(context.Background(), Config{
		Target:              time.Now().Add(targetIn),
		Workers:             2,
		GiveUp:              200 * ms,
		StopOnFirstHit:      true,
		CalibrationAttempts: attempts,
		Request:             reqFor(srv.URL),
		Logger:              log,
	})
	return log, res, err
}

func TestRunRetriesInconsistentSweep(t *testing.T) {
	var split atomic.Bool
	split.Store(true)
	// The first attempt fails; its retry log reunites the clocks.
	log, res, err := runSplit(t, &split, func() { split.Store(false) }, 7*time.Second, 3)
	if err != nil {
		t.Fatalf("Run: %v (log %q)", err, log.messages())
	}
	if n := retries(log); n != 1 {
		t.Errorf("%d retries, want 1 (log %q)", n, log.messages())
	}
	if res.Best == nil {
		t.Error("Best is nil")
	}
}

func TestRunGivesUpAfterCalibrationAttempts(t *testing.T) {
	var split atomic.Bool
	split.Store(true)
	log, _, err := runSplit(t, &split, func() {}, 20*time.Second, 2)
	var ise *InconsistentSweepError
	if !errors.As(err, &ise) {
		t.Fatalf("err = %v, want *InconsistentSweepError", err)
	}
	if n := retries(log); n != 1 {
		t.Errorf("%d retries, want 1 (log %q)", n, log.messages())
	}
}

func TestRunNoRetryWithoutTime(t *testing.T) {
	var split atomic.Bool
	split.Store(true)
	// A sweep takes over 1s, so a second one cannot also leave that long
	// before a target 2s away.
	log, _, err := runSplit(t, &split, func() {}, 2*time.Second, 3)
	if !errors.Is(err, ErrInconsistentSweep) {
		t.Fatalf("err = %v, want ErrInconsistentSweep", err)
	}
	if n := retries(log); n != 0 {
		t.Errorf("%d retries, want 0 (log %q)", n, log.messages())
	}
}

func TestRunNoRetryOtherCalibrationErrors(t *testing.T) {
	srv := newFakeServer(t, fakeOpts{NoDate: true})
	log := &flipLogger{onLog: func() {}}
	_, err := Run(context.Background(), Config{
		Target:  time.Now().Add(20 * time.Second),
		Request: reqFor(srv.URL),
		Logger:  log,
	})
	if err == nil || errors.Is(err, ErrInconsistentSweep) {
		t.Fatalf("err = %v, want a calibration probe error", err)
	}
	if n := retries(log); n != 0 {
		t.Errorf("%d retries, want 0 (log %q)", n, log.messages())
	}
}
