package kairos

import (
	"context"
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
