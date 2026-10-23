package kairos

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *recordingLogger) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.msgs...)
}

func newCalibrateSession(t *testing.T, o fakeOpts, cfg Config) *Session {
	t.Helper()
	srv := newFakeServer(t, o)
	cfg.Target = time.Now().Add(time.Hour)
	cfg.Request = reqFor(srv.URL)
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestCalibratePrecise(t *testing.T) {
	s := newCalibrateSession(t, fakeOpts{Skew: 300 * ms, Precise: true},
		Config{CalibrationServerTime: preciseExtract, Workers: 4})
	cal, err := s.Calibrate(context.Background())
	if err != nil {
		t.Fatalf("Calibrate: %v", err)
	}
	if cal.Target.Source != SourcePrecise {
		t.Errorf("Target.Source = %v, want SourcePrecise", cal.Target.Source)
	}
	if p := cal.Final.Point; p < 300*ms || p > 310*ms {
		t.Errorf("Final.Point = %v, want [300ms, 310ms]", p)
	}
	if cal.RTT <= 0 {
		t.Errorf("RTT = %v, want > 0", cal.RTT)
	}
	if cal.Reference != nil {
		t.Errorf("Reference = %+v, want nil", cal.Reference)
	}
	if cal.TakenAt.IsZero() {
		t.Error("TakenAt not set")
	}
}

func TestCalibrateSweep(t *testing.T) {
	s := newCalibrateSession(t, fakeOpts{Skew: 300 * ms}, Config{})
	start := time.Now()
	cal, err := s.Calibrate(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Calibrate: %v", err)
	}
	if cal.Target.Source != SourceSweep {
		t.Errorf("Target.Source = %v, want SourceSweep", cal.Target.Source)
	}
	tg := cal.Target
	if tg.Lo > 301*ms || tg.Hi < 299*ms {
		t.Errorf("Target = [%v, %v], want to contain ~300ms", tg.Lo, tg.Hi)
	}
	if w := tg.Hi - tg.Lo; w > 20*ms {
		t.Errorf("Target width = %v, want <= 20ms", w)
	}
	if elapsed >= 4*time.Second {
		t.Errorf("Calibrate took %v, want < 4s", elapsed)
	}
	if cal.RTT <= 0 {
		t.Errorf("RTT = %v, want > 0", cal.RTT)
	}
}

func TestCalibrateSweepWithReferenceAgree(t *testing.T) {
	s := newCalibrateSession(t, fakeOpts{}, Config{Reference: SystemClock{}})
	cal, err := s.Calibrate(context.Background())
	if err != nil {
		t.Fatalf("Calibrate: %v", err)
	}
	if cal.Final.Source != SourceReference {
		t.Errorf("Final.Source = %v, want SourceReference (target %+v, reference %+v)", cal.Final.Source, cal.Target, cal.Reference)
	}
	if cal.Disagreement != 0 {
		t.Errorf("Disagreement = %v, want 0", cal.Disagreement)
	}
	if p := cal.Final.Point.Abs(); p >= 5*ms {
		t.Errorf("|Final.Point| = %v, want < 5ms", p)
	}
	if cal.Reference == nil {
		t.Error("Reference = nil")
	}
}

func TestCalibrateSweepWithReferenceDisagree(t *testing.T) {
	log := &recordingLogger{}
	s := newCalibrateSession(t, fakeOpts{Skew: 300 * ms}, Config{Reference: SystemClock{}, Logger: log})
	cal, err := s.Calibrate(context.Background())
	if err != nil {
		t.Fatalf("Calibrate: %v", err)
	}
	if cal.Final.Point != cal.Target.Lo {
		t.Errorf("Final.Point = %v, want Target.Lo %v", cal.Final.Point, cal.Target.Lo)
	}
	if cal.Disagreement >= -250*ms {
		t.Errorf("Disagreement = %v, want < -250ms", cal.Disagreement)
	}
	if msgs := log.messages(); len(msgs) != 1 {
		t.Errorf("logger got %d messages, want 1: %q", len(msgs), msgs)
	}
}

func TestCalibrateNoDate(t *testing.T) {
	s := newCalibrateSession(t, fakeOpts{NoDate: true}, Config{})
	start := time.Now()
	_, err := s.Calibrate(context.Background())
	elapsed := time.Since(start)
	if err == nil || !strings.Contains(err.Error(), "calibration") || !strings.Contains(err.Error(), "server time") {
		t.Fatalf("err = %v, want calibration server time error", err)
	}
	if elapsed >= time.Second {
		t.Errorf("Calibrate took %v, want < 1s", elapsed)
	}
}

func TestCalibrateStatusIgnored(t *testing.T) {
	s := newCalibrateSession(t, fakeOpts{Status: func(time.Time) int { return 403 }}, Config{})
	if _, err := s.Calibrate(context.Background()); err != nil {
		t.Fatalf("Calibrate: %v", err)
	}
}

func TestCalibrateSweepShortRequestTimeout(t *testing.T) {
	s := newCalibrateSession(t, fakeOpts{Skew: 300 * ms}, Config{RequestTimeout: 500 * ms})
	cal, err := s.Calibrate(context.Background())
	if err != nil {
		t.Fatalf("Calibrate: %v", err)
	}
	if cal.Target.Source != SourceSweep {
		t.Errorf("Target.Source = %v, want SourceSweep", cal.Target.Source)
	}
}
