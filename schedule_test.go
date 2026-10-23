package kairos

import (
	"testing"
	"time"
)

var planTarget = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)

func TestPlanNarrow(t *testing.T) {
	est := Estimate{Source: SourcePrecise, Point: 300 * ms, Lo: 300 * ms, Hi: 300 * ms}
	s := plan(planTarget, est, 8, 5*ms, 30*ms, 2*time.Second)
	if len(s.FireAt) != 8 {
		t.Fatalf("len(FireAt) = %d, want 8", len(s.FireAt))
	}
	if want := planTarget.Add(-330 * ms); !s.FireAt[0].Equal(want) {
		t.Errorf("FireAt[0] = %v, want %v", s.FireAt[0], want)
	}
	if want := planTarget.Add(-295 * ms); !s.FireAt[7].Equal(want) {
		t.Errorf("FireAt[7] = %v, want %v", s.FireAt[7], want)
	}
	if s.Spacing != 5*ms {
		t.Errorf("Spacing = %v, want 5ms", s.Spacing)
	}
	if want := planTarget.Add(-300 * ms); !s.Ideal.Equal(want) {
		t.Errorf("Ideal = %v, want %v", s.Ideal, want)
	}
	if want := planTarget.Add(-300*ms + 2*time.Second); !s.Deadline.Equal(want) {
		t.Errorf("Deadline = %v, want %v", s.Deadline, want)
	}
}

func TestPlanSpread(t *testing.T) {
	est := Estimate{Source: SourceSweep, Point: 300 * ms, Lo: 0, Hi: 600 * ms}
	s := plan(planTarget, est, 8, 5*ms, 30*ms, 2*time.Second)
	if want := planTarget.Add(-600 * ms); !s.FireAt[0].Equal(want) {
		t.Errorf("FireAt[0] = %v, want %v", s.FireAt[0], want)
	}
	if want := 600 * time.Millisecond / 7; s.Spacing != want {
		t.Errorf("Spacing = %v, want %v", s.Spacing, want)
	}
	if d := s.FireAt[7].Sub(planTarget).Abs(); d >= time.Microsecond {
		t.Errorf("FireAt[7] is %v from target, want < 1us", d)
	}
	if want := planTarget.Add(2 * time.Second); !s.Deadline.Equal(want) {
		t.Errorf("Deadline = %v, want %v", s.Deadline, want)
	}
}

func TestPlanSingleWorker(t *testing.T) {
	est := Estimate{Source: SourceSweep, Point: 300 * ms, Lo: 0, Hi: 600 * ms}
	s := plan(planTarget, est, 1, 5*ms, 30*ms, 2*time.Second)
	if len(s.FireAt) != 1 {
		t.Fatalf("len(FireAt) = %d, want 1", len(s.FireAt))
	}
	if want := planTarget.Add(-600 * ms); !s.FireAt[0].Equal(want) {
		t.Errorf("FireAt[0] = %v, want %v", s.FireAt[0], want)
	}
	if s.Spacing != 5*ms {
		t.Errorf("Spacing = %v, want 5ms", s.Spacing)
	}
}

func TestEstimateServerTime(t *testing.T) {
	sent := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	delay := 40 * ms
	stamp := func(offset time.Duration) time.Time { return sent.Add(offset) }

	tests := []struct {
		name string
		srv  ServerTime
		want time.Time
	}{
		{"precise unchanged", ServerTime{Time: stamp(7 * ms), Precision: ms}, stamp(7 * ms)},
		{"inside coarse window", ServerTime{Time: stamp(-300 * ms), Precision: time.Second}, sent.Add(delay)},
		{"before stamp clamps up", ServerTime{Time: stamp(500 * ms), Precision: time.Second}, stamp(500 * ms)},
		{"past window clamps down", ServerTime{Time: stamp(-2 * time.Second), Precision: time.Second}, stamp(-time.Second - time.Nanosecond)},
		{"zero server", ServerTime{}, sent.Add(delay)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := estimateServerTime(Attempt{Sent: sent, Server: tc.srv}, delay)
			if !got.Equal(tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
