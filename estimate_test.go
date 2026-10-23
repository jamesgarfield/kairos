package kairos

import (
	"testing"
	"time"
)

const ms = time.Millisecond

func msRange(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range out {
		out[i] = time.Duration(i) * ms
	}
	return out
}

func TestQuantile(t *testing.T) {
	d := []time.Duration{5 * ms, 1 * ms, 4 * ms, 2 * ms, 3 * ms}
	if got := quantile(d, 0.5); got != 3*ms {
		t.Errorf("q.5 = %v, want 3ms", got)
	}
	if got := quantile(d, 0); got != 1*ms {
		t.Errorf("q0 = %v, want 1ms", got)
	}
	if got := quantile(d, 1); got != 5*ms {
		t.Errorf("q1 = %v, want 5ms", got)
	}
	if d[0] != 5*ms {
		t.Errorf("input was mutated: %v", d)
	}
	if got := quantile(msRange(10), 0.5); got != 5*ms {
		t.Errorf("q.5 of 0..9 = %v, want 5ms", got)
	}
	if got := quantile(nil, 0.5); got != 0 {
		t.Errorf("empty = %v, want 0", got)
	}
}

func TestPreciseEstimate(t *testing.T) {
	got := preciseEstimate(msRange(10))
	want := Estimate{SourcePrecise, 5 * ms, 1 * ms, 8 * ms}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestSweepEstimate(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	d := 300 * ms
	var samples []sweepSample
	for _, off := range []time.Duration{-3, -1, 1, 3} {
		sent := base.Add(700*ms + off*ms)
		samples = append(samples, sweepSample{Sent: sent, Stamp: sent.Add(d).Truncate(time.Second), RTT: 1 * ms})
	}
	want := Estimate{SourceSweep, 300 * ms, 299 * ms, 301 * ms}
	if got := sweepEstimate(samples, time.Second); got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}

	// Outlier with a wildly inconsistent stamp and a long RTT is dropped.
	sent := base.Add(700 * ms)
	samples = append(samples, sweepSample{Sent: sent, Stamp: sent.Add(5 * time.Second).Truncate(time.Second), RTT: 50 * ms})
	if got := sweepEstimate(samples, time.Second); got != want {
		t.Errorf("with outlier: got %+v, want %+v", got, want)
	}
}

func TestReferenceEstimate(t *testing.T) {
	got := referenceEstimate(100*ms, msRange(10))
	want := Estimate{SourceReference, 102500 * time.Microsecond, 100500 * time.Microsecond, 104 * ms}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestCombine(t *testing.T) {
	target := Estimate{SourceSweep, 300 * ms, 290 * ms, 310 * ms}
	r := func(p, lo, hi time.Duration) *Estimate {
		return &Estimate{SourceReference, p * ms, lo * ms, hi * ms}
	}
	near := r(305, 304, 306)
	far := r(350, 349, 351)
	low := r(250, 249, 251)
	tests := []struct {
		name    string
		ref     *Estimate
		policy  Policy
		want    Estimate
		wantDis time.Duration
	}{
		{"interval agree", near, PolicyInterval, Estimate{SourceReference, 305 * ms, 304 * ms, 306 * ms}, 0},
		{"interval high disagree", far, PolicyInterval, Estimate{SourceSweep, 310 * ms, 290 * ms, 310 * ms}, 40 * ms},
		{"reference wins", far, PolicyReferenceWins, *far, 40 * ms},
		{"target wins", far, PolicyTargetWins, target, 40 * ms},
		{"nil ref", nil, PolicyInterval, target, 0},
		{"nil ref reference wins", nil, PolicyReferenceWins, target, 0},
		{"interval low disagree", low, PolicyInterval, Estimate{SourceSweep, 290 * ms, 290 * ms, 310 * ms}, -40 * ms},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, dis := combine(target, tt.ref, tt.policy)
			if got != tt.want || dis != tt.wantDis {
				t.Errorf("got %+v dis %v, want %+v dis %v", got, dis, tt.want, tt.wantDis)
			}
		})
	}
}
