package kairos

import (
	"errors"
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
	if got, err := sweepEstimate(samples, time.Second); err != nil || got != want {
		t.Errorf("err %v, got %+v, want %+v", err, got, want)
	}

	// A stamp 5s late cannot be explained by a 50ms RTT, so it fails loudly
	// rather than being dropped.
	sent := base.Add(700 * ms)
	samples = append(samples, sweepSample{Sent: sent, Stamp: sent.Add(5 * time.Second).Truncate(time.Second), RTT: 50 * ms})
	if got, err := sweepEstimate(samples, time.Second); !errors.Is(err, ErrInconsistentSweep) {
		t.Errorf("with outlier: got %+v, err = %v, want ErrInconsistentSweep", got, err)
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

// jitterSamples returns samples sent at base whose stamp delay constraints
// are [l, l+1s) for each l in lowers, with RTTs 1ms and 3ms alternating so
// that J = (q90-q10)/2 = 1ms.
func jitterSamples(lowers ...time.Duration) []sweepSample {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var out []sweepSample
	for i, l := range lowers {
		rtt := 1 * ms
		if i%2 == 1 {
			rtt = 3 * ms
		}
		out = append(out, sweepSample{Sent: base, Stamp: base.Add(l), RTT: rtt})
	}
	return out
}

func TestSweepEstimateJitterWidens(t *testing.T) {
	// Constraints [299ms, 1299ms) and [-699ms, 301ms) intersect in [299, 301]; J = 1ms.
	got, err := sweepEstimate(jitterSamples(299*ms, -699*ms), time.Second)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	want := Estimate{SourceSweep, 300 * ms, 298 * ms, 302 * ms}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestSweepEstimateInconsistentWithinJitter(t *testing.T) {
	// max(lowers) = 301ms, min(uppers) = 300ms: a 1ms miss, within 2J = 2ms.
	got, err := sweepEstimate(jitterSamples(301*ms, -700*ms), time.Second)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	want := Estimate{SourceSweep, 300500 * time.Microsecond, 300 * ms, 301 * ms}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestSweepEstimateInconsistentBeyondJitter(t *testing.T) {
	// max(lowers) = 305ms, min(uppers) = 300ms: a 5ms miss, beyond 2J = 2ms.
	got, err := sweepEstimate(jitterSamples(305*ms, -700*ms), time.Second)
	if !errors.Is(err, ErrInconsistentSweep) {
		t.Fatalf("got %+v, err = %v, want ErrInconsistentSweep", got, err)
	}
}

// sweepMod perturbs one tickSweep sample: delay holds it at the server before
// it is stamped, adding the same to its RTT; fasterReturn shortens its return
// path, taking that off its RTT again.
type sweepMod struct{ delay, fasterReturn time.Duration }

// tickSweep simulates a fine pass: 51 samples 2ms apart straddling a Date
// tick, true stamp delay 68ms, RTT 30ms ±2ms (median 30ms, J = 2ms). Sample i
// is sent 118-2i ms before the tick, so undisturbed it has lower bound
// 118-2i ms if stamped after the tick (i >= 25) and upper bound 118-2i ms
// otherwise. Odd samples are stamped by a clock skew ahead.
func tickSweep(mods map[int]sweepMod, skew time.Duration) []sweepSample {
	const d = 68 * ms
	tick := time.Date(2026, 10, 24, 13, 0, 0, 0, time.UTC)
	var out []sweepSample
	for i := range 51 {
		sent := tick.Add(-118*ms + time.Duration(i)*2*ms)
		m := mods[i]
		stamp := sent.Add(d + m.delay)
		if i%2 == 1 {
			stamp = stamp.Add(skew)
		}
		rtt := 30*ms + time.Duration(i%5-2)*ms + m.delay - m.fasterReturn
		out = append(out, sweepSample{Sent: sent, Stamp: stamp.Truncate(time.Second), RTT: rtt})
	}
	return out
}

func TestSweepToleratesOneDelayedSample(t *testing.T) {
	clean, err := sweepEstimate(tickSweep(nil, 0), time.Second)
	if err != nil {
		t.Fatalf("clean sweep: %v", err)
	}
	if clean.Lo > 68*ms || clean.Hi < 68*ms {
		t.Fatalf("clean sweep %+v does not contain 68ms", clean)
	}
	// Sample 20 is held 20ms at the server, so it is stamped after the tick
	// (lower bound 78ms, above min(uppers) 70ms); its RTT 48ms is kept.
	got, err := sweepEstimate(tickSweep(map[int]sweepMod{20: {delay: 20 * ms}}, 0), time.Second)
	if err != nil {
		t.Fatalf("one delayed sample broke the sweep: %v", err)
	}
	if got != clean {
		t.Errorf("got %+v, want the clean estimate %+v", got, clean)
	}
}

func TestSweepRelaxesLongRTTSample(t *testing.T) {
	clean, err := sweepEstimate(tickSweep(nil, 0), time.Second)
	if err != nil {
		t.Fatalf("clean sweep: %v", err)
	}
	// Sample 20 is held 40ms: lower bound 78ms, RTT 68ms, above 2×median =
	// 60ms. It still constrains the sweep, relaxed by 38ms to 40ms.
	got, err := sweepEstimate(tickSweep(map[int]sweepMod{20: {delay: 40 * ms}}, 0), time.Second)
	if err != nil || got != clean {
		t.Errorf("err %v, got %+v, want the clean estimate %+v", err, got, clean)
	}
}

func TestSweepDelayedSampleFasterReturn(t *testing.T) {
	// Sample 10 (RTT 28ms undisturbed) is held 30ms, giving lower bound 98ms
	// against min(uppers) 70ms. A return path 3ms faster hides 3ms of the
	// delay: RTT 55ms, relaxation 25ms, relaxed lower 73ms. The 3ms left over
	// is within 2J = 4ms.
	got, err := sweepEstimate(tickSweep(map[int]sweepMod{10: {delay: 30 * ms, fasterReturn: 3 * ms}}, 0), time.Second)
	if err != nil {
		t.Fatalf("partially relaxed sample within jitter: %v", err)
	}
	if d := got.Point - 68*ms; d < -4*ms || d > 4*ms {
		t.Errorf("Point %v is more than 2J from 68ms (%+v)", got.Point, got)
	}

	// 7ms faster: relaxation 21ms, relaxed lower 77ms, 7ms beyond min(uppers).
	_, err = sweepEstimate(tickSweep(map[int]sweepMod{10: {delay: 30 * ms, fasterReturn: 7 * ms}}, 0), time.Second)
	var ise *InconsistentSweepError
	if !errors.As(err, &ise) {
		t.Fatalf("err = %v, want *InconsistentSweepError", err)
	}
	if ise.MaxLowerSample != 10 || ise.MaxLower != 77*ms || ise.Samples[10].Relaxation != 21*ms {
		t.Errorf("MaxLowerSample %d, MaxLower %v, relaxation %v; want 10, 77ms, 21ms",
			ise.MaxLowerSample, ise.MaxLower, ise.Samples[10].Relaxation)
	}
}

func TestSweepTwoBackendsInconsistent(t *testing.T) {
	// Odd samples come from a backend 40ms ahead. Sample 5 is the earliest it
	// stamps after the tick (lower bound 108ms, RTT 28ms so no relaxation);
	// sample 24 is the last even sample stamped before it (upper bound 70ms).
	samples := tickSweep(nil, 40*ms)
	_, err := sweepEstimate(samples, time.Second)
	if !errors.Is(err, ErrInconsistentSweep) {
		t.Fatalf("err = %v, want ErrInconsistentSweep", err)
	}
	var ise *InconsistentSweepError
	if !errors.As(err, &ise) {
		t.Fatalf("err = %v, want *InconsistentSweepError", err)
	}
	if ise.MedianRTT != 30*ms || ise.Jitter != 2*ms {
		t.Errorf("MedianRTT %v, Jitter %v; want 30ms, 2ms", ise.MedianRTT, ise.Jitter)
	}
	if ise.MaxLowerSample != 5 || ise.MaxLower != 108*ms || ise.MinUpperSample != 24 || ise.MinUpper != 70*ms {
		t.Errorf("max lower %v (sample %d), min upper %v (sample %d); want 108ms (5), 70ms (24)",
			ise.MaxLower, ise.MaxLowerSample, ise.MinUpper, ise.MinUpperSample)
	}
	if len(ise.Samples) != len(samples) {
		t.Fatalf("len(Samples) = %d, want %d", len(ise.Samples), len(samples))
	}
	for i, s := range ise.Samples {
		want := SweepSample{Sent: samples[i].Sent, Stamp: samples[i].Stamp, RTT: samples[i].RTT,
			Relaxation: max(0, samples[i].RTT-30*ms)}
		if s != want {
			t.Errorf("Samples[%d] = %+v, want %+v", i, s, want)
		}
	}
}

func TestSweepSlowCoarsePassStaleDate(t *testing.T) {
	// Every response carries the same stale Date. The coarse pass (10 samples
	// over 900ms) is slow, RTT 3ms, above 2×median = 2ms; the fine pass (51
	// samples 2ms apart, from 1s on) has RTT 1ms. The fine pass alone spans
	// 100ms, consistent with any stamp, so the slow coarse samples must still
	// constrain the sweep: coarse sample 0's lower bound, relaxed by 2ms,
	// misses the last fine sample's upper bound by 98ms.
	t0 := time.Date(2026, 10, 24, 13, 0, 0, 0, time.UTC)
	stale := t0.Add(-90 * time.Second)
	var samples []sweepSample
	for i := range 10 {
		samples = append(samples, sweepSample{Sent: t0.Add(time.Duration(i) * 100 * ms), Stamp: stale, RTT: 3 * ms})
	}
	for i := range 51 {
		samples = append(samples, sweepSample{Sent: t0.Add(time.Second + time.Duration(i)*2*ms), Stamp: stale, RTT: 1 * ms})
	}
	got, err := sweepEstimate(samples, time.Second)
	var ise *InconsistentSweepError
	if !errors.As(err, &ise) {
		t.Fatalf("got %+v, err = %v, want *InconsistentSweepError", got, err)
	}
	if ise.MaxLowerSample != 0 || ise.Samples[0].Relaxation != 2*ms || ise.MinUpperSample != 60 ||
		ise.MaxLower-ise.MinUpper != 98*ms {
		t.Errorf("max lower sample %d (relaxed %v), min upper sample %d, gap %v; want 0 (2ms), 60, 98ms",
			ise.MaxLowerSample, ise.Samples[0].Relaxation, ise.MinUpperSample, ise.MaxLower-ise.MinUpper)
	}
}
