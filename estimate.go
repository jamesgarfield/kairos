package kairos

import (
	"math"
	"slices"
	"time"
)

// preciseThreshold: extractor precision <= this selects the precise method.
const preciseThreshold = 10 * time.Millisecond

// quantile returns the element at index round(q*(n-1)) of a sorted copy of d,
// or 0 if d is empty.
func quantile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	i := int(math.Round(q * float64(len(s)-1)))
	i = max(0, min(i, len(s)-1))
	return s[i]
}

type sweepSample struct {
	Sent  time.Time
	Stamp time.Time
	RTT   time.Duration
}

// preciseEstimate summarizes directly observed stampDelays.
func preciseEstimate(delays []time.Duration) Estimate {
	return Estimate{
		Source: SourcePrecise,
		Point:  quantile(delays, 0.5),
		Lo:     quantile(delays, 0.1),
		Hi:     quantile(delays, 0.9),
	}
}

// sweepMedianRTT returns the median RTT of samples, the basis of each
// sample's relaxation and of the jitter cutoff.
func sweepMedianRTT(samples []sweepSample) time.Duration {
	rtts := make([]time.Duration, len(samples))
	for i, s := range samples {
		rtts[i] = s.RTT
	}
	return quantile(rtts, 0.5)
}

// keptSweepSamples drops samples whose RTT exceeds twice the median RTT. The
// rest are typical of the path: they set the sweep jitter and the RTTs a
// Calibration reports.
func keptSweepSamples(samples []sweepSample) []sweepSample {
	cutoff := 2 * sweepMedianRTT(samples)
	var kept []sweepSample
	for _, s := range samples {
		if s.RTT <= cutoff {
			kept = append(kept, s)
		}
	}
	return kept
}

// sweepRelaxation is how far a sample's lower bound is lowered: its RTT in
// excess of the median.
func sweepRelaxation(rtt, median time.Duration) time.Duration {
	return max(0, rtt-median)
}

// sweepEstimate intersects the per-sample constraints on the stamp delay D.
// Each stamp is truncated to precision, so a sample constrains D to
// [Stamp-Sent, Stamp+precision-Sent).
//
// A sample held at the server before it is stamped gets a late stamp, which
// raises its lower bound by the hold; it cannot lower any bound. The hold also
// adds to its RTT, so each sample's lower bound is relaxed by its RTT in
// excess of the median (sweepRelaxation): a delayed sample is discounted by
// what its own RTT shows, and no more. A hold hidden by a faster return path
// is left to the jitter. The data cannot tell a hold from a clock that is
// ahead, so a backend whose clock leads and whose RTT is longer by as much
// has its lead discounted as delay, and the estimate follows the other
// backends.
//
// Every sample constrains D, however long its RTT: relaxation already
// discounts its lower bound, and its upper bound holds since a hold only makes
// a stamp later. Dropping slow samples would lose constraints; if a slow
// coarse pass were dropped, the fine pass alone spans too little to expose a
// stale Date.
//
// The intersection is widened on each side by the sweep jitter J, half the
// q10..q90 RTT spread of keptSweepSamples. If the constraints still miss each
// other, that is max(lowers)-J > min(uppers)+J, it returns an
// *InconsistentSweepError.
func sweepEstimate(samples []sweepSample, precision time.Duration) (Estimate, error) {
	if len(samples) == 0 {
		return Estimate{Source: SourceSweep}, nil
	}
	med := sweepMedianRTT(samples)
	var lo, hi time.Duration
	loAt, hiAt := -1, -1
	for i, s := range samples {
		l := s.Stamp.Sub(s.Sent)
		u := l + precision
		l -= sweepRelaxation(s.RTT, med)
		if loAt < 0 || l > lo {
			lo, loAt = l, i
		}
		if hiAt < 0 || u < hi {
			hi, hiAt = u, i
		}
	}
	kept := keptSweepSamples(samples)
	rtts := make([]time.Duration, len(kept))
	for i, s := range kept {
		rtts[i] = s.RTT
	}
	j := (quantile(rtts, 0.9) - quantile(rtts, 0.1)) / 2
	if lo-j > hi+j {
		return Estimate{}, inconsistentSweep(samples, med, j, lo, loAt, hi, hiAt)
	}
	// A miss within the jitter (0 < lo-hi <= 2J) leaves lo-J <= hi+J, so the
	// widened interval is always ordered.
	lo -= j
	hi += j
	return Estimate{Source: SourceSweep, Point: lo + (hi-lo)/2, Lo: lo, Hi: hi}, nil
}

// inconsistentSweep builds the *InconsistentSweepError for a failed
// sweepEstimate.
func inconsistentSweep(samples []sweepSample, med, j, lo time.Duration, loAt int, hi time.Duration, hiAt int) error {
	out := make([]SweepSample, len(samples))
	for i, s := range samples {
		out[i] = SweepSample{Sent: s.Sent, Stamp: s.Stamp, RTT: s.RTT, Relaxation: sweepRelaxation(s.RTT, med)}
	}
	return &InconsistentSweepError{
		Samples:        out,
		MedianRTT:      med,
		Jitter:         j,
		MaxLower:       lo,
		MaxLowerSample: loAt,
		MinUpper:       hi,
		MinUpperSample: hiAt,
	}
}

// referenceEstimate converts a reference clock offset and its RTTs into an
// Estimate, assuming the one-way delay is half the RTT.
func referenceEstimate(offset time.Duration, rtts []time.Duration) Estimate {
	return Estimate{
		Source: SourceReference,
		Point:  offset + quantile(rtts, 0.5)/2,
		Lo:     offset + quantile(rtts, 0.1)/2,
		Hi:     offset + quantile(rtts, 0.9)/2,
	}
}

// combine reconciles the target estimate with an optional reference estimate.
// disagreement is ref.Point minus its clamp to the target interval.
func combine(target Estimate, ref *Estimate, p Policy) (final Estimate, disagreement time.Duration) {
	if ref == nil {
		return target, 0
	}
	c := max(target.Lo, min(ref.Point, target.Hi))
	disagreement = ref.Point - c
	switch p {
	case PolicyReferenceWins:
		return *ref, disagreement
	case PolicyTargetWins:
		return target, disagreement
	}
	if disagreement == 0 {
		return Estimate{
			Source: SourceReference,
			Point:  ref.Point,
			Lo:     max(ref.Lo, target.Lo),
			Hi:     min(ref.Hi, target.Hi),
		}, 0
	}
	return Estimate{Source: target.Source, Point: c, Lo: target.Lo, Hi: target.Hi}, disagreement
}
