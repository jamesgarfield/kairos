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

// keptSweepSamples drops samples whose RTT exceeds twice the median RTT.
func keptSweepSamples(samples []sweepSample) []sweepSample {
	rtts := make([]time.Duration, len(samples))
	for i, s := range samples {
		rtts[i] = s.RTT
	}
	cutoff := 2 * quantile(rtts, 0.5)
	var kept []sweepSample
	for _, s := range samples {
		if s.RTT <= cutoff {
			kept = append(kept, s)
		}
	}
	return kept
}

// sweepEstimate intersects the per-sample constraints on the stamp delay D.
// Each stamp is truncated to precision, so a sample constrains D to
// [Stamp-Sent, Stamp+precision-Sent).
func sweepEstimate(samples []sweepSample, precision time.Duration) Estimate {
	var kept []time.Duration
	var lo, hi time.Duration
	n := 0
	for _, s := range keptSweepSamples(samples) {
		kept = append(kept, s.RTT)
		l := s.Stamp.Sub(s.Sent)
		u := l + precision
		if n == 0 {
			lo, hi = l, u
		} else {
			lo = max(lo, l)
			hi = min(hi, u)
		}
		n++
	}
	if n == 0 {
		return Estimate{Source: SourceSweep}
	}
	j := (quantile(kept, 0.9) - quantile(kept, 0.1)) / 2
	lo -= j
	hi += j
	if lo > hi {
		lo, hi = hi, lo
	}
	return Estimate{Source: SourceSweep, Point: lo + (hi-lo)/2, Lo: lo, Hi: hi}
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
