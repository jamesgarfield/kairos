package kairos

import "time"

// plan computes each worker's local fire time from the final estimate. When
// the uncertainty interval is wider than the workers' natural spacing, the
// workers are spread across it so that some request lands just after target.
func plan(target time.Time, final Estimate, workers int, spacing, lead, giveUp time.Duration) Schedule {
	ideal := target.Add(-final.Point)
	first := ideal.Add(-lead)
	if early := target.Add(-final.Hi); early.Before(first) {
		first = early
	}
	latest := target.Add(-final.Lo)

	eff := spacing
	if workers > 1 && first.Add(time.Duration(workers-1)*spacing).Before(latest) {
		eff = latest.Sub(first) / time.Duration(workers-1)
	}

	fireAt := make([]time.Time, workers)
	for i := range fireAt {
		fireAt[i] = first.Add(time.Duration(i) * eff)
	}
	return Schedule{
		Ideal:    ideal,
		FireAt:   fireAt,
		Spacing:  eff,
		Deadline: latest.Add(giveUp),
	}
}

// estimateServerTime returns the best estimate of the server clock when the
// attempt was processed. A precise stamp is used as-is; a coarse stamp bounds
// the estimate Sent+stampDelay to the stamp's precision window.
func estimateServerTime(a Attempt, stampDelay time.Duration) time.Time {
	if !a.Server.Time.IsZero() && a.Server.Precision <= preciseThreshold {
		return a.Server.Time
	}
	e := a.Sent.Add(stampDelay)
	if a.Server.Time.IsZero() {
		return e
	}
	lo := a.Server.Time
	hi := lo.Add(a.Server.Precision - time.Nanosecond)
	if e.Before(lo) {
		return lo
	}
	if e.After(hi) {
		return hi
	}
	return e
}
