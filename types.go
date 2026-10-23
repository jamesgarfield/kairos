package kairos

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// RequestFunc builds a fresh request bound to ctx. It must not return a nil
// request with a nil error.
type RequestFunc func(ctx context.Context) (*http.Request, error)

// Logger receives diagnostic output. It must be safe for concurrent use.
type Logger interface {
	Logf(format string, args ...any)
}

// ServerTime is a server clock reading and the precision of that reading.
type ServerTime struct {
	Time      time.Time
	Precision time.Duration
}

// Attempt records one request sent by a worker.
type Attempt struct {
	Worker, N      int // N is 1-based within the worker; rewarm requests are not Attempts
	Sent, Received time.Time
	StatusCode     int
	Header         http.Header
	Body           []byte
	Server         ServerTime // zero if extraction failed
	Estimated      time.Time  // best estimate of server time at processing
	Hit            bool
	Err            error // transport, read, or extract error; status codes are NOT errors
}

// RTT is the round-trip time of the attempt.
func (a Attempt) RTT() time.Duration { return a.Received.Sub(a.Sent) }

// Source identifies how an Estimate was produced.
type Source int

const (
	// SourcePrecise: the median of precise server stamps minus send times.
	SourcePrecise Source = iota + 1
	// SourceSweep: a Date-header sweep that locates the instant the coarse
	// stamp ticks over.
	SourceSweep
	// SourceReference: a ReferenceClock offset plus half the RTT.
	SourceReference
)

// Estimate is an estimate of the stamp delay: the server clock minus the
// local send time, i.e. clock offset plus one-way latency.
type Estimate struct {
	Source Source
	Point  time.Duration // best estimate
	Lo, Hi time.Duration // plausible interval, Lo <= Point <= Hi
}

// Policy decides how the target and reference estimates are reconciled. It
// matters only when a ReferenceClock is configured.
type Policy int

const (
	// PolicyInterval (the default) uses the reference point when it lies in
	// the target interval, narrowing the interval to their intersection, and
	// otherwise clamps it to the nearest edge and records the Disagreement.
	PolicyInterval Policy = iota
	// PolicyReferenceWins uses the reference estimate; the target estimate
	// only yields a logged Disagreement. For a Date served by an edge whose
	// clock differs from the app's.
	PolicyReferenceWins
	// PolicyTargetWins uses the target estimate and ignores the reference
	// beyond recording the Disagreement.
	PolicyTargetWins
)

// Calibration is the result of clock calibration. All durations are stamp
// delays (see Estimate) except RTT and WakeLatency.
type Calibration struct {
	TakenAt      time.Time     // local time calibration finished; Result.CalibrationAge is measured from it
	Target       Estimate      // SourcePrecise or SourceSweep
	Reference    *Estimate     // nil if no ReferenceClock
	Final        Estimate      // the reconciled estimate (see Policy) that Fire schedules from
	Disagreement time.Duration // Reference.Point - clamp(Reference.Point, Target.Lo, Target.Hi); 0 if no reference
	RTT          time.Duration // median RTT against the calibration endpoint
	WakeLatency  time.Duration // median release-to-wake delay; releases are moved this much earlier
}

// Schedule is the planned local send timing.
type Schedule struct {
	Ideal    time.Time     // local: Target - Final.Point
	FireAt   []time.Time   // local, per worker
	Spacing  time.Duration // effective, after spread
	Deadline time.Time     // local: no sends at or after this
}

// Result is the outcome of a run.
type Result struct {
	Target         time.Time
	Calibration    Calibration
	CalibrationAge time.Duration // at Fire start
	Schedule       Schedule
	Attempts       [][]Attempt // indexed by worker
	Best           *Attempt    // hit with the earliest Estimated; nil if none
}

var (
	// ErrNoHit is returned by Fire and Run, with the full Result, when no
	// attempt was a hit before the deadline.
	ErrNoHit = errors.New("kairos: no hit")

	// ErrTooLate is returned, wrapped, by Fire and Run when the first
	// worker's fire time has already passed. Nothing is sent.
	ErrTooLate = errors.New("kairos: target too soon to fire")

	// ErrInconsistentSweep is returned, wrapped with the gap, by Calibrate
	// and Run when the samples of a Date sweep cannot all be right: their
	// constraints miss each other by more than the measured jitter.
	ErrInconsistentSweep = errors.New("kairos: inconsistent Date sweep: the samples' constraints do not intersect; " +
		"likely a cached or stale Date, or several backends with different clocks")
)
