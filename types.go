package kairos

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// RequestFunc builds a fresh request bound to ctx.
type RequestFunc func(ctx context.Context) (*http.Request, error)

// Logger receives diagnostic output.
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
	SourcePrecise Source = iota + 1
	SourceSweep
	SourceReference
)

// Estimate is a clock offset estimate; stampDelay = server - local at send.
type Estimate struct {
	Source        Source
	Point, Lo, Hi time.Duration
}

// Policy decides how target and reference estimates are reconciled.
type Policy int

const (
	PolicyInterval Policy = iota
	PolicyReferenceWins
	PolicyTargetWins
)

// Calibration is the result of clock calibration.
type Calibration struct {
	TakenAt      time.Time
	Target       Estimate  // SourcePrecise or SourceSweep
	Reference    *Estimate // nil if no ReferenceClock
	Final        Estimate
	Disagreement time.Duration // Reference.Point - clamp(Reference.Point, Target.Lo, Target.Hi); 0 if no reference
	RTT          time.Duration // median RTT against the calibration endpoint
	WakeLatency  time.Duration
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
	ErrNoHit   = errors.New("kairos: no hit")
	ErrTooLate = errors.New("kairos: target too soon")
)
