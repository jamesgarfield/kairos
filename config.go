package kairos

import (
	"fmt"
	"net/http"
	"time"
)

// Config controls a run. Zero values take the documented defaults.
type Config struct {
	// Target is the instant, on the server's clock, at or just after which a
	// request should land. Required.
	Target time.Time

	// GiveUp bounds retries. It is on the local clock: the deadline, after
	// which nothing new is sent, is (Target - Final.Lo) + GiveUp, i.e. GiveUp
	// after the latest plausible send instant. Default 2s.
	GiveUp time.Duration

	// Workers is the number of workers, each with its own keep-alive
	// connection. Default 8.
	Workers int

	// Spacing is the minimum offset between consecutive workers' first sends.
	// The schedule widens it so the workers span the calibration uncertainty
	// plus Lead; with the default Lead (3*RTT) and Workers that happens
	// whenever the RTT is above about 12ms. Schedule.Spacing reports the
	// effective value. Default 5ms.
	Spacing time.Duration

	// Lead is how far before the ideal send instant the first worker fires
	// (earlier still if the calibration interval demands it), so early
	// workers retry into the window. 0 means 3*Calibration.RTT.
	Lead time.Duration

	// CalibrationRounds is the number of rounds of the precise calibration
	// method, in which every worker sends at once. Must be at least 1 after
	// defaults. Default 10.
	CalibrationRounds int

	// CalibrateBefore is used by Run only: it calibrates this long before
	// Target. A Date sweep takes about 1-2s. Default 30s.
	CalibrateBefore time.Duration

	// RewarmBefore is how long before each worker's slot it sends one real
	// Request to rewarm its connection. Rewarm requests are not recorded in
	// Result; a rewarm failure is only logged. Default 150ms.
	RewarmBefore time.Duration

	// Spin is how long the single releaser busy-waits before each release,
	// after sleeping on a timer. Each release spins at most half the gap since
	// the previous release. Default 2ms.
	Spin time.Duration

	// InFlightGrace is how long requests in flight at the deadline may run
	// before they are cancelled. Default 500ms.
	InFlightGrace time.Duration

	// RequestTimeout bounds each calibration, warm-up and rewarm request,
	// measured from its send instant. Fire attempts are bounded by the
	// deadline plus InFlightGrace instead. Default 5s.
	RequestTimeout time.Duration

	// SweepStep is the minimum step between fine-pass samples of a Date
	// sweep. Default 2ms.
	SweepStep time.Duration

	// MaxBodyBytes caps how much of each response body is read; a longer
	// body is silently truncated before extraction and IsHit. Default 1 MiB.
	MaxBodyBytes int64

	// Request builds a fresh real request for every attempt and rewarm.
	// Required. Attempts are retried immediately, without backoff, so a
	// fast-failing endpoint is retried in a tight loop until a hit or the
	// deadline. A build error or nil request stops that worker.
	Request RequestFunc

	// Calibration builds calibration and warm-up requests. nil means Request.
	// Calibration ignores the HTTP status, since it only needs the time: a
	// 5xx or 429 from an edge calibrates against that edge's clock. A
	// response with a non-zero Age header (a cached response) is an error.
	Calibration RequestFunc

	// ServerTime reads the server time from real responses. nil means
	// DateHeader.
	ServerTime TimeExtractor

	// CalibrationServerTime reads the server time from calibration responses.
	// nil means ServerTime. A precision of 10ms or finer selects the precise
	// calibration method; coarser selects the Date sweep.
	CalibrationServerTime TimeExtractor

	// Reference is an optional independent clock, reconciled with the
	// target's estimate by Policy. nil means none.
	Reference ReferenceClock

	// Policy decides how the target and Reference estimates are reconciled.
	// The zero value is PolicyInterval. It has no effect without Reference.
	Policy Policy

	// IsHit decides whether an attempt counts as a hit. nil means: no error,
	// a 2xx status, and Estimated at or after Target.
	IsHit func(Attempt) bool

	// Logger receives diagnostics, never between a release and its send.
	// nil means no-op.
	Logger Logger

	// StopOnFirstHit cancels all workers as soon as any worker gets a hit.
	StopOnFirstHit bool

	// NewTransport builds the base transport for one worker; it is called
	// once per worker and must return a new transport each time. Use it to
	// set TLS, proxy and client-certificate options. The library then tunes
	// it for keep-alive: MaxIdleConnsPerHost 1, IdleConnTimeout 5m and
	// DisableCompression. nil means a clone of http.DefaultTransport.
	NewTransport func() *http.Transport
}

type nopLogger struct{}

func (nopLogger) Logf(string, ...any) {}

func (c Config) withDefaults() Config {
	if c.Workers == 0 {
		c.Workers = 8
	}
	if c.Spacing == 0 {
		c.Spacing = 5 * time.Millisecond
	}
	if c.CalibrationRounds == 0 {
		c.CalibrationRounds = 10
	}
	if c.RewarmBefore == 0 {
		c.RewarmBefore = 150 * time.Millisecond
	}
	if c.Spin == 0 {
		c.Spin = 2 * time.Millisecond
	}
	if c.GiveUp == 0 {
		c.GiveUp = 2 * time.Second
	}
	if c.CalibrateBefore == 0 {
		c.CalibrateBefore = 30 * time.Second
	}
	if c.InFlightGrace == 0 {
		c.InFlightGrace = 500 * time.Millisecond
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.SweepStep == 0 {
		c.SweepStep = 2 * time.Millisecond
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = 1 << 20
	}
	if c.ServerTime == nil {
		c.ServerTime = DateHeader
	}
	if c.CalibrationServerTime == nil {
		c.CalibrationServerTime = c.ServerTime
	}
	if c.Calibration == nil {
		c.Calibration = c.Request
	}
	if c.Logger == nil {
		c.Logger = nopLogger{}
	}
	if c.NewTransport == nil {
		c.NewTransport = func() *http.Transport {
			if t, ok := http.DefaultTransport.(*http.Transport); ok {
				return t.Clone()
			}
			return &http.Transport{Proxy: http.ProxyFromEnvironment}
		}
	}
	return c
}

// validate checks a config that has already been through withDefaults.
func (c Config) validate() error {
	if c.Target.IsZero() {
		return fmt.Errorf("kairos: invalid config: Target is required")
	}
	if c.Request == nil {
		return fmt.Errorf("kairos: invalid config: Request is required")
	}
	if c.Workers < 0 {
		return fmt.Errorf("kairos: invalid config: Workers must not be negative (got %d)", c.Workers)
	}
	if c.Spacing < 0 {
		return fmt.Errorf("kairos: invalid config: Spacing must not be negative (got %v)", c.Spacing)
	}
	if c.Lead < 0 {
		return fmt.Errorf("kairos: invalid config: Lead must not be negative (got %v)", c.Lead)
	}
	if c.GiveUp < 0 {
		return fmt.Errorf("kairos: invalid config: GiveUp must not be negative (got %v)", c.GiveUp)
	}
	for _, d := range []struct {
		name string
		v    time.Duration
	}{
		{"Spin", c.Spin},
		{"RewarmBefore", c.RewarmBefore},
		{"CalibrateBefore", c.CalibrateBefore},
		{"InFlightGrace", c.InFlightGrace},
		{"RequestTimeout", c.RequestTimeout},
		{"SweepStep", c.SweepStep},
	} {
		if d.v < 0 {
			return fmt.Errorf("kairos: invalid config: %s must not be negative (got %v)", d.name, d.v)
		}
	}
	if c.MaxBodyBytes < 0 {
		return fmt.Errorf("kairos: invalid config: MaxBodyBytes must not be negative (got %d)", c.MaxBodyBytes)
	}
	if c.CalibrationRounds < 1 {
		return fmt.Errorf("kairos: invalid config: CalibrationRounds must be at least 1 (got %d)", c.CalibrationRounds)
	}
	switch c.Policy {
	case PolicyInterval, PolicyReferenceWins, PolicyTargetWins:
	default:
		return fmt.Errorf("kairos: invalid config: unknown Policy %d", c.Policy)
	}
	return nil
}
