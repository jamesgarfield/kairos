package kairos

import (
	"fmt"
	"net/http"
	"time"
)

// Config controls a run. Zero values take the documented defaults.
type Config struct {
	Target            time.Time // server clock; required
	GiveUp            time.Duration
	Workers           int
	Spacing           time.Duration
	Lead              time.Duration // 0 = 3*Calibration.RTT
	CalibrationRounds int
	CalibrateBefore   time.Duration // Run only
	RewarmBefore      time.Duration
	Spin              time.Duration
	InFlightGrace     time.Duration
	RequestTimeout    time.Duration // calibration, rewarm, reference requests
	SweepStep         time.Duration
	MaxBodyBytes      int64

	Request               RequestFunc    // required
	Calibration           RequestFunc    // nil = Request
	ServerTime            TimeExtractor  // nil = DateHeader
	CalibrationServerTime TimeExtractor  // nil = ServerTime
	Reference             ReferenceClock // nil = none
	Policy                Policy
	IsHit                 func(Attempt) bool // nil = defaultHit
	Logger                Logger             // nil = no-op
	StopOnFirstHit        bool
	NewTransport          func() *http.Transport // nil = http.DefaultTransport.(*http.Transport).Clone()
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
			return http.DefaultTransport.(*http.Transport).Clone()
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
	return nil
}
