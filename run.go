package kairos

import (
	"context"
	"errors"
	"time"
)

// Run is the one-call convenience: it creates a Session, waits until
// Target-CalibrateBefore, calibrates, and fires.
//
// If that moment has already passed it calibrates immediately. A calibration
// that fails with ErrInconsistentSweep is retried, up to CalibrationAttempts
// in all, while there is time (see CalibrationAttempts); each failure is
// logged and the last is returned. As with Fire,
// a Result is returned alongside ErrNoHit, a wrapped ErrTooLate, or the
// context error. A New or Calibrate failure is returned with a zero Result;
// cancellation while waiting returns the context error.
func Run(ctx context.Context, cfg Config) (Result, error) {
	s, err := New(cfg)
	if err != nil {
		return Result{}, err
	}
	defer s.Close()

	if at := s.cfg.Target.Add(-s.cfg.CalibrateBefore); time.Now().Before(at) {
		if err := sleepUntil(ctx, at, 0); err != nil {
			return Result{}, err
		}
	}
	cal, err := s.calibrateRetrying(ctx)
	if err != nil {
		return Result{}, err
	}
	return s.Fire(ctx, cal)
}

// calibrateRetrying calls Calibrate, retrying an ErrInconsistentSweep as
// CalibrationAttempts describes.
func (s *Session) calibrateRetrying(ctx context.Context) (Calibration, error) {
	n := s.cfg.CalibrationAttempts
	for i := 1; ; i++ {
		start := time.Now()
		cal, err := s.Calibrate(ctx)
		if err == nil || !errors.Is(err, ErrInconsistentSweep) || i == n {
			return cal, err
		}
		took := time.Since(start)
		if !time.Now().Add(2 * took).Before(s.cfg.Target) {
			s.cfg.Logger.Logf("kairos: calibration attempt %d of %d failed, no time left to retry: %v", i, n, err)
			return cal, err
		}
		s.cfg.Logger.Logf("kairos: calibration attempt %d of %d failed, retrying: %v", i, n, err)
	}
}
