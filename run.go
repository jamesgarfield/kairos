package kairos

import (
	"context"
	"time"
)

// Run is the one-call convenience: it creates a Session, waits until
// Target-CalibrateBefore, calibrates, and fires.
//
// If that moment has already passed it calibrates immediately. As with Fire,
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
	cal, err := s.Calibrate(ctx)
	if err != nil {
		return Result{}, err
	}
	return s.Fire(ctx, cal)
}
