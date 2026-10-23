// Package kairos lands HTTP requests just after a target time on a server's
// clock.
//
// The server's clock is learned by calibration, from up to three sources:
// a precise timestamp in the response body or header (see ServerTime
// extractors), a sweep of request deadlines against the coarse Date header,
// and an optional ReferenceClock such as SystemClock. Policy decides how the
// target and reference estimates are reconciled.
//
// Run calibrates and then fires staggered workers, each on its own keep-alive
// connection, so that one request lands just after Target. Use New, Calibrate
// and Fire directly for finer control.
//
// There is no built-in log format: the returned Result is the consumer's log.
// Each Attempt, together with Schedule.FireAt for its worker, records the
// fire time, send and receive times, RTT, server time, estimated server time
// (compare with Target), hit status and error, including attempts made before
// the target.
package kairos
