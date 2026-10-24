# kairos

A Go library for landing an HTTP request at, or as soon as possible after, a
target instant **on a remote server's clock**. Use it when a booking window,
auction or trading session opens at a set time, or when a job must start
exactly when business opens.

kairos measures the server's clock, schedules a staggered burst of workers so
that one request arrives just after the target, and hands back every attempt
for you to inspect or log.

```sh
go get github.com/jamesgarfield/kairos
```

Requires Go 1.26. Standard library only.

## Quick start

```go
cfg := kairos.Config{
	Target: time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC), // server clock
	Request: func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://example.com/open", strings.NewReader(`{"action":"open"}`))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Idempotency-Key", "open-2026-11-01")
		return req, nil
	},
	ServerTime: kairos.DateHeader,   // the default; shown for clarity
	Reference:  kairos.SystemClock{}, // optional: trust an NTP-synced local clock
}

res, err := kairos.Run(ctx, cfg)
switch {
case errors.Is(err, kairos.ErrNoHit):
	// No hit before the deadline; res still holds every attempt.
case err != nil:
	return err
}
fmt.Printf("best hit: worker %d, %v after target\n",
	res.Best.Worker, res.Best.Estimated.Sub(res.Target))
```

`Run` can be called well before the target. It waits until `CalibrateBefore`
ahead of the target (30s by default), then calibrates and fires.

For finer control, split the steps. For example, you can calibrate early,
check the numbers, and then commit:

```go
s, err := kairos.New(cfg)
if err != nil {
	return err
}
defer s.Close()

cal, err := s.Calibrate(ctx)
if err != nil {
	return err
}
// Inspect cal.Final, cal.RTT and cal.Disagreement here.

res, err := s.Fire(ctx, cal)
```

## How it works

1. **Calibrate.** kairos measures the *stamp delay*: the server's clock minus
   your local send time. That is clock offset plus one-way latency in one
   number. It combines up to three sources:
   - **A precise server stamp.** If your `CalibrationServerTime` extractor
     reports a precision of 10ms or finer, every worker sends together for
     `CalibrationRounds` rounds and kairos takes the median.
   - **A `Date`-header sweep.** If the server only gives the 1-second `Date`
     header, kairos fires requests stepped across a second boundary and finds
     the instant the header ticks over. That narrows the delay to a few
     milliseconds and takes about 1–2s.
   - **A reference clock** (optional). `SystemClock` assumes your machine is
     NTP-synced. `HTTPReference` reads precise time from another endpoint.
     `Policy` decides how the reference is reconciled with the target's own
     measurement. The default, `PolicyInterval`, uses the reference when it
     falls inside the measured range, and otherwise uses the nearest edge of
     the range and records the disagreement.

2. **Schedule.** Workers fire `Spacing` apart. The first one starts `Lead`
   early (3× RTT by default) so that early workers retry *into* the window.
   Firing early is cheap, because a worker just retries; firing late can't be
   recovered. When calibration is uncertain, the schedule therefore widens to
   cover the whole plausible range and leans early.

3. **Fire.** Each worker has its own keep-alive connection, rewarmed shortly
   before its slot. One releaser goroutine sleeps, then busy-waits only for
   the next fire time and releases that worker. Only one goroutine ever
   spins, however many workers there are. Workers retry immediately until
   they get a hit or reach the deadline (`GiveUp` after the latest plausible
   send). Requests still in flight get `InFlightGrace` to finish.

## What you supply

| Field | Purpose | Default |
|---|---|---|
| `Target` | The instant on the server's clock | required |
| `Request` | Builds a fresh request for each attempt | required |
| `Calibration` | Builds calibration requests, for when the real request has side effects | `Request` |
| `ServerTime` / `CalibrationServerTime` | Reads the server time from a response | `DateHeader` |
| `IsHit` | Decides what counts as success | no error, 2xx, and at or after `Target` |
| `Reference` and `Policy` | Optional independent clock, and how it's reconciled | none, `PolicyInterval` |
| `Logger` | Diagnostics; never called between a release and its send | no-op |
| `NewTransport` | TLS, proxy and client-cert settings (kairos tunes it for keep-alive) | clone of `http.DefaultTransport` |
| `StopOnFirstHit` | Cancel the other workers after the first hit | `false` |

For a timestamp in the response body, use `kairos.BodyLayout`:

```go
cfg.CalibrationServerTime = kairos.BodyLayout("2006-01-02 15:04:05.000000", time.UTC, time.Microsecond)
```

Defaults for the tuning knobs: 8 workers, 5ms spacing, 10 calibration rounds,
2s give-up, 150ms rewarm, 2ms spin, 500ms in-flight grace and 5s request
timeout. See the `Config` doc comments for the full list.

## Results, not logs

kairos has no log format of its own. `Result` holds the following:

- every `Attempt` from every worker, including the ones before the target;
- the `Calibration`;
- the `Schedule` (each worker's `FireAt`, the effective spacing and the
  deadline);
- the `Best` hit.

Each attempt records its send and receive times, RTT, status, headers, body,
server time, the estimated server time at which it was processed, whether it
was a hit, and any error. Log or store whatever you need.

## Things to know

- **Duplicates are assumed harmless.** Several workers may succeed. Make sure
  the server rejects duplicates, or that your requests carry idempotency keys.
- **The rewarm sends your real request** about `RewarmBefore` (150ms by
  default) before each worker's slot, and it isn't recorded in `Result`. If
  that's unsafe, use a harmless `Calibration` request and be aware of the
  rewarm.
- **Calibration ignores HTTP status**, because it only needs a time. A 5xx
  from an edge calibrates against that edge's clock.
- **A cached or stale `Date` fails loudly.** Calibration rejects responses
  carrying an `Age` header. It returns `ErrInconsistentSweep` when the sweep's
  samples contradict each other, which typically means a CDN cache or
  several backends with different clocks. On a network with very uneven
  latency it can occasionally reject a valid sweep; retry the calibration if
  that happens.
- **Errors:** `ErrNoHit` (comes with a full `Result`), `ErrTooLate` (the
  target is too close to fire; nothing was sent), `ErrInconsistentSweep`, and
  `ErrCoarseReference` (an `HTTPReference` whose time is too coarse).
- **Precision depends on the source.** Sub-millisecond landing is achievable
  with a precise server stamp or a trusted reference clock. With only the
  `Date` header, results are as tight as the sweep allows, typically a few
  milliseconds.

## License

MIT. See [LICENSE](LICENSE).
