# kairos: precision-timed HTTP request library

## Intent

A Go library for landing an HTTP request at, or as soon as possible after, a
target instant **on a remote server's clock**. The user has needed this more
than once: for booking windows that open at a set time, for trades or auctions
that open at a set time, and for starting a job on a server exactly when
business opens. It is a generic facility. No single consumer drives the design.

It replaces a one-off `main` program ("autohome") that did this with flags, a
hardcoded timestamp-body format, per-worker log files and `log.Fatal`. The
library keeps that program's timing algorithm. Everything consumer-specific
becomes something the caller supplies.

## Goals

- Land at least one request whose server-side stamp is at or just after
  `Target`. "Just after" means sub-millisecond to a few milliseconds when the
  network allows it.
- Work for any HTTP/HTTPS endpoint. The consumer builds the requests and says
  how to read the server time and what counts as a hit.
- Two ways to use it: a one-call `Run`, and separate `Calibrate` and `Fire`
  steps so a caller can calibrate early, inspect the numbers, and then commit.
- Return all data (every attempt, the calibration, the best hit) to the caller.
  Never exit the process, never log fatally, never write files.

## Non-goals

- Transports other than HTTP/HTTPS (gRPC, websockets, FIX).
- Preventing duplicate hits. **Duplicates are harmless by assumption**: either
  the server rejects them or the consumer's requests carry idempotency keys.
  Multiple workers may succeed, and that is fine.
- Long-running operation. A whole fire phase lasts a few seconds at most.
- A CLI. A small example program is fine, but the product is the package.

## Anti-goals (a failure even if it technically works)

- The library adds jitter between "decide to send" and "bytes on the wire",
  for example by doing disk I/O, allocating heavily, or contending on locks in
  the hot path.
- Many goroutines busy-wait at once and starve each other, or starve the
  network goroutines, on machines with few cores.
- Errors are swallowed. Following the user's standing rule, errors are returned
  to a level that can handle them. Ignoring `fmt.Fprintf`-style write errors
  and logging close errors is acceptable.
- Retries keep going past the configured deadline.

## Constraints

- Module `github.com/jamesgarfield/kairos`, package `kairos`. Go 1.26
  (installed toolchain). Standard library only, unless the builder has a strong
  reason otherwise.
- HTTP/HTTPS only.
- Each worker owns a dedicated keep-alive connection.

## Decided design (from the conversation)

### Consumer-supplied pieces
- **Real request**: a function that builds a fresh `*http.Request` for every
  attempt, so idempotency keys, auth and bodies can be regenerated each time.
- **Calibration request**: configured separately, because it may be a
  different endpoint from the real one (for example, a harmless ping when the
  real request places a trade). If unset, the real request is used.
- **Server-time extraction**: a function of the response and its fully read
  body that returns the server's timestamp **and its precision**. The library
  reads the body itself so that the received-time is exact. Calibration and
  real responses get separate extractors, because a ping endpoint and an
  order endpoint will likely format time differently. A built-in extractor
  reads the `Date` header (1s precision), and it's the default when none is
  given, since essentially every HTTP response carries `Date`.
- **Hit condition**: an interface or closure over an attempt (response
  metadata, body, server time, error). Default: server time >= `Target`.
  Because duplicates are harmless, a consumer may decide that a server
  rejection such as "already done" counts as a hit.
- **Logger**: an interface. A nil value means no-op. Logging must never sit
  in the timing-critical path, so attempts are buffered and logging can happen
  after the run, or asynchronously.

### Retries and stopping
- Immediate retry on failure (no backoff) is intended.
- An explicit **end time / deadline** bounds all retries. Nothing is sent
  after it.
- Stopping uses context cancellation. The caller's `ctx` cancels everything.
  An option (e.g. `StopOnFirstHit`) makes the library cancel the remaining
  workers once any worker gets a hit.

### API shape (the names are guidance, not mandated)
```go
type Config struct {
    Target   time.Time     // on the server's clock
    GiveUp   time.Duration // retries stop this long after Target (local clock, see below)
    Workers  int
    Spacing  time.Duration // offset between consecutive workers' first send
    Lead     time.Duration // 0 = derive from calibrated RTT (old default: 3x RTT)
    CalibrationRounds int

    Request     func(ctx context.Context) (*http.Request, error)
    Calibration func(ctx context.Context) (*http.Request, error) // nil = Request
    ServerTime            TimeExtractor  // real responses; nil = Date header
    CalibrationServerTime TimeExtractor  // nil = ServerTime
    Reference             ReferenceClock // optional; see Calibration model
    IsHit       func(a Attempt) bool // nil = server time >= Target
    Logger      Logger               // nil = no-op
    StopOnFirstHit bool
    // plus a hook for building the transport (see below)
}

func Run(ctx context.Context, cfg Config) (Result, error) // Calibrate + Fire
```
Both `Run` and the split `Calibrate`/`Fire` exist. Because calibration warms
the per-worker connections that `Fire` then uses, the split form needs an
object that owns those connections across the two calls, e.g.
`s, _ := kairos.New(cfg); cal, _ := s.Calibrate(ctx); res, _ := s.Fire(ctx, cal)`
plus `s.Close()`. **(My call; the user agreed to "both" but did not specify
this shape.)**

`Result` contains all attempts from all workers, the calibration numbers used,
the computed schedule, and the best hit (earliest server time that satisfied
the hit condition). It must be enough for a consumer to write logs equivalent
to the old per-worker TSV files.

### Transport
The library owns connection management: one client and transport per worker,
one idle keep-alive connection each, compression disabled, and a re-warm
request shortly before each worker's fire time in case the server dropped the
idle connection. Consumers need to control TLS settings, proxies and client
certs, so there is a hook for building the base transport, which the library
then tunes for keep-alive. **(My proposal; the user approved it as part of the
overall shape.)**

### Busy-wait starvation fix
The user asked for this to be fixed. The approach: **one scheduler goroutine**
does the precise timing. It sleeps coarsely, then spins only for the *next*
worker's fire time, and then releases that worker. So at most one goroutine
spins, however many workers there are. Retries are not timed precisely and
never spin. Releasing a worker costs a wake-up delay (normally microseconds).
Calibration should measure it and correct for it, or the builder should show
it is negligible. **(My proposal; the user accepted the direction.)**

## Calibration model (decided with the user)

The target server may only expose the HTTP `Date` header (1s resolution), so
calibration supports three sources of clock information. The consumer
configures one or more of them:

1. **Precise server stamp**: the target's responses carry a millisecond-or-
   finer time in the body or a header. This uses the original median method
   (below).
2. **`Date`-header sweep**: fire calibration requests at local send times
   stepped across a second boundary, and find the send time at which the
   server's `Date` value ticks over. That gives `stampDelay` (offset plus
   one-way latency) to roughly the step size. The step size and the sweep
   strategy (linear, or a coarse pass followed by a fine pass) are left to
   the builder. A sweep needs at least one second boundary, so it takes
   about 1–2s or more. Account for that in `CalibrateBefore`.
3. **Reference clock provider**: a pluggable interface that supplies precise
   "true" time, assuming the target server is NTP-synced to true time. The
   user confirmed that the original's default `http://localhost:8080/ping`
   was exactly this: a reference clock, not the target. Built-in providers:
   - the local system clock, assumed synced;
   - an HTTP endpoint that returns precise time, with a configurable URL and
     body format, which covers the old localhost `/ping`.

   Consumers may implement their own providers. Reading an HTTP reference
   also measures the client-side cost of building and sending a request,
   which the user suspects was part of the original's math. Keep that
   component in the latency model.

**Latency** is measured separately from offset. Every calibration request
against the target yields an RTT. When no precise server stamp exists,
one-way time is estimated as RTT/2 *(my call)*, and the total predicted delay
includes local request build/send overhead.

**Combining sources (decided with the user: "interval + spread")**

What the sources are good for: the reference clock is precise but rests on an
assumption, since it says nothing about the target itself. The `Date` sweep
measures the target directly, but it is coarse, and it measures whichever box
sets `Date`. Behind a CDN or load balancer that may not be the app server that
enforces the target. Errors also cost different amounts. Firing early is cheap
because workers retry into the window. Firing late can't be recovered.

Default policy:
1. The sweep yields an **interval** `[lo, hi]` for `stampDelay`, widened by the
   jitter measured during the sweep so that noise does not cause false
   disagreements.
2. If the reference estimate falls inside the interval, use it as the point
   estimate. That gives precision plus validation.
3. If it falls outside, clamp it to the nearest edge of the interval, and
   record the disagreement in `Calibration`.
4. **Spread**: when the remaining uncertainty is wider than the configured
   worker fan would cover, widen the fan. The schedule spans from the earliest
   plausible send instant to the latest, instead of betting on one point. When
   uncertain, lean early. With a fixed worker count this coarsens coverage, so
   `Calibration`/`Result` report the effective spacing used.

The combining policy is a **configurable option**. Alternatives the user
considered and rejected as the default, which should be available as options:
- **Reference wins, sweep only warns**: for consumers who know `Date` comes
  from an edge whose clock differs from the app's.
- **Sweep wins**: for consumers who distrust the reference.

Weighted averaging was rejected outright. Server skew is a fixed error, not
random noise, so averaging just splits the error. Failing on disagreement was
also rejected: no human is present at calibration time.

When only one source is configured, that source is used alone. With a sweep
only, the interval is the uncertainty, and the spread rule still applies.
`Calibration` records which sources were used, their individual estimates,
the interval, the final estimate and its uncertainty, and any disagreement.

**Hit ranking**: every hit has at least a second-resolution stamp (`Date`).
Hits are ranked by server stamp, and within the stamp's resolution by local
receive time adjusted by calibration. The default `IsHit` compares against
`Target` using the best estimate of the server time at which the request was
processed, not just the raw coarse stamp. *(My call.)* The sub-millisecond
goal applies when a precise stamp or a trusted reference clock is available.
With `Date` only, the goal is "as tight as the sweep resolution allows".

## Timing algorithm (kept from the original program)

1. **Calibrate** (precise-stamp source): for `CalibrationRounds` rounds, every worker's client sends
   the calibration request *at the same time*, so the server sees the same
   load as the real burst. For each response, record
   `stampDelay = serverTime - localSend` (clock offset plus one-way latency
   in a single number) and `rtt = received - localSend`. Use the medians.
2. **Schedule**: `ideal = Target - stampDelay`. Worker 0's first send is at
   `ideal - Lead`, and worker *i* is at `first + i*Spacing`. The early lead is
   deliberate: requests after an idle pause are often slower than calibration
   predicts, so early workers retry *into* the window instead of overshooting
   it. If `first` is already in the past, return an error.
3. **Fire**: each worker re-warms its connection about 150ms before its slot,
   waits for release, sends, and retries immediately until it gets a hit, the
   deadline passes, or the context is cancelled.
4. **Report**: return everything. The best hit is the earliest qualifying
   server time.

Original capture details to keep:
- `sent` is taken right before `client.Do`.
- `received` is taken after the body has been fully read, or right after `Do`
  returns an error.
- Non-200 responses and body-parse failures count as errors.
- In calibration, the original aborted on the first error. The library should
  return that error, wrapped with the round number.

The original per-worker TSV columns were: attempt number, sent (local),
received (local), rtt, server time, delta vs target, and error. The header
also recorded the worker id, the target and the worker's local fire time.
`Attempt` and `Result` must carry at least these.

### Deadline and lifetime (my calls)
- The deadline is on the **local clock**: `ideal + GiveUp`, where `GiveUp` is
  a duration after the target. This matches the original and avoids
  converting a server-clock deadline through `stampDelay`. The config takes
  `GiveUp time.Duration`, not a `Deadline time.Time`.
- No new sends after the deadline. Requests already in flight get a short
  grace period to finish and be recorded (a server may have acted on them),
  then they're cancelled.
- `Run` can be called well before the target. It sleeps until
  `Target - CalibrateBefore` (default about 30s), then calibrates and fires,
  so calibration is never stale. A `Calibration` carries the time it was
  taken. `Fire` accepts any calibration, but `Result` reports its age.

Known issues in the original that the library fixes: the logs silently dropped
pre-target responses, `log.Fatal`/`os.Exit` were used, and every worker
busy-waited at once.

## Left to the builder

- Exact names, option style (struct vs functional options), and the `Logger`
  interface's methods.
- Defaults for `Workers`, `Spacing`, `CalibrationRounds`, the re-warm offset,
  and the spin window. The original used 8, 5ms, 10, 150ms and 2ms.
- How the `Lead` default is derived. The original used 3x RTT.
- How validation errors are typed or wrapped.
- The test strategy. A local `httptest` server with a controllable clock is
  the obvious basis. Timing assertions should tolerate CI noise.
- Whether to include an example program reproducing the old CLI.
