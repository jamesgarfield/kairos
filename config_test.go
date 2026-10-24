package kairos

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func testRequest(ctx context.Context) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid/", nil)
}

func TestWithDefaults(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	c := Config{Target: t0, Request: testRequest}.withDefaults()

	if c.Workers != 8 || c.Spacing != 5*time.Millisecond || c.CalibrationRounds != 10 ||
		c.RewarmBefore != 150*time.Millisecond || c.Spin != 2*time.Millisecond ||
		c.GiveUp != 2*time.Second || c.CalibrateBefore != 30*time.Second ||
		c.InFlightGrace != 500*time.Millisecond || c.RequestTimeout != 5*time.Second ||
		c.SweepStep != 2*time.Millisecond || c.MaxBodyBytes != 1<<20 || c.Lead != 0 ||
		c.CalibrationAttempts != 3 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if c.ServerTime == nil || c.CalibrationServerTime == nil || c.Calibration == nil ||
		c.Logger == nil || c.NewTransport == nil {
		t.Fatalf("expected non-nil default funcs: %+v", c)
	}
	if c.IsHit != nil {
		t.Fatal("IsHit must stay nil")
	}
	if c.NewTransport() == nil {
		t.Fatal("default NewTransport returned nil")
	}
	c.Logger.Logf("no-op %d", 1)
}

func TestWithDefaultsPreservesExplicit(t *testing.T) {
	calledCal := false
	cal := func(ctx context.Context) (*http.Request, error) { calledCal = true; return nil, nil }
	c := Config{
		Target: time.Unix(1, 0), Request: testRequest, Calibration: cal,
		Workers: 3, Spacing: time.Millisecond, CalibrationRounds: 4,
		RewarmBefore: time.Second, Spin: time.Microsecond, GiveUp: time.Minute,
		CalibrateBefore: time.Hour, InFlightGrace: time.Second, RequestTimeout: time.Minute,
		SweepStep: time.Second, MaxBodyBytes: 7, Lead: 9 * time.Millisecond,
		CalibrationAttempts: 5,
	}.withDefaults()
	if c.Workers != 3 || c.Spacing != time.Millisecond || c.CalibrationRounds != 4 ||
		c.RewarmBefore != time.Second || c.Spin != time.Microsecond || c.GiveUp != time.Minute ||
		c.CalibrateBefore != time.Hour || c.InFlightGrace != time.Second ||
		c.RequestTimeout != time.Minute || c.SweepStep != time.Second ||
		c.MaxBodyBytes != 7 || c.Lead != 9*time.Millisecond || c.CalibrationAttempts != 5 {
		t.Fatalf("explicit values not preserved: %+v", c)
	}
	if _, _ = c.Calibration(context.Background()); !calledCal {
		t.Fatal("explicit Calibration replaced")
	}
}

func TestValidate(t *testing.T) {
	t0 := time.Unix(1700000000, 0)
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"valid", Config{Target: t0, Request: testRequest}, ""},
		{"zero target", Config{Request: testRequest}, "Target"},
		{"nil request", Config{Target: t0}, "Request"},
		{"negative workers", Config{Target: t0, Request: testRequest, Workers: -1}, "Workers"},
		{"negative spacing", Config{Target: t0, Request: testRequest, Spacing: -1}, "Spacing"},
		{"negative lead", Config{Target: t0, Request: testRequest, Lead: -1}, "Lead"},
		{"negative giveup", Config{Target: t0, Request: testRequest, GiveUp: -1}, "GiveUp"},
		{"negative spin", Config{Target: t0, Request: testRequest, Spin: -1}, "Spin"},
		{"negative rewarm", Config{Target: t0, Request: testRequest, RewarmBefore: -1}, "RewarmBefore"},
		{"negative calibrate before", Config{Target: t0, Request: testRequest, CalibrateBefore: -1}, "CalibrateBefore"},
		{"negative grace", Config{Target: t0, Request: testRequest, InFlightGrace: -1}, "InFlightGrace"},
		{"negative request timeout", Config{Target: t0, Request: testRequest, RequestTimeout: -1}, "RequestTimeout"},
		{"negative sweep step", Config{Target: t0, Request: testRequest, SweepStep: -1}, "SweepStep"},
		{"negative calibration attempts", Config{Target: t0, Request: testRequest, CalibrationAttempts: -1}, "CalibrationAttempts"},
		{"negative max body", Config{Target: t0, Request: testRequest, MaxBodyBytes: -1}, "MaxBodyBytes"},
		{"negative rounds", Config{Target: t0, Request: testRequest, CalibrationRounds: -1}, "CalibrationRounds"},
		{"unknown policy", Config{Target: t0, Request: testRequest, Policy: Policy(99)}, "Policy"},
		{"policy target wins", Config{Target: t0, Request: testRequest, Policy: PolicyTargetWins}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.withDefaults().validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestAttemptRTT(t *testing.T) {
	s := time.Unix(10, 0)
	a := Attempt{Sent: s, Received: s.Add(42 * time.Millisecond)}
	if a.RTT() != 42*time.Millisecond {
		t.Fatalf("RTT = %v", a.RTT())
	}
}

func TestDateHeaderSanity(t *testing.T) {
	resp := &http.Response{Header: http.Header{"Date": {"Tue, 15 Nov 1994 08:12:31 GMT"}}}
	st, err := DateHeader(resp, nil)
	if err != nil || st.Precision != time.Second || st.Time.Year() != 1994 {
		t.Fatalf("got %+v, %v", st, err)
	}
	if _, err := DateHeader(&http.Response{Header: http.Header{}}, nil); err == nil || !strings.Contains(err.Error(), "Date") {
		t.Fatalf("missing header error = %v", err)
	}
}

type wrappedRT struct{}

func (wrappedRT) RoundTrip(*http.Request) (*http.Response, error) { return nil, nil }

func TestDefaultNewTransportWrappedDefault(t *testing.T) {
	orig := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = orig })
	http.DefaultTransport = wrappedRT{}

	c := Config{Target: time.Unix(1, 0), Request: testRequest}.withDefaults()
	if c.NewTransport() == nil {
		t.Fatal("NewTransport returned nil")
	}
}
