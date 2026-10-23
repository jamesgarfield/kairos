package kairos

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeOpts configures newFakeServer.
type fakeOpts struct {
	Skew    time.Duration                 // server clock = time.Now()+Skew
	Precise bool                          // body in "2006-01-02 15:04:05.000000" UTC; else body empty
	NoDate  bool                          // omit Date header
	Status  func(serverNow time.Time) int // nil = 200
	Delay   time.Duration                 // sleep before stamping
	Count   *atomic.Int64                 // if non-nil, incremented per request
}

var preciseExtract = BodyLayout("2006-01-02 15:04:05.000000", time.UTC, time.Microsecond)

// newFakeServer starts a server whose clock is skewed from the local clock.
func newFakeServer(t *testing.T, o fakeOpts) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o.Count != nil {
			o.Count.Add(1)
		}
		if o.Delay > 0 {
			time.Sleep(o.Delay)
		}
		serverNow := time.Now().Add(o.Skew)
		if o.NoDate {
			w.Header()["Date"] = nil
		} else {
			w.Header().Set("Date", serverNow.UTC().Format(http.TimeFormat))
		}
		status := http.StatusOK
		if o.Status != nil {
			status = o.Status(serverNow)
		}
		w.WriteHeader(status)
		if o.Precise {
			if _, err := w.Write([]byte(serverNow.UTC().Format("2006-01-02 15:04:05.000000"))); err != nil {
				t.Logf("fake server write: %v", err)
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}
