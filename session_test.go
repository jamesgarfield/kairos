package kairos

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

func reqFor(url string) RequestFunc {
	return func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	}
}

func newTestSession(t *testing.T, o fakeOpts, ext TimeExtractor) (*Session, *http.Request) {
	t.Helper()
	srv := newFakeServer(t, o)
	s, err := New(Config{Target: time.Now().Add(time.Hour), Request: reqFor(srv.URL), ServerTime: ext, Workers: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(s.Close)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	return s, req
}

func TestNewInvalid(t *testing.T) {
	s, err := New(Config{Target: time.Now().Add(time.Hour)})
	if err == nil || s != nil {
		t.Fatalf("New() = %v, %v; want nil session and error", s, err)
	}
}

func TestNewTransportHook(t *testing.T) {
	s, err := New(Config{
		Target:  time.Now().Add(time.Hour),
		Request: reqFor("http://127.0.0.1:1"),
		Workers: 3,
		NewTransport: func() *http.Transport {
			return &http.Transport{TLSHandshakeTimeout: 7 * time.Second}
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer s.Close()
	if len(s.clients) != 3 {
		t.Fatalf("len(clients) = %d, want 3", len(s.clients))
	}
	seen := map[*http.Transport]bool{}
	for i, c := range s.clients {
		tr := c.Transport.(*http.Transport)
		if seen[tr] {
			t.Errorf("worker %d shares a transport", i)
		}
		seen[tr] = true
		if tr.TLSHandshakeTimeout != 7*time.Second {
			t.Errorf("worker %d TLSHandshakeTimeout = %v", i, tr.TLSHandshakeTimeout)
		}
		if tr.MaxIdleConnsPerHost != 1 || !tr.DisableCompression || tr.IdleConnTimeout != 5*time.Minute {
			t.Errorf("worker %d transport not tuned: %+v", i, tr)
		}
		if c.Timeout != 0 {
			t.Errorf("worker %d client Timeout = %v, want 0", i, c.Timeout)
		}
	}
}

func TestNewNilTransport(t *testing.T) {
	s, err := New(Config{
		Target:       time.Now().Add(time.Hour),
		Request:      reqFor("http://127.0.0.1:1"),
		NewTransport: func() *http.Transport { return nil },
	})
	if err == nil || s != nil {
		t.Fatalf("New() = %v, %v; want error", s, err)
	}
}

func TestSend(t *testing.T) {
	s, req := newTestSession(t, fakeOpts{Precise: true}, preciseExtract)
	a := s.send(s.clients[0], req, preciseExtract)
	if a.Err != nil {
		t.Fatalf("Err = %v", a.Err)
	}
	if a.StatusCode != 200 {
		t.Errorf("StatusCode = %d", a.StatusCode)
	}
	if a.Server.Precision != time.Microsecond {
		t.Errorf("Precision = %v", a.Server.Precision)
	}
	if d := a.Server.Time.Sub(a.Sent).Abs(); d >= 50*time.Millisecond {
		t.Errorf("|Server.Time - Sent| = %v", d)
	}
	if a.Received.Before(a.Sent) {
		t.Errorf("Received %v before Sent %v", a.Received, a.Sent)
	}
}

func TestSendStatusNotError(t *testing.T) {
	s, req := newTestSession(t, fakeOpts{Status: func(time.Time) int { return 403 }}, DateHeader)
	a := s.send(s.clients[0], req, DateHeader)
	if a.Err != nil {
		t.Fatalf("Err = %v", a.Err)
	}
	if a.StatusCode != 403 {
		t.Errorf("StatusCode = %d", a.StatusCode)
	}
	if a.Server.Time.IsZero() {
		t.Error("Server not set from Date")
	}
}

func TestSendNoDate(t *testing.T) {
	s, req := newTestSession(t, fakeOpts{NoDate: true}, DateHeader)
	a := s.send(s.clients[0], req, DateHeader)
	if a.Err == nil || !strings.Contains(a.Err.Error(), "server time") {
		t.Fatalf("Err = %v, want server time error", a.Err)
	}
}

func TestSendDoError(t *testing.T) {
	s, req := newTestSession(t, fakeOpts{}, DateHeader)
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	a := s.send(s.clients[0], req.WithContext(ctx), DateHeader)
	if a.Err == nil || a.Received.IsZero() {
		t.Fatalf("Err = %v, Received = %v; want both set", a.Err, a.Received)
	}
}
