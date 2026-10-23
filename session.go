package kairos

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Session owns one warm keep-alive client per worker.
type Session struct {
	cfg     Config // defaulted
	clients []*http.Client
}

// New validates cfg and builds one client, with its own transport, per worker.
func New(cfg Config) (*Session, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &Session{cfg: cfg, clients: make([]*http.Client, 0, cfg.Workers)}
	for i := 0; i < cfg.Workers; i++ {
		tr := cfg.NewTransport()
		if tr == nil {
			return nil, fmt.Errorf("kairos: invalid config: NewTransport returned nil for worker %d", i)
		}
		tr.MaxIdleConnsPerHost = 1
		tr.IdleConnTimeout = 5 * time.Minute
		tr.DisableCompression = true
		// Timeout stays 0: contexts bound each request.
		s.clients = append(s.clients, &http.Client{Transport: tr})
	}
	return s, nil
}

// Close closes idle connections on every worker transport.
func (s *Session) Close() {
	for _, c := range s.clients {
		if tr, ok := c.Transport.(*http.Transport); ok {
			tr.CloseIdleConnections()
		}
	}
}

// send performs req on c and records the outcome. It does no logging and
// takes Sent immediately before c.Do. Status codes are never errors.
func (s *Session) send(c *http.Client, req *http.Request, ext TimeExtractor) Attempt {
	var a Attempt
	a.Sent = time.Now()
	resp, err := c.Do(req)
	if err != nil {
		a.Received = time.Now()
		a.Err = err
		return a
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, s.cfg.MaxBodyBytes))
	a.Received = time.Now()
	closeErr := resp.Body.Close()
	a.StatusCode = resp.StatusCode
	a.Header = resp.Header
	a.Body = body
	switch {
	case readErr != nil:
		a.Err = fmt.Errorf("read body: %w", readErr)
	case closeErr != nil:
		a.Err = fmt.Errorf("close body: %w", closeErr)
	}
	st, err := ext(resp, body)
	if err != nil {
		a.Err = errors.Join(a.Err, fmt.Errorf("server time: %w", err))
		return a
	}
	a.Server = st
	return a
}
