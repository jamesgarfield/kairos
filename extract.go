package kairos

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// TimeExtractor reads the server's clock from a response.
type TimeExtractor func(resp *http.Response, body []byte) (ServerTime, error)

// DateHeader extracts server time from the Date response header (1s precision).
func DateHeader(resp *http.Response, body []byte) (ServerTime, error) {
	v := resp.Header.Get("Date")
	if v == "" {
		return ServerTime{}, errors.New("kairos: response has no Date header")
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return ServerTime{}, fmt.Errorf("kairos: parse Date header: %w", err)
	}
	return ServerTime{Time: t, Precision: time.Second}, nil
}

// BodyLayout returns a TimeExtractor that parses the whitespace-trimmed
// response body as a time in the given layout and location, reporting the
// given precision. A nil loc means time.UTC.
func BodyLayout(layout string, loc *time.Location, precision time.Duration) TimeExtractor {
	if loc == nil {
		loc = time.UTC
	}
	return func(resp *http.Response, body []byte) (ServerTime, error) {
		s := strings.TrimSpace(string(body))
		t, err := time.ParseInLocation(layout, s, loc)
		if err != nil {
			return ServerTime{}, fmt.Errorf("kairos: parse body %q with layout %q: %w", s, layout, err)
		}
		return ServerTime{Time: t, Precision: precision}, nil
	}
}
