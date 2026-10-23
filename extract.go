package kairos

import (
	"errors"
	"fmt"
	"net/http"
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
