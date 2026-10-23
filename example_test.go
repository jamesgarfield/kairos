package kairos_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jamesgarfield/kairos"
)

// ExampleRun fires at a placeholder URL and logs the Result on the consumer
// side. It has no Output comment, so it is compiled but never run.
func ExampleRun() {
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
		ServerTime: kairos.DateHeader,
		Reference:  kairos.SystemClock{},
	}

	res, err := kairos.Run(context.Background(), cfg)
	if err != nil {
		fmt.Println("run:", err)
	}
	if res.Best != nil {
		fmt.Printf("best: worker %d attempt %d status %d, %v after target\n",
			res.Best.Worker, res.Best.N, res.Best.StatusCode, res.Best.Estimated.Sub(res.Target))
	}
	for w, attempts := range res.Attempts {
		for _, a := range attempts {
			fmt.Printf("worker=%d n=%d fire=%s sent=%s rtt=%v status=%d delta=%v hit=%t err=%v\n",
				a.Worker, a.N, res.Schedule.FireAt[w].Format(time.RFC3339Nano),
				a.Sent.Format(time.RFC3339Nano), a.RTT(), a.StatusCode,
				a.Estimated.Sub(res.Target), a.Hit, a.Err)
		}
	}
}
