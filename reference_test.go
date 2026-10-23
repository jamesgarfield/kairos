package kairos

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSystemClock(t *testing.T) {
	off, err := SystemClock{}.Offset(context.Background())
	if err != nil || off != 0 {
		t.Fatalf("got %v, %v", off, err)
	}
}

func TestHTTPReference(t *testing.T) {
	srv := newFakeServer(t, fakeOpts{Skew: 250 * ms, Precise: true})
	ref := HTTPReference{URL: srv.URL, Extract: preciseExtract}
	off, err := ref.Offset(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if off < 245*ms || off > 255*ms {
		t.Errorf("offset = %v, want within [245ms, 255ms]", off)
	}
}

func TestHTTPReferenceCoarse(t *testing.T) {
	srv := newFakeServer(t, fakeOpts{})
	ref := HTTPReference{URL: srv.URL, Extract: DateHeader}
	_, err := ref.Offset(context.Background())
	if !errors.Is(err, ErrCoarseReference) {
		t.Fatalf("err = %v, want ErrCoarseReference", err)
	}
}

func TestHTTPReferenceStatusError(t *testing.T) {
	srv := newFakeServer(t, fakeOpts{Precise: true, Status: func(_ time.Time) int { return 500 }})
	ref := HTTPReference{URL: srv.URL, Extract: preciseExtract, Samples: 2}
	if _, err := ref.Offset(context.Background()); err == nil {
		t.Fatal("want error for 500")
	}
}

func TestHTTPReferenceTimeoutPerSample(t *testing.T) {
	old := referenceTimeout
	referenceTimeout = 100 * ms
	t.Cleanup(func() { referenceTimeout = old })
	// each sample (~40ms) is under the timeout; the total (~320ms) exceeds it
	srv := newFakeServer(t, fakeOpts{Precise: true, Delay: 40 * ms})
	ref := HTTPReference{URL: srv.URL, Extract: preciseExtract, Samples: 8}
	if _, err := ref.Offset(context.Background()); err != nil {
		t.Fatalf("per-sample timeout should not span samples: %v", err)
	}
}
