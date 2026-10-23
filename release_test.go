package kairos

import (
	"context"
	"sort"
	"testing"
	"time"
)

func TestRunReleases(t *testing.T) {
	wake := measureWakeLatency(20)
	start := time.Now()
	const n = 8
	rs := make([]release, n)
	recv := make([]time.Time, n)
	done := make(chan int, n)
	for i := range rs {
		rs[i] = release{at: start.Add(50*ms + time.Duration(i)*5*ms), ch: make(chan struct{})}
		go func(i int) {
			<-rs[i].ch
			recv[i] = time.Now()
			done <- i
		}(i)
	}
	runReleases(context.Background(), rs, 2*ms, wake)
	for range rs {
		<-done
	}
	var lat []time.Duration
	for i, r := range rs {
		d := recv[i].Sub(r.at)
		if d < -wake-200*time.Microsecond {
			t.Errorf("release %d received %v before at; wake=%v", i, -d, wake)
		}
		lat = append(lat, d)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	if med := (lat[n/2-1] + lat[n/2]) / 2; med >= ms {
		t.Errorf("median recv-at = %v, want < 1ms", med)
	}
}

func TestRunReleasesCancel(t *testing.T) {
	now := time.Now()
	rs := []release{
		{at: now.Add(20 * ms), ch: make(chan struct{})},
		{at: now.Add(500 * ms), ch: make(chan struct{})},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-rs[0].ch
		cancel()
	}()
	var returned time.Time
	runReleases(ctx, rs, 2*ms, 0)
	returned = time.Now()
	if returned.After(now.Add(20*ms + 20*ms)) {
		t.Errorf("runReleases returned %v after first release, want < 20ms", returned.Sub(now.Add(20*ms)))
	}
	select {
	case <-rs[1].ch:
		t.Error("second channel was closed after cancel")
	default:
	}
}

func TestSleepUntilCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*ms)
	defer cancel()
	if err := sleepUntil(ctx, time.Now().Add(time.Second), 0); err == nil {
		t.Error("want ctx error")
	}
}

func TestReleaseSpin(t *testing.T) {
	tests := []struct {
		name      string
		spin, gap time.Duration
		first     bool
		want      int
	}{
		{"first uses full spin", 2 * ms, 0, true, 2},
		{"wide gap keeps spin", 2 * ms, 10 * ms, false, 2},
		{"narrow gap halves", 2 * ms, 2 * ms, false, 1},
		{"zero gap no spin", 2 * ms, 0, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := releaseSpin(tt.spin, tt.gap, tt.first); got != time.Duration(tt.want)*ms {
				t.Errorf("releaseSpin = %v, want %v", got, time.Duration(tt.want)*ms)
			}
		})
	}
}
