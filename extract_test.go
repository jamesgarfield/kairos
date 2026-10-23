package kairos

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestDateHeader(t *testing.T) {
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("Date", "Fri, 23 Oct 2026 17:00:05 GMT")
	st, err := DateHeader(resp, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 10, 23, 17, 0, 5, 0, time.UTC); !st.Time.Equal(want) {
		t.Errorf("Time = %v, want %v", st.Time, want)
	}
	if st.Precision != time.Second {
		t.Errorf("Precision = %v, want 1s", st.Precision)
	}

	missing := &http.Response{Header: http.Header{}}
	if _, err := DateHeader(missing, nil); err == nil || !strings.Contains(err.Error(), "Date") {
		t.Errorf("missing header err = %v, want error containing Date", err)
	}

	garbage := &http.Response{Header: http.Header{}}
	garbage.Header.Set("Date", "not a date")
	if _, err := DateHeader(garbage, nil); err == nil {
		t.Error("garbage header: want error")
	}
}

func TestBodyLayout(t *testing.T) {
	nyc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	ex := BodyLayout("2006-01-02 15:04:05.000000", nyc, time.Microsecond)
	st, err := ex(&http.Response{}, []byte("2026-10-23 13:00:00.123456\n"))
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 10, 23, 13, 0, 0, 123456000, nyc); !st.Time.Equal(want) {
		t.Errorf("Time = %v, want %v", st.Time, want)
	}
	if st.Precision != time.Microsecond {
		t.Errorf("Precision = %v, want 1µs", st.Precision)
	}

	_, err = ex(&http.Response{}, []byte("garbage body"))
	if err == nil || !strings.Contains(err.Error(), `"garbage body"`) {
		t.Errorf("bad body err = %v, want error quoting body", err)
	}
}

func TestBodyLayoutNilLocation(t *testing.T) {
	ex := BodyLayout("2006-01-02 15:04:05", nil, time.Second)
	st, err := ex(&http.Response{}, []byte("2026-10-23 13:00:00"))
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 10, 23, 13, 0, 0, 0, time.UTC); !st.Time.Equal(want) || st.Time.Location() != time.UTC {
		t.Errorf("Time = %v, want %v in UTC", st.Time, want)
	}
}
