package metrics

import (
	"bytes"
	"strings"
	"testing"
)

func TestCounterAndHistogramExposition(t *testing.T) {
	r := &Registry{}
	c := r.NewCounterVec("requests_total", "Requests.", "route", "status")
	h := r.NewHistogramVec("latency_seconds", "Latency.", []float64{0.1, 1}, "route")

	c.Inc("/v1/transfers", "201")
	c.Inc("/v1/transfers", "201")
	c.Inc("/v1/transfers", "422")
	h.Observe(0.05, "/v1/transfers")
	h.Observe(0.5, "/v1/transfers")
	h.Observe(3, "/v1/transfers")

	var buf bytes.Buffer
	r.Write(&buf)
	out := buf.String()

	for _, want := range []string{
		"# TYPE requests_total counter",
		`requests_total{route="/v1/transfers",status="201"} 2`,
		`requests_total{route="/v1/transfers",status="422"} 1`,
		"# TYPE latency_seconds histogram",
		`latency_seconds_bucket{route="/v1/transfers",le="0.1"} 1`,
		`latency_seconds_bucket{route="/v1/transfers",le="1"} 2`,
		`latency_seconds_bucket{route="/v1/transfers",le="+Inf"} 3`,
		`latency_seconds_sum{route="/v1/transfers"} 3.55`,
		`latency_seconds_count{route="/v1/transfers"} 3`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing line %q in output:\n%s", want, out)
		}
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	r := &Registry{}
	c := r.NewCounterVec("x_total", "X.", "v")
	c.Inc(`a"b\c`)

	var buf bytes.Buffer
	r.Write(&buf)
	if want := `x_total{v="a\"b\\c"} 1`; !strings.Contains(buf.String(), want) {
		t.Fatalf("got %q, want line %q", buf.String(), want)
	}
}

func TestWrongNumberOfLabelsPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	r := &Registry{}
	r.NewCounterVec("y_total", "Y.", "a", "b").Inc("only-one")
}
