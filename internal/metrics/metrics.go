package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Metrics struct {
	reg *Registry

	HTTPRequests      *CounterVec
	HTTPDuration      *HistogramVec
	Transfers         *CounterVec
	WebhooksReceived  *CounterVec
	WebhooksRejected  *CounterVec
	WebhooksProcessed *CounterVec
	WebhooksPending   *Gauge
}

func New() *Metrics {
	r := &Registry{}
	m := &Metrics{
		reg: r,
		HTTPRequests: r.NewCounterVec("http_requests_total",
			"HTTP requests by method, route pattern and status code.", "method", "route", "status"),
		HTTPDuration: r.NewHistogramVec("http_request_duration_seconds",
			"HTTP request latency by method and route pattern.",
			[]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}, "method", "route"),
		Transfers: r.NewCounterVec("ledger_transfers_total",
			"Transfer requests by result (created, replayed or error code).", "result"),
		WebhooksReceived: r.NewCounterVec("webhook_events_received_total",
			"Webhook events accepted, split by whether they were duplicates.", "provider", "duplicate"),
		WebhooksRejected: r.NewCounterVec("webhook_events_rejected_total",
			"Webhook requests rejected before being stored.", "provider", "reason"),
		WebhooksProcessed: r.NewCounterVec("webhook_events_processed_total",
			"Webhook processing attempts by result (processed, retry, failed).", "result"),
		WebhooksPending: r.NewGauge("webhook_events_pending",
			"Webhook events waiting to be processed (backlog)."),
	}
	r.NewGaugeFunc("go_goroutines", "Number of goroutines.", func() float64 {
		return float64(runtime.NumGoroutine())
	})
	r.NewGaugeFunc("go_memstats_heap_alloc_bytes", "Bytes of allocated heap objects.", func() float64 {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return float64(ms.HeapAlloc)
	})
	return m
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.reg.Write(w)
	})
}

type collector interface {
	write(w io.Writer)
}

type Registry struct {
	mu         sync.Mutex
	collectors []collector
}

func (r *Registry) add(c collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectors = append(r.collectors, c)
}

func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	collectors := append([]collector(nil), r.collectors...)
	r.mu.Unlock()
	for _, c := range collectors {
		c.write(w)
	}
}

func header(w io.Writer, name, help, kind string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func labelString(names, values []string, extra ...string) string {
	if len(names) == 0 && len(extra) == 0 {
		return ""
	}
	parts := make([]string, 0, len(names)+len(extra)/2)
	for i, n := range names {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, n, escape(values[i])))
	}
	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, extra[i], escape(extra[i+1])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escape(s string) string { return labelEscaper.Replace(s) }

func key(values []string) string { return strings.Join(values, "\xff") }

func checkLabels(name string, want int, got []string) {
	if len(got) != want {
		panic(fmt.Sprintf("metric %s: expected %d label values, got %d", name, want, len(got)))
	}
}

type CounterVec struct {
	name, help string
	labels     []string
	mu         sync.Mutex
	series     map[string]*counterSeries
}

type counterSeries struct {
	values []string
	value  float64
}

func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, labels: labels, series: map[string]*counterSeries{}}
	r.add(c)
	return c
}

func (c *CounterVec) Inc(labelValues ...string) { c.Add(1, labelValues...) }

func (c *CounterVec) Add(v float64, labelValues ...string) {
	checkLabels(c.name, len(c.labels), labelValues)
	if v < 0 {
		panic("counter cannot decrease")
	}
	k := key(labelValues)
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.series[k]
	if !ok {
		s = &counterSeries{values: append([]string(nil), labelValues...)}
		c.series[k] = s
	}
	s.value += v
}

func (c *CounterVec) Value(labelValues ...string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.series[key(labelValues)]; ok {
		return s.value
	}
	return 0
}

func (c *CounterVec) write(w io.Writer) {
	header(w, c.name, c.help, "counter")
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range sortedKeys(c.series) {
		s := c.series[k]
		fmt.Fprintf(w, "%s%s %s\n", c.name, labelString(c.labels, s.values), formatFloat(s.value))
	}
}

type HistogramVec struct {
	name, help string
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*histogramSeries
}

type histogramSeries struct {
	values []string
	counts []uint64
	sum    float64
	count  uint64
}

func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	h := &HistogramVec{name: name, help: help, labels: labels, buckets: b, series: map[string]*histogramSeries{}}
	r.add(h)
	return h
}

func (h *HistogramVec) Observe(v float64, labelValues ...string) {
	checkLabels(h.name, len(h.labels), labelValues)
	k := key(labelValues)
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.series[k]
	if !ok {
		s = &histogramSeries{values: append([]string(nil), labelValues...), counts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	for i, upper := range h.buckets {
		if v <= upper {
			s.counts[i]++
			break
		}
	}
	s.sum += v
	s.count++
}

func (h *HistogramVec) write(w io.Writer) {
	header(w, h.name, h.help, "histogram")
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range sortedKeys(h.series) {
		s := h.series[k]
		var cumulative uint64
		for i, upper := range h.buckets {
			cumulative += s.counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelString(h.labels, s.values, "le", formatFloat(upper)), cumulative)
		}
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelString(h.labels, s.values, "le", "+Inf"), s.count)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, labelString(h.labels, s.values), formatFloat(s.sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, labelString(h.labels, s.values), s.count)
	}
}

type Gauge struct {
	name, help string
	mu         sync.Mutex
	value      float64
}

func (r *Registry) NewGauge(name, help string) *Gauge {
	g := &Gauge{name: name, help: help}
	r.add(g)
	return g
}

func (g *Gauge) Set(v float64) {
	g.mu.Lock()
	g.value = v
	g.mu.Unlock()
}

func (g *Gauge) write(w io.Writer) {
	header(w, g.name, g.help, "gauge")
	g.mu.Lock()
	defer g.mu.Unlock()
	fmt.Fprintf(w, "%s %s\n", g.name, formatFloat(g.value))
}

type gaugeFunc struct {
	name, help string
	fn         func() float64
}

func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.add(&gaugeFunc{name: name, help: help, fn: fn})
}

func (g *gaugeFunc) write(w io.Writer) {
	header(w, g.name, g.help, "gauge")
	fmt.Fprintf(w, "%s %s\n", g.name, formatFloat(g.fn()))
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
