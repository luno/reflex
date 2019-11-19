package rsql

import "github.com/prometheus/client_golang/prometheus"

var (
	cursorSetCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "cursors_table",
		Name:      "set_total",
		Help:      "Total number of set cursor queries performed per table",
	}, []string{"table"})

	eventsPollCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "poll_total",
		Help:      "Total number of get next events queries performed per table",
	}, []string{"table"})

	eventsGapDetectCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "gap_detected_total",
		Help:      "Total number of gaps detected while streaming events",
	}, []string{"table"})

	eventsGapFilledCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "gap_filled_total",
		Help:      "Total number of gaps filled",
	}, []string{"table"})

	eventsGapListenGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "gap_listening",
		Help:      "Wether or not any gap listeners have been registered.",
	}, []string{"table"})

	rcacheHitsCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "rcache_hits_total",
		Help:      "Total number of read-through cache hits per table",
	}, []string{"table"})

	rcacheMissCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "rcache_misses_total",
		Help:      "Total number of read-through cache misses per table",
	}, []string{"table"})
)

func makeCursorSetCounter(table string) func() {
	return cursorSetCounter.WithLabelValues(table).Inc
}

func init() {
	prometheus.MustRegister(cursorSetCounter)
	prometheus.MustRegister(eventsPollCounter)
	prometheus.MustRegister(rcacheHitsCounter)
	prometheus.MustRegister(rcacheMissCounter)
	prometheus.MustRegister(eventsGapDetectCounter)
	prometheus.MustRegister(eventsGapFilledCounter)
}
