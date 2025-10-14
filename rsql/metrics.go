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

	eventsBlockingGapGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "blocking_gap",
		Help:      "Whether the event loader is blocked on a gap",
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

	eventsDeserializationDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "deserialization_duration_seconds",
		Help:      "Time spent deserializing events from database rows to reflex.Event objects",
		Buckets:   []float64{0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10},
	}, []string{"table"})

	eventsLoadingDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "loading_duration_seconds",
		Help:      "Time spent loading and deserializing batches of events from database",
		Buckets:   []float64{0.001, 0.01, 0.1, 1, 10, 60},
	}, []string{"table"})

	eventsDeserializedCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "events_table",
		Name:      "events_deserialized_total",
		Help:      "Total number of events deserialized from database rows",
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
	prometheus.MustRegister(eventsGapListenGauge)
	prometheus.MustRegister(eventsBlockingGapGauge)
	prometheus.MustRegister(eventsDeserializationDuration)
	prometheus.MustRegister(eventsLoadingDuration)
	prometheus.MustRegister(eventsDeserializedCounter)
}
