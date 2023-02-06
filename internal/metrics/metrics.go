package metrics

import (
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const consumerLabel = "consumer_name"

// Labels returns the prometheus labels for the consumer
func Labels(name string) prometheus.Labels {
	return prometheus.Labels{consumerLabel: name}
}

var (
	// ConsumerLag is a metric for how far behind the consumer is
	// based on the last consumed event
	ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "reflex",
		Subsystem: "consumer",
		Name:      "lag_seconds",
		Help:      "Lag between now and the current event timestamp in seconds",
	}, []string{consumerLabel})

	// ConsumerAge is a metric for how old events coming that are being
	// processed are.
	ConsumerAge = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "reflex",
		Subsystem: "consumer",
		Name:      "event_age_seconds",
		Help:      "The age of events that are being processed by the consumer",
		Buckets: []float64{
			0.1, .25, .5, 1, 2.5, 5,
			10, 25, 50, 100, 250, 500, // ~10 minutes
			1_000, 2_500, 5_000, 10_000, 25_000, 50_000, // ~13 hours
			100_000, // > 1 day
		},
	}, []string{consumerLabel})

	// ConsumerLagAlert is whether or not the consumer is too far behind
	ConsumerLagAlert = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "reflex",
		Subsystem: "consumer",
		Name:      "lag_alert",
		Help:      "Whether or not the consumer lag crosses its alert threshold",
	}, []string{consumerLabel})

	// ConsumerActivityGauge is whether or not the consumer has processed an event
	ConsumerActivityGauge = newActivityGauge(
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "reflex",
			Subsystem: "consumer",
			Name:      "active",
			Help: "Whether or not the consumer was active (consumed an event) " +
				"in the activity ttl period",
		}, []string{consumerLabel}))

	// ConsumerLatency is how long the consumer is taking to process an event
	ConsumerLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "reflex",
		Subsystem: "consumer",
		Name:      "latency_seconds",
		Help:      "Event loop latency in seconds",
		Buckets:   []float64{0.001, 0.01, 0.1, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0},
	}, []string{consumerLabel})

	// ConsumerErrors is the number of errors from processing events
	ConsumerErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "reflex",
		Subsystem: "consumer",
		Name:      "error_count",
		Help:      "Number of errors processing events",
	}, []string{consumerLabel})
)

func init() {
	prometheus.MustRegister(
		ConsumerLag,
		ConsumerAge,
		ConsumerLagAlert,
		ConsumerActivityGauge,
		ConsumerLatency,
		ConsumerErrors,
	)
}

func newActivityGauge(g *prometheus.GaugeVec) *activityGauge {
	return &activityGauge{
		gv:     g,
		states: make(map[string]state),
	}
}

// activityGauge provides a prometheus GaugeVec which indicates whether or not
// a consumer was recently active (consumed an event).
type activityGauge struct {
	gv     *prometheus.GaugeVec
	mu     sync.Mutex
	states map[string]state
}

type state struct {
	labels prometheus.Labels
	tick   time.Time
	ttl    time.Duration
}

// Register registers the consumer labels with its ttl and ticks it as active and returns a consumer key.
func (g *activityGauge) Register(labels prometheus.Labels, ttl time.Duration) string {
	key := labelsToKey(labels)

	g.mu.Lock()
	defer g.mu.Unlock()

	g.states[key] = state{
		labels: labels,
		ttl:    ttl,
		tick:   time.Now(),
	}
	return key
}

// SetActive ticks the consumer key as active.
func (g *activityGauge) SetActive(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	s := g.states[key]
	s.tick = time.Now()
	g.states[key] = s
}

func (g *activityGauge) Describe(ch chan<- *prometheus.Desc) {
	g.gv.Describe(ch)
}

// Collect sets and collects the internal GaugeVec activity values for all registered
// consumers labels.
func (g *activityGauge) Collect(ch chan<- prometheus.Metric) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, s := range g.states {
		if s.ttl < 0 {
			continue
		}
		v := 0.0
		if time.Since(s.tick) < s.ttl {
			v = 1
		}
		g.gv.With(s.labels).Set(v)
	}
	g.gv.Collect(ch)
}

func labelsToKey(labels prometheus.Labels) string {
	s := strings.Builder{}
	for k, v := range labels {
		s.WriteString(k)
		s.Write([]byte{255})
		s.WriteString(v)
		s.Write([]byte{255})
	}
	return s.String()
}
