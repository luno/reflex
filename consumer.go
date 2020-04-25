package reflex

import (
	"context"
	"time"

	"github.com/luno/fate"
	"github.com/prometheus/client_golang/prometheus"
)

const defaultLagAlert = 30 * time.Minute
const defaultActivityTTL = 24 * time.Hour

type consumer struct {
	fn          func(context.Context, fate.Fate, *Event) error
	name        string
	lagAlert    time.Duration
	activityTTL time.Duration

	lagGauge      prometheus.Gauge
	lagAlertGauge prometheus.Gauge
	errorCounter  prometheus.Counter
	latencyHist   prometheus.Observer
	activityKey   string
}

type ConsumerOption func(*consumer)

// WithConsumerLagAlert provides an option to set the consumer lag alert
// threshold. Setting it to -1 disables the alert.
func WithConsumerLagAlert(d time.Duration) ConsumerOption {
	return func(c *consumer) {
		c.lagAlert = d
	}
}

// WithConsumerActivityTTL provides an option to set the consumer activity
// metric ttl; ie. if no events is consumed in `tll` duration the consumer
// is considered inactive. Setting it to -1 disables the activity metric.
func WithConsumerActivityTTL(ttl time.Duration) ConsumerOption {
	return func(c *consumer) {
		c.activityTTL = ttl
	}
}

// NewConsumer returns a new instrumented consumer of events.
func NewConsumer(name string, fn func(context.Context, fate.Fate, *Event) error,
	opts ...ConsumerOption) Consumer {

	labels := prometheus.Labels{consumerLabel: name}

	c := &consumer{
		fn:            fn,
		name:          name,
		lagAlert:      defaultLagAlert,
		activityTTL:   defaultActivityTTL,
		lagGauge:      consumerLag.With(labels),
		lagAlertGauge: consumerLagAlert.With(labels),
		errorCounter:  consumerErrors.With(labels),
		latencyHist:   consumerLatency.With(labels),
	}

	for _, o := range opts {
		o(c)
	}

	c.activityKey = consumerActivityGauge.Register(labels, c.activityTTL)

	return c
}

func (c *consumer) Name() string {
	return c.name
}

func (c *consumer) Consume(ctx context.Context, fate fate.Fate,
	event *Event) error {
	t0 := time.Now()

	consumerActivityGauge.SetActive(c.activityKey)

	lag := t0.Sub(event.Timestamp)
	c.lagGauge.Set(lag.Seconds())

	alert := 0.0
	if lag > c.lagAlert && c.lagAlert > 0 {
		alert = 1
	}
	c.lagAlertGauge.Set(alert)

	err := c.fn(ctx, fate, event)
	if err != nil {
		c.errorCounter.Inc()
	}

	latency := time.Since(t0)
	c.latencyHist.Observe(latency.Seconds())

	return err
}
