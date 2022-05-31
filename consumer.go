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
// threshold.
func WithConsumerLagAlert(d time.Duration) ConsumerOption {
	return func(c *consumer) {
		c.lagAlert = d
	}
}

// WithoutConsumerLag provides an option to disable the consumer lag alert.
func WithoutConsumerLag() ConsumerOption {
	return func(c *consumer) {
		c.lagAlert = -1
	}
}

// WithConsumerLagAlertGauge provides an option to set the consumer lag alert
// gauge. Handy for custom alert metadata as labels.
func WithConsumerLagAlertGauge(g prometheus.Gauge) ConsumerOption {
	return func(c *consumer) {
		c.lagAlertGauge = g
	}
}

// WithConsumerActivityTTL provides an option to set the consumer activity
// metric ttl; ie. if no events is consumed in `tll` duration the consumer
// is considered inactive.
func WithConsumerActivityTTL(ttl time.Duration) ConsumerOption {
	return func(c *consumer) {
		c.activityTTL = ttl
	}
}

// WithoutConsumerActivityTTL provides an option to disable the consumer activity metric ttl.
func WithoutConsumerActivityTTL() ConsumerOption {
	return func(c *consumer) {
		c.activityTTL = -1
	}
}

func labels(name string) prometheus.Labels {
	return prometheus.Labels{consumerLabel: name}
}

// NewConsumer returns a new instrumented consumer of events.
func NewConsumer(name string, fn func(context.Context, fate.Fate, *Event) error,
	opts ...ConsumerOption) Consumer {

	ls := labels(name)

	c := &consumer{
		fn:            fn,
		name:          name,
		lagAlert:      defaultLagAlert,
		activityTTL:   defaultActivityTTL,
		lagAlertGauge: consumerLagAlert.With(ls),
		errorCounter:  consumerErrors.With(ls),
		latencyHist:   consumerLatency.With(ls),
	}

	for _, o := range opts {
		o(c)
	}

	c.activityKey = consumerActivityGauge.Register(ls, c.activityTTL)

	return c
}

func (c *consumer) Name() string {
	return c.name
}

func (c *consumer) Consume(ctx context.Context, ft fate.Fate,
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

	err := c.fn(ctx, ft, event)
	if err != nil && !IsExpected(err) {
		c.errorCounter.Inc()
	}

	latency := time.Since(t0)
	c.latencyHist.Observe(latency.Seconds())

	return err
}

// Reset the consumer, create metrics ready for Consume
func (c *consumer) Reset() error {
	c.lagGauge = consumerLag.With(labels(c.name))
	return nil
}

// Stop the consumer, discard metrics
func (c *consumer) Stop() error {
	consumerLag.Delete(labels(c.name))
	return nil
}
