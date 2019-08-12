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
	name        ConsumerName
	lagAlert    time.Duration
	activityTTL time.Duration
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
func NewConsumer(name ConsumerName, fn func(context.Context, fate.Fate, *Event) error,
	opts ...ConsumerOption) Consumer {
	c := &consumer{
		fn:          fn,
		name:        name,
		lagAlert:    defaultLagAlert,
		activityTTL: defaultActivityTTL,
	}

	for _, o := range opts {
		o(c)
	}

	consumerActivityGauge.Register(c.makePromLabels(), c.activityTTL)

	return c
}

func (c *consumer) Name() ConsumerName {
	return c.name
}

func (c *consumer) makePromLabels() prometheus.Labels {
	return prometheus.Labels{consumerLabel: c.name.String()}
}

func (c *consumer) Consume(ctx context.Context, fate fate.Fate,
	event *Event) error {
	labels := c.makePromLabels()
	t0 := time.Now()

	consumerActivityGauge.SetActive(labels)

	lag := t0.Sub(event.Timestamp)
	consumerLag.With(labels).Set(lag.Seconds())

	alert := 0.0
	if lag > c.lagAlert && c.lagAlert > 0 {
		alert = 1
	}
	consumerLagAlert.With(labels).Set(alert)

	err := c.fn(ctx, fate, event)
	if err != nil {
		consumerErrors.With(labels).Inc()
	}

	latency := time.Since(t0)
	consumerLatency.With(labels).Observe(latency.Seconds())

	return err
}
