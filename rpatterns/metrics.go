package rpatterns

import "github.com/prometheus/client_golang/prometheus"

var batchConsumerBufferLength = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "reflex",
	Subsystem: "rpatterns",
	Name:      "batch_consumer_buffer_length",
}, []string{"consumer_name"})

func init() {
	prometheus.MustRegister(
		batchConsumerBufferLength,
	)
}
