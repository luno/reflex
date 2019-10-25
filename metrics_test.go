package reflex

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestActivityGauge(t *testing.T) {
	g := newActivityGauge(prometheus.NewGaugeVec(
		prometheus.GaugeOpts{}, []string{consumerLabel}))

	label1 := prometheus.Labels{consumerLabel: "label1"}
	label2 := prometheus.Labels{consumerLabel: "label2"}
	label3 := prometheus.Labels{consumerLabel: "label3"}

	assertMetric := func(ch chan prometheus.Metric) {
		require.Len(t, ch, 2)
		for i := 0; i < 2; i++ {
			m := <-ch
			dm := new(dto.Metric)
			err := m.Write(dm)
			require.NoError(t, err)

			if dm.Label[0].GetValue() == "label1" {
				require.Equal(t, 0.0, dm.Gauge.GetValue())
			} else {
				require.Equal(t, 1.0, dm.Gauge.GetValue())
			}
		}
	}

	k1 := g.Register(label1, time.Nanosecond) // will always be inactive
	k2 := g.Register(label2, time.Minute)     // will always be active
	k3 := g.Register(label3, -1)              // disabled

	ch := make(chan prometheus.Metric, 5)
	g.Collect(ch)
	assertMetric(ch)

	g.SetActive(k1)
	g.SetActive(k2)
	g.SetActive(k3)

	ch = make(chan prometheus.Metric, 5)
	g.Collect(ch)
	assertMetric(ch)
}
