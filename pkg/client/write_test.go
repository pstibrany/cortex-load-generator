package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlignTimestampToInterval(t *testing.T) {
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(30, 0), 10*time.Second))
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(31, 0), 10*time.Second))
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(39, 0), 10*time.Second))
	assert.Equal(t, time.Unix(40, 0), alignTimestampToInterval(time.Unix(40, 0), 10*time.Second))
}

func TestUpdateSeries(t *testing.T) {
	const series = 500
	const totalRate = 10000
	const seconds = 7

	counters := make([]float64, series)

	updateCounters(counters, seconds*time.Second, totalRate)

	avg := 0.0
	for _, v := range counters {
		avg += v
	}

	require.InDelta(t, avg/float64(len(counters)), seconds*(totalRate/series), 1)
}
