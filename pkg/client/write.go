package client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/prompb"
)

const (
	maxErrMsgLen = 256
)

type WriteClientConfig struct {
	// Cortex URL.
	URL url.URL

	// The tenant ID to use to push metrics to Cortex.
	UserID string

	// Number of series to generate per write request.
	SeriesCount    int
	TotalRate      float64
	RandomIncrease bool
	ExtraLabels    int

	WriteInterval    time.Duration
	WriteTimeout     time.Duration
	WriteConcurrency int
	WriteBatchSize   int
}

type WriteClient struct {
	client    *http.Client
	cfg       WriteClientConfig
	writeGate *gate.Gate
	logger    log.Logger
}

func NewWriteClient(cfg WriteClientConfig, logger log.Logger) *WriteClient {
	var rt http.RoundTripper = &http.Transport{}
	rt = &clientRoundTripper{userID: cfg.UserID, rt: rt}

	c := &WriteClient{
		client:    &http.Client{Transport: rt},
		cfg:       cfg,
		writeGate: gate.New(cfg.WriteConcurrency),
		logger:    logger,
	}

	go c.run()

	return c
}

func (c *WriteClient) run() {
	counters := make([]float64, c.cfg.SeriesCount)
	ts := alignTimestampToInterval(time.Now(), c.cfg.WriteInterval)

	c.writeSeries(ts, counters)

	ticker := time.NewTicker(c.cfg.WriteInterval)

	prevTs := ts
	for {
		select {
		case <-ticker.C:
			ts := alignTimestampToInterval(time.Now(), c.cfg.WriteInterval)
			if c.cfg.RandomIncrease {
				updateCountersRandomIncrease(counters, ts.Sub(prevTs), c.cfg.TotalRate)
			} else {
				updateCountersSameIncrease(counters, ts.Sub(prevTs), c.cfg.TotalRate)
			}
			prevTs = ts

			c.writeSeries(ts, counters)
		}
	}
}

func (c *WriteClient) writeSeries(ts time.Time, counters []float64) {
	series := make([]*prompb.TimeSeries, 0, len(counters))

	for ix := 0; ix < len(counters); ix++ {
		lbls := []*prompb.Label{{
			Name:  "__name__",
			Value: "load_generator_counter",
		}, {
			Name:  "idx",
			Value: strconv.Itoa(ix + 1),
		}}

		for j := 0; j < c.cfg.ExtraLabels; j++ {
			lbls = append(lbls, &prompb.Label{
				Name:  fmt.Sprintf("lbl_%d", j),
				Value: fmt.Sprintf("%d", j),
			})
		}

		// Make sure labels are sorted.
		sort.Slice(lbls, func(i, j int) bool {
			return lbls[i].Name < lbls[j].Name
		})

		series = append(series, &prompb.TimeSeries{
			Labels: lbls,
			Samples: []prompb.Sample{{
				Value:     counters[ix],
				Timestamp: ts.UnixMilli(),
			}},
		})
	}

	// Honor the batch size.
	wg := sync.WaitGroup{}

	for o := 0; o < len(series); o += c.cfg.WriteBatchSize {
		wg.Add(1)

		go func(o int) {
			defer wg.Done()

			// Honow the max concurrency
			ctx := context.Background()
			c.writeGate.Start(ctx)
			defer c.writeGate.Done()

			end := o + c.cfg.WriteBatchSize
			if end > len(series) {
				end = len(series)
			}

			req := &prompb.WriteRequest{
				Timeseries: series[o:end],
			}

			err := c.send(ctx, req)
			if err != nil {
				level.Error(c.logger).Log("msg", "failed to write series", "err", err)
			}
		}(o)
	}

	wg.Wait()
}

func (c *WriteClient) send(ctx context.Context, req *prompb.WriteRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.cfg.URL.String(), bytes.NewReader(compressed))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "cortex-load-generator")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq = httpReq.WithContext(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.WriteInterval)
	defer cancel()

	httpResp, err := c.client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return err
	}
	return err
}

func alignTimestampToInterval(ts time.Time, interval time.Duration) time.Time {
	return time.Unix(0, (ts.UnixNano()/int64(interval))*int64(interval))
}

func generateSineWaveSeries(t time.Time, seriesCount int) []*prompb.TimeSeries {
	out := make([]*prompb.TimeSeries, 0, seriesCount)
	value := generateSineWaveValue(t)

	for i := 1; i <= seriesCount; i++ {
		out = append(out, &prompb.TimeSeries{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "cortex_load_generator_sine_wave",
			}, {
				Name:  "wave",
				Value: strconv.Itoa(i),
			}},
			Samples: []prompb.Sample{{
				Value:     value,
				Timestamp: t.UnixMilli(),
			}},
		})
	}

	return out
}

func generateSineWaveValue(t time.Time) float64 {
	// With a 15-second scrape interval this gives a ten-minute period
	period := float64(40 * (15 * time.Second))
	radians := float64(t.UnixNano()) / period * 2 * math.Pi
	return math.Sin(radians)
}

func updateCountersSameIncrease(counters []float64, elapsed time.Duration, totalRate float64) {
	avgRate := totalRate / float64(len(counters))
	avgIncrease := avgRate * elapsed.Seconds()

	for ix := 0; ix < len(counters); ix++ {
		counters[ix] += avgIncrease
	}
}

func updateCountersRandomIncrease(counters []float64, elapsed time.Duration, totalRate float64) {
	avgRate := totalRate / float64(len(counters))
	avgIncrease := avgRate * elapsed.Seconds()

	for ix := 0; ix < len(counters); ix++ {
		inc := rand.NormFloat64()*(avgIncrease/3) + avgIncrease
		// round to 3 places
		inc = math.Round(inc*1000) / 1000

		counters[ix] += inc
	}
}
