package datadogfirehosenozzle

import (
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-firehose-nozzle/metrics"
	noaaerrors "github.com/cloudfoundry/noaa/errors"
	"github.com/gorilla/websocket"
)

/**
  The nozzle cannot be constantly posting. It must post at semi regular intervals.
  However, when a post needs to be retried, the nozzle should not be blocked from posting further
  Therefore, the nozzle will have workers for posting.
**/

func (d *DatadogFirehoseNozzle) startPostingToDatadog() error {
	// Start the (multiple) workers which will process envelopes
	for i := 0; i < d.config.NumWorkers; i++ {
		go d.postToDatadog()
	}

	ticker := time.NewTicker(time.Duration(d.config.FlushDurationSeconds) * time.Second)
	for {
		select {
		case <-ticker.C:
			d.postNow <- true
		case err := <-d.errs:
			d.handleError(err)
			return err
		case <-d.stopper:
			return nil
		}
	}
}

func (d *DatadogFirehoseNozzle) postToDatadog() {
	for {
		select {
		case <-d.postNow:
			d.postToDatadog()
		case <-d.stopper:
			return
		case <-d.workersStopper:
			return
		}
	}
}

func (d *DatadogFirehoseNozzle) PostMetrics() {
	d.mapLock.Lock()
	// deep copy the metrics map to pass to PostMetrics so that we can unlock d.metricsMap while posting
	metricsMap := make(metrics.MetricsMap)
	for k, v := range d.metricsMap {
		metricsMap[k] = v
	}
	totalMessagesReceived := d.totalMessagesReceived
	d.mapLock.Unlock()

	// Add internal metrics
	k, v := d.client.MakeInternalMetric("totalMessagesReceived", totalMessagesReceived)
	metricsMap[k] = v
	k, v = d.client.MakeInternalMetric("totalMetricsSent", d.totalMetricsSent)
	metricsMap[k] = v
	k, v = d.client.MakeInternalMetric("slowConsumerAlert", atomic.LoadUint64(&d.slowConsumerAlert))
	metricsMap[k] = v

	err := d.client.PostMetrics(metricsMap)
	if err != nil {
		d.log.Errorf("Error posting metrics: %s\n\n", err)
		return
	}

	d.totalMetricsSent += uint64(len(metricsMap))
	d.mapLock.Lock()
	d.metricsMap = make(metrics.MetricsMap)
	d.mapLock.Unlock()
	d.ResetSlowConsumerError()
}

func (d *DatadogFirehoseNozzle) handleError(err error) {
	if retryErr, ok := err.(noaaerrors.RetryError); ok {
		err = retryErr.Err
	}

	switch closeErr := err.(type) {
	case *websocket.CloseError:
		switch closeErr.Code {
		case websocket.CloseNormalClosure:
		// no op
		case websocket.ClosePolicyViolation:
			d.log.Errorf("Error while reading from the firehose: %v", err)
			d.log.Errorf("Disconnected because nozzle couldn't keep up. Please try scaling up the nozzle.")
			d.AlertSlowConsumerError()
		default:
			d.log.Errorf("Error while reading from the firehose: %v", err)
		}
	default:
		d.log.Errorf("Error while reading from the firehose: %v", err)

	}

	d.log.Infof("Closing connection with traffic controller due to %v", err)
	d.consumer.Close()
	d.PostMetrics()
}
