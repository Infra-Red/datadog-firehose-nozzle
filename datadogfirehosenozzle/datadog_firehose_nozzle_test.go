package datadogfirehosenozzle

import (
	"bytes"
	"os"

	"code.cloudfoundry.org/localip"
	"github.com/DataDog/datadog-firehose-nozzle/metrics"
	. "github.com/DataDog/datadog-firehose-nozzle/testhelpers"
	"github.com/gogo/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/DataDog/datadog-firehose-nozzle/datadogclient"
	"github.com/DataDog/datadog-firehose-nozzle/nozzleconfig"
	"github.com/DataDog/datadog-firehose-nozzle/uaatokenfetcher"
	"github.com/cloudfoundry/gosteno"
	noaaerrors "github.com/cloudfoundry/noaa/errors"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gorilla/websocket"
)

var _ = Describe("Datadog Firehose Nozzle", func() {
	var (
		fakeUAA        *FakeUAA
		fakeFirehose   *FakeFirehose
		fakeDatadogAPI *FakeDatadogAPI
		config         *nozzleconfig.NozzleConfig
		nozzle         *DatadogFirehoseNozzle
		log            *gosteno.Logger
		logContent     *bytes.Buffer
		fakeBuffer     *FakeBufferSink
	)

	BeforeEach(func() {
		content := make([]byte, 1024)
		logContent = bytes.NewBuffer(content)
		fakeBuffer = NewFakeBufferSink(logContent)
		c := &gosteno.Config{
			Sinks: []gosteno.Sink{
				fakeBuffer,
			},
		}
		gosteno.Init(c)
		log = gosteno.NewLogger("test")
	})

	Context("regular usage", func() {
		BeforeEach(func() {
			fakeUAA = NewFakeUAA("bearer", "123456789")
			fakeToken := fakeUAA.AuthToken()
			fakeFirehose = NewFakeFirehose(fakeToken)
			fakeDatadogAPI = NewFakeDatadogAPI()
			fakeUAA.Start()
			fakeFirehose.Start()
			fakeDatadogAPI.Start()

			config = &nozzleconfig.NozzleConfig{
				UAAURL:               fakeUAA.URL(),
				FlushDurationSeconds: 2,
				FlushMaxBytes:        10240,
				DataDogURL:           fakeDatadogAPI.URL(),
				DataDogAPIKey:        "1234567890",
				TrafficControllerURL: strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1),
				DisableAccessControl: false,
				WorkerTimeoutSeconds: 10,
				MetricPrefix:         "datadog.nozzle.",
				Deployment:           "nozzle-deployment",
				AppMetrics:           false,
				NumWorkers:           1,
			}
			os.Remove("firehose_nozzle.db")
		})

		JustBeforeEach(func() {
			tokenFetcher := uaatokenfetcher.New(fakeUAA.URL(), "un", "pwd", true, log)
			nozzle = NewDatadogFirehoseNozzle(config, tokenFetcher, log)
			go nozzle.Start()
			time.Sleep(time.Second)
		})

		AfterEach(func() {
			nozzle.Stop()
			fakeUAA.Close()
			fakeFirehose.Close()
			fakeDatadogAPI.Close()
			os.Remove("firehose_nozzle.db")
		})

		It("receives data from the firehose", func() {
			for i := 0; i < 10; i++ {
				envelope := events.Envelope{
					Origin:    proto.String("origin"),
					Timestamp: proto.Int64(1000000000),
					EventType: events.Envelope_ValueMetric.Enum(),
					ValueMetric: &events.ValueMetric{
						Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
						Value: proto.Float64(float64(i)),
						Unit:  proto.String("gauge"),
					},
					Deployment: proto.String("deployment-name"),
					Job:        proto.String("doppler"),
				}
				fakeFirehose.AddEvent(envelope)
			}

			var contents []byte
			Eventually(fakeDatadogAPI.ReceivedContents, 15*time.Second, time.Second).Should(Receive(&contents))

			var payload datadogclient.Payload
			err := json.Unmarshal(Decompress(contents), &payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(payload.Series).To(HaveLen(23)) // +3 is because of the internal metrics
		}, 2)

		It("gets a valid authentication token", func() {
			Eventually(fakeFirehose.Requested).Should(BeTrue())
			Consistently(fakeFirehose.LastAuthorization).Should(Equal("bearer 123456789"))
		})

		It("adds internal metrics and generates aggregate messages when idle", func() {
			for i := 0; i < 10; i++ {
				envelope := events.Envelope{
					Origin:    proto.String("origin"),
					Timestamp: proto.Int64(1000000000),
					EventType: events.Envelope_ValueMetric.Enum(),
					ValueMetric: &events.ValueMetric{
						Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
						Value: proto.Float64(float64(i)),
						Unit:  proto.String("gauge"),
					},
					Deployment: proto.String("deployment-name"),
					Job:        proto.String("doppler"),
				}
				fakeFirehose.AddEvent(envelope)
			}

			var contents []byte
			Eventually(fakeDatadogAPI.ReceivedContents, 15*time.Second, time.Second).Should(Receive(&contents))

			var payload datadogclient.Payload
			err := json.Unmarshal(Decompress(contents), &payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(payload.Series).To(HaveLen(23))

			validateMetrics(payload, 10, 0)

			// Wait a bit more for the new tick. We should receive only internal metrics
			Eventually(fakeDatadogAPI.ReceivedContents, 15*time.Second, time.Second).Should(Receive(&contents))
			err = json.Unmarshal(Decompress(contents), &payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(payload.Series).To(HaveLen(3)) // only internal metrics

			validateMetrics(payload, 10, 23)
		}, 3)

		It("reports a slow-consumer error when the server disconnects abnormally", func() {
			for i := 0; i < 10; i++ {
				envelope := events.Envelope{
					Origin:    proto.String("origin"),
					Timestamp: proto.Int64(1000000000),
					EventType: events.Envelope_ValueMetric.Enum(),
					ValueMetric: &events.ValueMetric{
						Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
						Value: proto.Float64(float64(i)),
						Unit:  proto.String("gauge"),
					},
					Deployment: proto.String("deployment-name"),
					Job:        proto.String("doppler"),
				}
				fakeFirehose.AddEvent(envelope)
			}

			fakeFirehose.SetCloseMessage(websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Client did not respond to ping before keep-alive timeout expired."))
			fakeFirehose.CloseWebSocket()

			var contents []byte
			Eventually(fakeDatadogAPI.ReceivedContents).Should(Receive(&contents))

			var payload datadogclient.Payload
			err := json.Unmarshal(Decompress(contents), &payload)
			Expect(err).ToNot(HaveOccurred())

			slowConsumerMetric := findSlowConsumerMetric(payload)
			Expect(slowConsumerMetric).NotTo(BeNil())
			Expect(slowConsumerMetric.Points).To(HaveLen(1))
			Expect(slowConsumerMetric.Points[0].Value).To(BeEquivalentTo(1))

			logOutput := fakeBuffer.GetContent()
			Expect(logOutput).To(ContainSubstring("Error while reading from the firehose"))
			Expect(logOutput).To(ContainSubstring("Client did not respond to ping before keep-alive timeout expired."))
			Expect(logOutput).To(ContainSubstring("Disconnected because nozzle couldn't keep up."))
		}, 2)

		It("tries to reestablish a websocket connection when it is closed", func() {
			for i := 0; i < 10; i++ {
				envelope := events.Envelope{
					Origin:    proto.String("origin"),
					Timestamp: proto.Int64(1000000000),
					EventType: events.Envelope_ValueMetric.Enum(),
					ValueMetric: &events.ValueMetric{
						Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
						Value: proto.Float64(float64(i)),
						Unit:  proto.String("gauge"),
					},
					Deployment: proto.String("deployment-name"),
					Job:        proto.String("doppler"),
				}
				fakeFirehose.AddEvent(envelope)
			}

			fakeFirehose.SetCloseMessage(websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Client did not respond to ping before keep-alive timeout expired."))
			fakeFirehose.CloseWebSocket()

			var contents []byte
			Eventually(fakeDatadogAPI.ReceivedContents).Should(Receive(&contents))

			var payload datadogclient.Payload
			err := json.Unmarshal(Decompress(contents), &payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(payload.Series).To(HaveLen(23))

			logOutput := fakeBuffer.GetContent()
			Expect(logOutput).To(ContainSubstring("Error while reading from the firehose"))
			Expect(logOutput).To(ContainSubstring("Client did not respond to ping before keep-alive timeout expired."))
			Expect(logOutput).To(ContainSubstring("Disconnected because nozzle couldn't keep up."))
			Eventually(fakeBuffer.GetContent).Should(ContainSubstring("Websocket connection lost, reestablishing connection..."))

			// Nozzle should have reconnected.
			// Wait a bit more for the new tick. We should receive only internal metrics
			Eventually(fakeDatadogAPI.ReceivedContents, 15*time.Second, time.Second).Should(Receive(&contents))
			err = json.Unmarshal(Decompress(contents), &payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(payload.Series).To(HaveLen(3)) // only internal metrics
			validateMetrics(payload, 10, 23)

			// If we fail too many times quickly, the nozzle should crash
			// We should hit the condition for too many retry
			fakeFirehose.CloseWebSocket()
			time.Sleep(time.Second)
			fakeFirehose.CloseWebSocket()
			time.Sleep(time.Second)
			fakeFirehose.CloseWebSocket()
			Eventually(fakeBuffer.GetContent, 5*time.Second).Should(ContainSubstring("Too many retries, shutting down..."))
			// Restart the nozzle since it crashed, because it will be stopped by the test teardown
			go nozzle.Start()
			time.Sleep(time.Second)
		}, 2)

		It("retries websocket connection on certain types of error", func() {
			Expect(shouldReconnect(noaaerrors.RetryError{})).To(BeTrue())
			Expect(shouldReconnect(noaaerrors.NonRetryError{})).To(BeFalse())
			Expect(shouldReconnect(nil)).To(BeFalse())
			Expect(shouldReconnect(fmt.Errorf(""))).To(BeFalse())
		})

		It("doesn't report a slow-consumer error when closed for other reasons", func() {
			fakeFirehose.SetCloseMessage(websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "Weird things happened."))
			fakeFirehose.CloseWebSocket()

			var contents []byte
			Eventually(fakeDatadogAPI.ReceivedContents).Should(Receive(&contents))

			var payload datadogclient.Payload
			err := json.Unmarshal(Decompress(contents), &payload)
			Expect(err).ToNot(HaveOccurred())

			slowConsumerMetric := findSlowConsumerMetric(payload)
			Expect(slowConsumerMetric).NotTo(BeNil())
			Expect(slowConsumerMetric.Type).To(Equal("gauge"))
			Expect(slowConsumerMetric.Points).To(HaveLen(1))
			Expect(slowConsumerMetric.Points[0].Value).To(BeEquivalentTo(0))

			logOutput := fakeBuffer.GetContent()
			Expect(logOutput).To(ContainSubstring("Error while reading from the firehose"))
			Expect(logOutput).NotTo(ContainSubstring("Client did not respond to ping before keep-alive timeout expired."))
			Expect(logOutput).NotTo(ContainSubstring("Disconnected because nozzle couldn't keep up."))
		}, 2)

		Context("receives a truncatingbuffer.droppedmessage value metric", func() {
			It("reports a slow-consumer error", func() {
				slowConsumerError := events.Envelope{
					Origin:    proto.String("doppler"),
					Timestamp: proto.Int64(1000000000),
					EventType: events.Envelope_CounterEvent.Enum(),
					CounterEvent: &events.CounterEvent{
						Name:  proto.String("TruncatingBuffer.DroppedMessages"),
						Delta: proto.Uint64(1),
						Total: proto.Uint64(1),
					},
					Deployment: proto.String("deployment-name"),
					Job:        proto.String("doppler"),
				}
				fakeFirehose.AddEvent(slowConsumerError)

				var contents []byte
				Eventually(fakeDatadogAPI.ReceivedContents, 15*time.Second, time.Second).Should(Receive(&contents))

				var payload datadogclient.Payload
				err := json.Unmarshal(Decompress(contents), &payload)
				Expect(err).ToNot(HaveOccurred())

				slowConsumerMetric := findSlowConsumerMetric(payload)
				Expect(slowConsumerMetric).NotTo(BeNil())
				Expect(slowConsumerMetric.Type).To(Equal("gauge"))
				Expect(slowConsumerMetric.Points).To(HaveLen(1))
				Expect(slowConsumerMetric.Points[0].Value).To(BeEquivalentTo(1))
				Expect(fakeBuffer.GetContent()).To(ContainSubstring("We've intercepted an upstream message which indicates that the nozzle or the TrafficController is not keeping up. Please try scaling up the nozzle."))
			})
		})

		Context("reports a slow-consumer error", func() {
			It("unsets the error after sending it", func() {
				envelope := events.Envelope{
					Origin:    proto.String("doppler"),
					Timestamp: proto.Int64(1000000000),
					EventType: events.Envelope_CounterEvent.Enum(),
					CounterEvent: &events.CounterEvent{
						Name:  proto.String("TruncatingBuffer.DroppedMessages"),
						Delta: proto.Uint64(1),
						Total: proto.Uint64(1),
					},
					ValueMetric: &events.ValueMetric{
						Name:  proto.String(fmt.Sprintf("metricName-%d", 1)),
						Value: proto.Float64(float64(1)),
						Unit:  proto.String("gauge"),
					},
					Deployment: proto.String("deployment-name"),
					Job:        proto.String("doppler"),
				}
				fakeFirehose.AddEvent(envelope)

				var contents []byte
				Eventually(fakeDatadogAPI.ReceivedContents, 15*time.Second, time.Second).Should(Receive(&contents))

				var payload datadogclient.Payload
				err := json.Unmarshal(Decompress(contents), &payload)
				Expect(err).ToNot(HaveOccurred())

				slowConsumerMetric := findSlowConsumerMetric(payload)
				Expect(slowConsumerMetric).NotTo(BeNil())
				Expect(slowConsumerMetric.Type).To(Equal("gauge"))
				Expect(slowConsumerMetric.Points).To(HaveLen(1))
				Expect(slowConsumerMetric.Points[0].Value).To(BeEquivalentTo(1))
				Expect(fakeBuffer.GetContent()).To(ContainSubstring("We've intercepted an upstream message which indicates that the nozzle or the TrafficController is not keeping up. Please try scaling up the nozzle."))

				Eventually(fakeDatadogAPI.ReceivedContents, 15*time.Second, time.Second).Should(Receive(&contents))
				err = json.Unmarshal(Decompress(contents), &payload)
				Expect(err).ToNot(HaveOccurred())

				slowConsumerMetric = findSlowConsumerMetric(payload)
				Expect(slowConsumerMetric).NotTo(BeNil())
				Expect(slowConsumerMetric.Type).To(Equal("gauge"))
				Expect(slowConsumerMetric.Points).To(HaveLen(1))
				Expect(slowConsumerMetric.Points[0].Value).To(BeEquivalentTo(0))
			}, 2)
		})

		Context("with DeploymentFilter provided", func() {
			BeforeEach(func() {
				config.DeploymentFilter = "good-deployment-name"
			})

			It("includes messages that match deployment filter", func() {
				goodEnvelope := events.Envelope{
					Origin:     proto.String("origin"),
					Timestamp:  proto.Int64(1000000000),
					Deployment: proto.String("good-deployment-name"),
				}
				fakeFirehose.AddEvent(goodEnvelope)
				Eventually(fakeDatadogAPI.ReceivedContents, 15*time.Second, time.Second).Should(Receive())
			})

			It("filters out messages from other deployments", func() {
				badEnvelope := events.Envelope{
					Origin:     proto.String("origin"),
					Timestamp:  proto.Int64(1000000000),
					Deployment: proto.String("bad-deployment-name"),
				}
				fakeFirehose.AddEvent(badEnvelope)

				rxContents := filterOutNozzleMetrics(config.Deployment, fakeDatadogAPI.ReceivedContents)
				Consistently(rxContents, 5*time.Second, time.Second).ShouldNot(Receive())
			})
		})
	})

	Context("when the DisableAccessControl is set to true", func() {
		var tokenFetcher *FakeTokenFetcher

		BeforeEach(func() {
			fakeUAA = NewFakeUAA("", "")
			fakeToken := fakeUAA.AuthToken()
			fakeFirehose = NewFakeFirehose(fakeToken)
			fakeDatadogAPI = NewFakeDatadogAPI()
			tokenFetcher = &FakeTokenFetcher{}

			fakeUAA.Start()
			fakeFirehose.Start()
			fakeDatadogAPI.Start()

			config = &nozzleconfig.NozzleConfig{
				FlushDurationSeconds: 1,
				FlushMaxBytes:        10240,
				DataDogURL:           fakeDatadogAPI.URL(),
				DataDogAPIKey:        "1234567890",
				TrafficControllerURL: strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1),
				DisableAccessControl: true,
				NumWorkers:           1,
				AppMetrics:           false,
			}

			os.Remove("firehose_nozzle.db")
		})

		JustBeforeEach(func() {
			tokenFetcher := uaatokenfetcher.New(fakeUAA.URL(), "un", "pwd", true, log)
			nozzle = NewDatadogFirehoseNozzle(config, tokenFetcher, log)
			go nozzle.Start()
			time.Sleep(time.Second)
		})

		AfterEach(func() {
			nozzle.Stop()
			fakeUAA.Close()
			fakeFirehose.Close()
			fakeDatadogAPI.Close()
			os.Remove("firehose_nozzle.db")
		})

		It("can still tries to connect to the firehose", func() {
			Eventually(fakeFirehose.Requested).Should(BeTrue())
		})

		It("gets an empty authentication token", func() {
			Consistently(fakeUAA.Requested).Should(Equal(false))
			Consistently(fakeFirehose.LastAuthorization).Should(Equal(""))
		})

		It("does not require the presence of config.UAAURL", func() {
			Consistently(func() int { return tokenFetcher.NumCalls }).Should(Equal(0))
		})
	})

	Context("when idle timeout has expired", func() {
		var fakeIdleFirehose *FakeIdleFirehose
		BeforeEach(func() {
			fakeIdleFirehose = NewFakeIdleFirehose(time.Second * 7)
			fakeDatadogAPI = NewFakeDatadogAPI()

			fakeIdleFirehose.Start()
			fakeDatadogAPI.Start()

			config = &nozzleconfig.NozzleConfig{
				DataDogURL:           fakeDatadogAPI.URL(),
				DataDogAPIKey:        "1234567890",
				TrafficControllerURL: strings.Replace(fakeIdleFirehose.URL(), "http:", "ws:", 1),
				DisableAccessControl: true,
				IdleTimeoutSeconds:   1,
				WorkerTimeoutSeconds: 10,
				FlushDurationSeconds: 1,
				FlushMaxBytes:        10240,
				NumWorkers:           1,
				AppMetrics:           false,
			}

			tokenFetcher := &FakeTokenFetcher{}
			nozzle = NewDatadogFirehoseNozzle(config, tokenFetcher, log)
			os.Remove("firehose_nozzle.db")
		})

		AfterEach(func() {
			fakeDatadogAPI.Close()
			os.Remove("firehose_nozzle.db")
		})

		It("Start returns an error", func() {
			err := nozzle.Start()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("i/o timeout"))
		})
	})

	Context("when workers timeout", func() {
		BeforeEach(func() {
			fakeUAA = NewFakeUAA("bearer", "123456789")
			fakeToken := fakeUAA.AuthToken()
			fakeFirehose = NewFakeFirehose(fakeToken)
			fakeDatadogAPI = NewFakeDatadogAPI()
			fakeUAA.Start()
			fakeFirehose.Start()
			fakeDatadogAPI.Start()

			config = &nozzleconfig.NozzleConfig{
				UAAURL:               fakeUAA.URL(),
				FlushDurationSeconds: 2,
				FlushMaxBytes:        10240,
				DataDogURL:           fakeDatadogAPI.URL(),
				DataDogAPIKey:        "1234567890",
				TrafficControllerURL: strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1),
				DisableAccessControl: false,
				WorkerTimeoutSeconds: 1,
				MetricPrefix:         "datadog.nozzle.",
				Deployment:           "nozzle-deployment",
				AppMetrics:           false,
				NumWorkers:           1,
			}
			os.Remove("firehose_nozzle.db")
		})

		JustBeforeEach(func() {
			tokenFetcher := uaatokenfetcher.New(fakeUAA.URL(), "un", "pwd", true, log)
			nozzle = NewDatadogFirehoseNozzle(config, tokenFetcher, log)
		})

		AfterEach(func() {
			nozzle.Stop()
			fakeUAA.Close()
			fakeFirehose.Close()
			fakeDatadogAPI.Close()
			os.Remove("firehose_nozzle.db")
		})

		It("logs a warning", func() {
			go nozzle.Start()
			time.Sleep(time.Second)
			nozzle.workersStopper <- true // Stop one worker
			nozzle.stopWorkers()          // We should hit the worker timeout for one worker

			logOutput := fakeBuffer.GetContent()
			Expect(logOutput).To(ContainSubstring("Could not stop 1 workers after 1s"))
		}, 4)
	})
})

func findSlowConsumerMetric(payload datadogclient.Payload) *metrics.Series {
	for _, metric := range payload.Series {
		if metric.Metric == "datadog.nozzle.slowConsumerAlert" {
			return &metric
		}
	}
	return nil
}

func filterOutNozzleMetrics(deployment string, c <-chan []byte) <-chan []byte {
	filter := "deployment:" + deployment
	result := make(chan []byte)
	go func() {
		for b := range c {
			if !strings.Contains(string(Decompress(b)), filter) {
				result <- b
			}
		}
	}()
	return result
}

func validateMetrics(payload datadogclient.Payload, totalMessagesReceived int, totalMetricsSent int) {
	totalMessagesReceivedFound := false
	totalMetricsSentFound := false
	slowConsumerAlertFound := false
	for _, metric := range payload.Series {
		Expect(metric.Type).To(Equal("gauge"))

		internalMetric := false
		var metricValue int
		if metric.Metric == "datadog.nozzle.totalMessagesReceived" {
			totalMessagesReceivedFound = true
			internalMetric = true
			metricValue = totalMessagesReceived
		}
		if metric.Metric == "datadog.nozzle.totalMetricsSent" {
			totalMetricsSentFound = true
			internalMetric = true
			metricValue = totalMetricsSent
		}
		if metric.Metric == "datadog.nozzle.slowConsumerAlert" {
			slowConsumerAlertFound = true
			internalMetric = true
			metricValue = 0
		}

		if internalMetric {
			Expect(metric.Points).To(HaveLen(1))
			Expect(metric.Points[0].Timestamp).To(BeNumerically(">", time.Now().Unix()-10), "Timestamp should not be less than 10 seconds ago")
			Expect(metric.Points[0].Value).To(Equal(float64(metricValue)))
			ip, _ := localip.LocalIP()
			Expect(metric.Tags).To(Equal([]string{"deployment:nozzle-deployment", "ip:" + ip}))
		}
	}
	Expect(totalMessagesReceivedFound).To(BeTrue())
	Expect(totalMetricsSentFound).To(BeTrue())
	Expect(slowConsumerAlertFound).To(BeTrue())
}
