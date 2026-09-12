# Mini SQS

Mini SQS is an in-memory message broker with named queues, blocking consumers,
visibility timeouts, receipt-based acknowledgements, retries, and dead-letter
queues. The backend runs on port 8080 and Docker Compose exposes the React
dashboard on port 5174.

Run `docker compose up --build`, then open `http://localhost:5174`. The frontend
is compiled in a Node build stage and served by a minimal nginx runtime image.
Browser requests use same-origin `/api` and `/prometheus` paths, which nginx
proxies to the backend and Prometheus containers respectively.

## Delivery semantics

Publishing accepts an optional `idempotencyKey`. Within a five-minute window,
repeating a publish with the same key on the same queue returns the originally
accepted message instead of enqueueing a duplicate. Deduplication records are
in-memory and scoped to one queue.

Consumption is at least once. A message can be delivered again when its
visibility timeout expires, including when a consumer completes an external
side effect but fails before acknowledging the message. Consumers must make
their own processing idempotent, for example by storing processed message IDs
or using idempotent writes. The broker cannot guarantee that an external side
effect occurs exactly once.

## Persistence

The backend appends `CREATE`, `PUBLISH`, `RECEIVE`, `ACK`, `REQUEUE`, and `DLQ`
records to `data/queue.log` and flushes every transition before changing
in-memory state.
At startup it replays the log, restoring outstanding messages and dead-letter
queues. Messages that were in flight when the process stopped become available
again, preserving at-least-once delivery. Set `MINI_SQS_LOG_PATH` to use a
different file; Docker Compose stores the default path in a named volume.

## Metrics

`GET /metrics` exposes Prometheus text metrics labeled by queue. Counters cover
published, acknowledged, retried, and dead-lettered messages; gauges report
available depth and in-flight deliveries. Wait time is measured from message
creation to each delivery, and processing latency from delivery to successful
acknowledgement. Both latency summaries include exact nearest-rank P50, P95,
and P99 values plus sample count and sum.

Prometheus runs at `http://localhost:9090` under Docker Compose and scrapes the
backend's `/metrics` endpoint every five seconds. Its time-series database uses
a named volume, so collected development metrics survive container restarts.

## Consumer simulator

Open the dashboard at `http://localhost:5174`, select **Simulator**, choose a
queue, and configure the consumer count, processing delay, and failure
probability. The defaults are 20 consumers, 500 ms processing, and a 10%
failure rate. Start the simulator, publish messages, then return to the
dashboard to watch in-flight, retry, latency, and DLQ metrics.

The workers run in the C++ backend rather than in browser tabs, so large worker
counts are independent of React rendering and page lifecycle. A simulated
failure intentionally omits the acknowledgement; the queue's normal visibility
timeout then drives retries and dead-lettering. Only one simulation is active
at a time, and starting another configuration replaces the current run.

The simulator API is `POST /simulator` to start or replace a run,
`GET /simulator` for live counters, and `DELETE /simulator` to stop it.

## Load and stress testing

Select **Load test** in the dashboard to create a fresh queue and publish from
1,000 to 50,000 messages through concurrent C++ producer threads. Each run
configures its producer and consumer counts, processing delay, failure
probability, visibility timeout, and maximum receive count. Queue names must be
unique because load queues use the broker's normal persistent registry.

The load runner starts the existing consumer simulator against that queue. A
failed processing attempt does not acknowledge its receipt handle. The message
remains in flight until its configured visibility timeout, then the broker
either requeues it with an incremented receive count and a new receipt handle,
or moves it to the DLQ once the maximum receive count is reached. Old receipt
handles cannot acknowledge a later delivery.

Results are derived from actual broker transitions, not frontend estimates:
published, acknowledged, retried, dead-lettered, average throughput, peak
throughput, and duration. Throughput uses acknowledgement timestamps recorded
by the C++ broker. Current throughput is the rolling ACK rate over the previous
second and returns to zero after one idle second. Average throughput is total
ACKs divided by the first-to-last ACK span, while peak throughput is the highest
rolling ACK rate observed during the run; average and peak remain available
after completion. The adjacent metrics also show queue depth, in-flight
deliveries, retries, DLQ size, and P95 processing latency for the same queue.

The load API is `POST /load-tests` to start, `GET /load-tests` for live results,
and `DELETE /load-tests` to stop. A start request has this shape:

```json
{
	"queue": "load-001",
	"messageCount": 10000,
	"producerCount": 8,
	"consumerCount": 20,
	"processingDelayMs": 10,
	"failureProbability": 0.1,
	"visibilityTimeoutMs": 1000,
	"maxReceiveCount": 5
}
```