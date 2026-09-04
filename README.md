# Mini SQS

Mini SQS is an in-memory message broker with named queues, blocking consumers,
visibility timeouts, receipt-based acknowledgements, retries, and dead-letter
queues. The backend runs on port 8080 and the React dashboard runs on port 5173.

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