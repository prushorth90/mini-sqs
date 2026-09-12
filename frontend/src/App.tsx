import { useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import {
  Activity,
  Box,
  Clock3,
  Gauge,
  Inbox,
  LayoutDashboard,
  Play,
  RefreshCw,
  RotateCcw,
  Send,
  Square,
  Timer,
  TriangleAlert,
  Users,
} from 'lucide-react'
import './App.css'

const apiBase = import.meta.env.VITE_API_URL ?? 'http://localhost:8080'
const prometheusBase = import.meta.env.VITE_PROMETHEUS_URL ?? 'http://localhost:9090'

type MessageStatus = 'available' | 'in-flight' | 'dead-letter'

type Message = {
  messageId: string
  body: string
  createdAt: number
  receiveCount: number
  status: MessageStatus
}

type DashboardMetrics = {
  queueDepth: number
  inFlight: number
  retries: number
  dlqSize: number
  throughput: number
  p95Latency: number
}

type SimulatorState = {
  running: boolean
  queue: string
  consumerCount: number
  processingDelayMs: number
  failureProbability: number
  received: number
  acknowledged: number
  failed: number
}

const emptyMetrics: DashboardMetrics = {
  queueDepth: 0,
  inFlight: 0,
  retries: 0,
  dlqSize: 0,
  throughput: 0,
  p95Latency: 0,
}

const emptySimulator: SimulatorState = {
  running: false,
  queue: '',
  consumerCount: 0,
  processingDelayMs: 0,
  failureProbability: 0,
  received: 0,
  acknowledged: 0,
  failed: 0,
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url)
  if (!response.ok) throw new Error(`Request failed with status ${response.status}`)
  return response.json() as Promise<T>
}

async function queryMetric(query: string): Promise<number> {
  const url = new URL('/api/v1/query', prometheusBase)
  url.searchParams.set('query', query)
  const response = await fetchJson<{
    data: { result: Array<{ value: [number, string] }> }
  }>(url.toString())
  return Number(response.data.result[0]?.value[1] ?? 0)
}

function queueMetric(name: string, queue: string) {
  return `${name}{queue="${queue}"}`
}

function formatAge(createdAt: number) {
  const seconds = Math.max(0, Math.floor((Date.now() - createdAt) / 1000))
  if (seconds < 60) return `${seconds}s`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`
  return `${Math.floor(seconds / 86400)}d`
}

function formatLatency(seconds: number) {
  if (seconds === 0) return '0 ms'
  if (seconds < 1) return `${Math.round(seconds * 1000)} ms`
  return `${seconds.toFixed(2)} s`
}

export default function App() {
  const [view, setView] = useState<'dashboard' | 'simulator'>('dashboard')
  const [queues, setQueues] = useState<string[]>([])
  const [selectedQueue, setSelectedQueue] = useState('')
  const selectedQueueRef = useRef('')
  const [messages, setMessages] = useState<Message[]>([])
  const [metrics, setMetrics] = useState(emptyMetrics)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const [messageBody, setMessageBody] = useState('')
  const [idempotencyKey, setIdempotencyKey] = useState('')
  const [publishing, setPublishing] = useState(false)
  const [publishFeedback, setPublishFeedback] = useState('')
  const [consumerCount, setConsumerCount] = useState(20)
  const [processingDelayMs, setProcessingDelayMs] = useState(500)
  const [failurePercent, setFailurePercent] = useState(10)
  const [simulator, setSimulator] = useState(emptySimulator)
  const [simulatorPending, setSimulatorPending] = useState(false)
  const [simulatorFeedback, setSimulatorFeedback] = useState('')

  async function loadQueues() {
    const response = await fetchJson<{ queues: string[] }>(`${apiBase}/queues`)
    setQueues(response.queues)
    setSelectedQueue((current) =>
      current && response.queues.includes(current) ? current : (response.queues[0] ?? ''),
    )
  }

  async function loadQueue(queue: string, quiet = false) {
    if (!queue) {
      setMessages([])
      setMetrics(emptyMetrics)
      setLoading(false)
      return
    }
    if (!quiet) setLoading(true)
    setRefreshing(true)
    try {
      const encodedQueue = encodeURIComponent(queue)
      const [recent, deadLetters, queueDepth, inFlight, retries, throughput, p95Latency] =
        await Promise.all([
          fetchJson<{ messages: Message[] }>(`${apiBase}/queues/${encodedQueue}/messages/recent`),
          fetchJson<{ messages: Message[] }>(`${apiBase}/queues/${encodedQueue}/dlq/messages`),
          queryMetric(queueMetric('queue_depth', queue)),
          queryMetric(queueMetric('messages_in_flight', queue)),
          queryMetric(queueMetric('messages_retried_total', queue)),
          queryMetric(`rate(messages_published_total{queue="${queue}"}[1m])`),
          queryMetric(
            `message_processing_latency_seconds{queue="${queue}",quantile="0.95"}`,
          ),
        ])
      setMessages(recent.messages)
      setMetrics({
        queueDepth,
        inFlight,
        retries,
        dlqSize: deadLetters.messages.length,
        throughput,
        p95Latency,
      })
      setError('')
      setLastUpdated(new Date())
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Unable to load dashboard')
    } finally {
      setLoading(false)
      setRefreshing(false)
    }
  }

  async function handlePublish(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const body = messageBody.trim()
    if (!selectedQueue || !body) return

    setPublishing(true)
    setPublishFeedback('')
    try {
      const response = await fetch(`${apiBase}/queues/${encodeURIComponent(selectedQueue)}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          body,
          ...(idempotencyKey.trim() ? { idempotencyKey: idempotencyKey.trim() } : {}),
        }),
      })
      if (!response.ok) throw new Error(`Publish failed with status ${response.status}`)
      const result = await response.json() as { messageId: string; deduplicated: boolean }
      setMessageBody('')
      setIdempotencyKey('')
      setPublishFeedback(
        result.deduplicated
          ? `Duplicate request returned ${result.messageId.slice(0, 8)}`
          : `Published ${result.messageId.slice(0, 8)}`,
      )
      await loadQueue(selectedQueue, true)
      window.setTimeout(() => {
        if (selectedQueueRef.current === selectedQueue) void loadQueue(selectedQueue, true)
      }, 5500)
    } catch (requestError) {
      setPublishFeedback(
        requestError instanceof Error ? requestError.message : 'Unable to publish message',
      )
    } finally {
      setPublishing(false)
    }
  }

  async function loadSimulator() {
    const state = await fetchJson<SimulatorState>(`${apiBase}/simulator`)
    setSimulator(state)
  }

  async function handleStartSimulator(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!selectedQueue) return

    setSimulatorPending(true)
    setSimulatorFeedback('')
    try {
      const response = await fetch(`${apiBase}/simulator`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          queue: selectedQueue,
          consumerCount,
          processingDelayMs,
          failureProbability: failurePercent / 100,
        }),
      })
      if (!response.ok) throw new Error(`Start failed with status ${response.status}`)
      await loadSimulator()
      setSimulatorFeedback(`Consumers started on ${selectedQueue}`)
    } catch (requestError) {
      setSimulatorFeedback(
        requestError instanceof Error ? requestError.message : 'Unable to start consumers',
      )
    } finally {
      setSimulatorPending(false)
    }
  }

  async function handleStopSimulator() {
    setSimulatorPending(true)
    setSimulatorFeedback('')
    try {
      const response = await fetch(`${apiBase}/simulator`, { method: 'DELETE' })
      if (!response.ok) throw new Error(`Stop failed with status ${response.status}`)
      await loadSimulator()
      setSimulatorFeedback('Consumers stopped')
    } catch (requestError) {
      setSimulatorFeedback(
        requestError instanceof Error ? requestError.message : 'Unable to stop consumers',
      )
    } finally {
      setSimulatorPending(false)
    }
  }

  useEffect(() => {
    loadQueues().catch((requestError: unknown) => {
      setError(requestError instanceof Error ? requestError.message : 'Unable to load queues')
      setLoading(false)
    })
    const interval = window.setInterval(() => {
      loadQueues().catch(() => undefined)
    }, 5000)
    return () => window.clearInterval(interval)
  }, [])

  useEffect(() => {
    selectedQueueRef.current = selectedQueue
    void loadQueue(selectedQueue)
    const interval = window.setInterval(() => void loadQueue(selectedQueue, true), 5000)
    return () => window.clearInterval(interval)
  }, [selectedQueue])

  useEffect(() => {
    void loadSimulator().catch(() => undefined)
    const interval = window.setInterval(() => void loadSimulator().catch(() => undefined), 1000)
    return () => window.clearInterval(interval)
  }, [])

  const cards = [
    { label: 'Queue depth', value: metrics.queueDepth.toLocaleString(), icon: Inbox },
    { label: 'In flight', value: metrics.inFlight.toLocaleString(), icon: Activity },
    { label: 'Retries', value: metrics.retries.toLocaleString(), icon: RotateCcw },
    { label: 'DLQ size', value: metrics.dlqSize.toLocaleString(), icon: TriangleAlert },
    { label: 'Throughput', value: `${metrics.throughput.toFixed(2)}/s`, icon: Gauge },
    { label: 'P95 latency', value: formatLatency(metrics.p95Latency), icon: Clock3 },
  ]

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark"><Box size={18} strokeWidth={2.5} /></span>
          <div><strong>Mini SQS</strong><small>Broker console</small></div>
        </div>
        <div className="view-switcher" aria-label="Console views">
          <button
            className={view === 'dashboard' ? 'view-button active' : 'view-button'}
            onClick={() => setView('dashboard')}
            type="button"
          >
            <LayoutDashboard size={15} /><span>Dashboard</span>
          </button>
          <button
            className={view === 'simulator' ? 'view-button active' : 'view-button'}
            onClick={() => setView('simulator')}
            type="button"
          >
            <Users size={15} /><span>Simulator</span>
          </button>
        </div>
        <div className="sidebar-heading">
          <span>Queues</span><span className="queue-count">{queues.length}</span>
        </div>
        <nav aria-label="Queues">
          {queues.map((queue) => (
            <button
              className={queue === selectedQueue ? 'queue-button active' : 'queue-button'}
              key={queue}
              onClick={() => setSelectedQueue(queue)}
              type="button"
            >
              <span className="queue-indicator" /><span>{queue}</span>
            </button>
          ))}
          {!loading && queues.length === 0 && <p className="empty-sidebar">No queues yet</p>}
        </nav>
        <div className={error ? 'connection disconnected' : 'connection'}>
          <span /> {error ? 'Connection issue' : 'Backend connected'}
        </div>
      </aside>

      <main className="workspace">
        <header className="workspace-header">
          <div>
            <p className="eyebrow">{view === 'dashboard' ? 'Queue overview' : 'Load testing'}</p>
            <h1>{view === 'dashboard' ? (selectedQueue || 'No queue selected') : 'Consumer simulator'}</h1>
          </div>
          <div className="header-actions">
            <span className="updated">
              {lastUpdated ? `Updated ${lastUpdated.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}` : 'Waiting for data'}
            </span>
            <button
              className="icon-button"
              disabled={refreshing}
              onClick={() => {
                void loadQueues()
                if (selectedQueue) void loadQueue(selectedQueue, true)
              }}
              title="Refresh dashboard"
              type="button"
            >
              <RefreshCw className={refreshing ? 'spinning' : ''} size={18} />
            </button>
          </div>
        </header>

        {error && <div className="error-banner" role="alert">{error}</div>}

        {view === 'dashboard' ? <>
        <section className="metric-grid" aria-label="Queue metrics">
          {cards.map(({ label, value, icon: Icon }) => (
            <article className="metric-card" key={label}>
              <div className="metric-label"><Icon size={16} /><span>{label}</span></div>
              <strong>{loading ? '—' : value}</strong>
            </article>
          ))}
        </section>

        <section className="publish-panel">
          <div className="publish-heading">
            <div><p className="eyebrow">Producer</p><h2>Publish message</h2></div>
            {publishFeedback && <span className="publish-feedback" role="status">{publishFeedback}</span>}
          </div>
          <form className="publish-form" onSubmit={handlePublish}>
            <label>
              <span>Queue</span>
              <select
                disabled={queues.length === 0 || publishing}
                onChange={(event) => setSelectedQueue(event.target.value)}
                value={selectedQueue}
              >
                {queues.map((queue) => <option key={queue} value={queue}>{queue}</option>)}
              </select>
            </label>
            <label className="body-field">
              <span>Message body</span>
              <textarea
                disabled={!selectedQueue || publishing}
                onChange={(event) => setMessageBody(event.target.value)}
                placeholder="Enter message payload"
                required
                rows={3}
                value={messageBody}
              />
            </label>
            <label>
              <span>Idempotency key <small>optional</small></span>
              <input
                disabled={!selectedQueue || publishing}
                onChange={(event) => setIdempotencyKey(event.target.value)}
                placeholder="order-123"
                type="text"
                value={idempotencyKey}
              />
            </label>
            <button
              className="publish-button"
              disabled={!selectedQueue || !messageBody.trim() || publishing}
              type="submit"
            >
              <Send size={16} />
              <span>{publishing ? 'Publishing…' : 'Publish'}</span>
            </button>
          </form>
        </section>

        <section className="messages-panel">
          <div className="section-heading">
            <div><p className="eyebrow">Latest state</p><h2>Recent messages</h2></div>
            <span>{messages.length} shown</span>
          </div>
          <div className="table-wrap">
            <table>
              <thead><tr><th>Message ID</th><th>Status</th><th>Receives</th><th>Age</th></tr></thead>
              <tbody>
                {messages.map((message) => (
                  <tr key={message.messageId}>
                    <td><code title={message.messageId}>{message.messageId.slice(0, 8)}</code><span className="message-preview">{message.body}</span></td>
                    <td><span className={`status-badge ${message.status}`}>{message.status}</span></td>
                    <td>{message.receiveCount}</td>
                    <td>{formatAge(message.createdAt)}</td>
                  </tr>
                ))}
                {!loading && messages.length === 0 && (
                  <tr><td className="empty-table" colSpan={4}>No messages in this queue</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
        </> : <section className="simulator-layout">
          <div className="simulator-panel">
            <div className="simulator-heading">
              <div><p className="eyebrow">Configuration</p><h2>Consumer workers</h2></div>
              <span className={simulator.running ? 'run-status running' : 'run-status'}>
                <i />{simulator.running ? 'Running' : 'Stopped'}
              </span>
            </div>
            <form className="simulator-form" onSubmit={handleStartSimulator}>
              <label>
                <span>Queue</span>
                <select
                  disabled={simulatorPending || queues.length === 0}
                  onChange={(event) => setSelectedQueue(event.target.value)}
                  value={selectedQueue}
                >
                  {queues.map((queue) => <option key={queue} value={queue}>{queue}</option>)}
                </select>
              </label>
              <label>
                <span>Consumers</span>
                <input
                  max={500}
                  min={1}
                  onChange={(event) => setConsumerCount(Number(event.target.value))}
                  required
                  type="number"
                  value={consumerCount}
                />
              </label>
              <label>
                <span>Processing delay <small>ms</small></span>
                <input
                  max={600000}
                  min={0}
                  onChange={(event) => setProcessingDelayMs(Number(event.target.value))}
                  required
                  step={50}
                  type="number"
                  value={processingDelayMs}
                />
              </label>
              <label>
                <span>Failure probability <small>%</small></span>
                <input
                  max={100}
                  min={0}
                  onChange={(event) => setFailurePercent(Number(event.target.value))}
                  required
                  type="number"
                  value={failurePercent}
                />
              </label>
              <div className="simulator-actions">
                <button
                  className="start-button"
                  disabled={!selectedQueue || simulatorPending}
                  type="submit"
                >
                  <Play size={16} fill="currentColor" />
                  <span>{simulator.running ? 'Restart' : 'Start'}</span>
                </button>
                <button
                  className="stop-button"
                  disabled={!simulator.running || simulatorPending}
                  onClick={() => void handleStopSimulator()}
                  type="button"
                >
                  <Square size={15} fill="currentColor" /><span>Stop</span>
                </button>
              </div>
            </form>
            {simulatorFeedback && <p className="simulator-feedback" role="status">{simulatorFeedback}</p>}
          </div>

          <div className="run-panel">
            <div className="simulator-heading">
              <div><p className="eyebrow">Live run</p><h2>{simulator.queue || 'No active queue'}</h2></div>
              {simulator.running && <span className="run-config">{simulator.consumerCount} workers · {simulator.processingDelayMs} ms · {(simulator.failureProbability * 100).toFixed(0)}%</span>}
            </div>
            <div className="run-stats">
              <article><Users size={17} /><span>Received</span><strong>{simulator.received.toLocaleString()}</strong></article>
              <article><Activity size={17} /><span>Acknowledged</span><strong>{simulator.acknowledged.toLocaleString()}</strong></article>
              <article><TriangleAlert size={17} /><span>Failed</span><strong>{simulator.failed.toLocaleString()}</strong></article>
              <article><Timer size={17} /><span>Success rate</span><strong>{simulator.received ? `${((simulator.acknowledged / simulator.received) * 100).toFixed(1)}%` : '—'}</strong></article>
            </div>
          </div>
        </section>}
      </main>
    </div>
  )
}
