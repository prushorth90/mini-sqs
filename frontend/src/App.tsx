import { useEffect, useState } from 'react'
import {
  Activity,
  Box,
  Clock3,
  Gauge,
  Inbox,
  RefreshCw,
  RotateCcw,
  TriangleAlert,
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

const emptyMetrics: DashboardMetrics = {
  queueDepth: 0,
  inFlight: 0,
  retries: 0,
  dlqSize: 0,
  throughput: 0,
  p95Latency: 0,
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
  const [queues, setQueues] = useState<string[]>([])
  const [selectedQueue, setSelectedQueue] = useState('')
  const [messages, setMessages] = useState<Message[]>([])
  const [metrics, setMetrics] = useState(emptyMetrics)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState('')
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)

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
    void loadQueue(selectedQueue)
    const interval = window.setInterval(() => void loadQueue(selectedQueue, true), 5000)
    return () => window.clearInterval(interval)
  }, [selectedQueue])

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
          <div><p className="eyebrow">Queue overview</p><h1>{selectedQueue || 'No queue selected'}</h1></div>
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

        <section className="metric-grid" aria-label="Queue metrics">
          {cards.map(({ label, value, icon: Icon }) => (
            <article className="metric-card" key={label}>
              <div className="metric-label"><Icon size={16} /><span>{label}</span></div>
              <strong>{loading ? '—' : value}</strong>
            </article>
          ))}
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
      </main>
    </div>
  )
}
