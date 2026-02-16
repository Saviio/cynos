import { useState, useEffect, useCallback, useRef } from 'react'
import {
  type Stock,
  getDatabase,
  getStockCount,
  createStockIvmQuery,
  createStockReQueryQuery,
  insertStocks,
} from './db'
import { col } from '@cynos/core'

const PRICE_THRESHOLDS = [100, 200, 300, 500, 700, 900]

const DISPLAY_COLS: (keyof Stock)[] = ['id', 'symbol', 'name', 'price', 'change', 'volume', 'high', 'low', 'pe']

const DISPLAY_LIMIT = 100

type Mode = 'ivm' | 'requery'

interface BenchResult {
  mode: Mode
  totalUpdates: number
  elapsed: number
  ups: number
  notifications: number
}

type CellKey = string

export default function IvmDemo() {
  const [priceThreshold, setPriceThreshold] = useState(500)
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [inserting, setInserting] = useState(false)
  const [benchRunning, setBenchRunning] = useState(false)
  const [benchDuration, setBenchDuration] = useState(3)
  const [currentMode, setCurrentMode] = useState<Mode | null>(null)
  const [liveUps, setLiveUps] = useState(0)
  const [liveNotifications, setLiveNotifications] = useState(0)
  const [results, setResults] = useState<BenchResult[]>([])

  // Live data state (delta-driven)
  const [stocks, setStocks] = useState<Stock[]>([])
  const changedCellsRef = useRef<Map<CellKey, number>>(new Map())
  const stockMapRef = useRef<Map<number, Stock>>(new Map())
  // Sorted display cache — only re-sort when rows are added/removed, not on updates
  const sortedCacheRef = useRef<Stock[]>([])
  const unsubLiveRef = useRef<(() => void) | null>(null)
  const highlightCounter = useRef(0)

  // Apply delta to stockMap, detect changed cells, update state
  const applyDelta = useCallback((delta: { added: Stock[], removed: Stock[] }) => {
    const map = stockMapRef.current
    const tick = ++highlightCounter.current
    let needResort = false
    const changedKeys: string[] = []

    // Build removed lookup (update = delete old + insert new with same id)
    const removedById = new Map<number, Stock>()
    for (const row of delta.removed) {
      removedById.set(row.id, row)
    }

    // Pure deletes (not part of an update)
    for (const row of delta.removed) {
      if (!delta.added.some(a => a.id === row.id)) {
        map.delete(row.id)
        needResort = true
      }
    }

    // Process added
    for (const row of delta.added) {
      const prev = removedById.get(row.id)
      if (prev) {
        // Update: diff fields
        for (const c of DISPLAY_COLS) {
          if (prev[c] !== row[c]) {
            const k = `${row.id}-${c}`
            changedCellsRef.current.set(k, tick)
            changedKeys.push(k)
          }
        }
      } else {
        // Truly new row
        if (!map.has(row.id)) {
          needResort = true
          for (const c of DISPLAY_COLS) {
            const k = `${row.id}-${c}`
            changedCellsRef.current.set(k, tick)
            changedKeys.push(k)
          }
        }
      }
      map.set(row.id, row)
    }

    // Only re-sort when membership changes; otherwise just patch the cached slice
    if (needResort || sortedCacheRef.current.length === 0) {
      sortedCacheRef.current = Array.from(map.values()).sort((a, b) => a.id - b.id).slice(0, DISPLAY_LIMIT)
    } else {
      // Patch in-place: update rows that are in the display window
      const display = sortedCacheRef.current
      for (let i = 0; i < display.length; i++) {
        const fresh = map.get(display[i].id)
        if (fresh) display[i] = fresh
      }
    }

    setStocks([...sortedCacheRef.current])
    if (changedKeys.length > 0) {
      setTimeout(() => {
        for (const k of changedKeys) {
          if (changedCellsRef.current.get(k) === tick) changedCellsRef.current.delete(k)
        }
      }, 400)
    }
  }, [])

  // Subscribe to live IVM query for the selected price threshold
  const subscribeLive = useCallback(async () => {
    unsubLiveRef.current?.()
    unsubLiveRef.current = null
    stockMapRef.current.clear()
    sortedCacheRef.current = []

    const obs = await createStockIvmQuery(priceThreshold)
    const initial = obs.getResult() as Stock[]
    for (const row of initial) stockMapRef.current.set(row.id, row)
    sortedCacheRef.current = Array.from(stockMapRef.current.values()).sort((a, b) => a.id - b.id).slice(0, DISPLAY_LIMIT)
    setStocks([...sortedCacheRef.current])

    // Throttle delta processing to once per animation frame
    let pendingDelta: { added: Stock[], removed: Stock[] } | null = null
    let rafId: number | null = null

    const unsub = obs.subscribe((delta: { added: Stock[], removed: Stock[] }) => {
      // Merge into pending delta — use for..of to avoid spread allocation
      if (pendingDelta) {
        for (const r of delta.added) pendingDelta.added.push(r)
        for (const r of delta.removed) pendingDelta.removed.push(r)
      } else {
        pendingDelta = { added: delta.added.slice(), removed: delta.removed.slice() }
      }

      if (rafId === null) {
        rafId = requestAnimationFrame(() => {
          if (pendingDelta) {
            applyDelta(pendingDelta)
            pendingDelta = null
          }
          rafId = null
        })
      }
    })
    unsubLiveRef.current = () => {
      unsub()
      if (rafId !== null) cancelAnimationFrame(rafId)
    }
  }, [priceThreshold, applyDelta])

  useEffect(() => {
    getDatabase().then(() => {
      setTotalCount(getStockCount())
      setLoading(false)
      subscribeLive()
    })
    return () => { unsubLiveRef.current?.() }
  }, [])

  // Re-subscribe when price threshold changes
  useEffect(() => {
    if (!loading) subscribeLive()
  }, [priceThreshold])

  const stopRef = useRef(false)
  const updatingRef = useRef(false)
  const [updating, setUpdating] = useState(false)
  const [updateUps, setUpdateUps] = useState(0)

  // Continuous random updates for live table demo
  // Uses 16ms tight loop (like runBench) + setTimeout yield for smooth UI
  const startUpdates = useCallback(async () => {
    if (updatingRef.current) return
    updatingRef.current = true
    setUpdating(true)
    const db = await getDatabase()
    const numericFields: (keyof Stock)[] = ['price', 'change', 'volume', 'high', 'low', 'pe']
    let windowUpdates = 0
    let windowStart = performance.now()

    while (updatingRef.current) {
      const batchStart = performance.now()
      while (updatingRef.current && performance.now() - batchStart < 16) {
        const maxId = getStockCount()
        const id = Math.floor(Math.random() * maxId) + 1
        const field = numericFields[Math.floor(Math.random() * numericFields.length)]
        let val: number
        switch (field) {
          case 'price': val = Math.round((10 + Math.random() * 990) * 100) / 100; break
          case 'change': val = Math.round((Math.random() - 0.5) * 100 * 100) / 100; break
          case 'volume': val = Math.floor(Math.random() * 1e8); break
          case 'high': case 'low': val = Math.round((10 + Math.random() * 990) * 100) / 100; break
          case 'pe': val = Math.round((5 + Math.random() * 95) * 10) / 10; break
          default: val = 0
        }
        await db.update('stocks').set(field, val).where(col('id').eq(id)).exec()
        windowUpdates++
      }
      // Sliding window UPS: reset every 1s for stable instantaneous reading
      const now = performance.now()
      const windowElapsed = now - windowStart
      if (windowElapsed >= 1000) {
        setUpdateUps(Math.round(windowUpdates / (windowElapsed / 1000)))
        windowUpdates = 0
        windowStart = now
      }
      // Yield to event loop for UI rendering
      await new Promise(r => setTimeout(r, 0))
    }
    setUpdating(false)
    setUpdateUps(0)
  }, [])

  const stopUpdates = useCallback(() => {
    updatingRef.current = false
  }, [])

  const runBench = useCallback(async (mode: Mode, durationSec: number): Promise<BenchResult> => {
    const db = await getDatabase()
    const maxId = getStockCount()
    const numericFields: (keyof Stock)[] = ['price', 'change', 'volume', 'high', 'low', 'pe']
    let notifications = 0

    let unsub: (() => void) | null = null
    if (mode === 'ivm') {
      const obs = await createStockIvmQuery(priceThreshold)
      const u = obs.subscribe((_delta: { added: Stock[], removed: Stock[] }) => {
        notifications++
        setLiveNotifications(notifications)
      })
      unsub = () => u()
    } else {
      const obs = await createStockReQueryQuery(priceThreshold)
      const u = obs.subscribe((_data: Stock[]) => {
        notifications++
        setLiveNotifications(notifications)
      })
      unsub = () => u()
    }

    let totalUpdates = 0
    const startTime = performance.now()
    const endTime = startTime + durationSec * 1000
    stopRef.current = false

    while (performance.now() < endTime && !stopRef.current) {
      const batchStart = performance.now()
      while (performance.now() - batchStart < 16 && performance.now() < endTime && !stopRef.current) {
        const id = Math.floor(Math.random() * maxId) + 1
        const field = numericFields[Math.floor(Math.random() * numericFields.length)]
        let val: number
        switch (field) {
          case 'price': val = Math.round((10 + Math.random() * 990) * 100) / 100; break
          case 'change': val = Math.round((Math.random() - 0.5) * 100 * 100) / 100; break
          case 'volume': val = Math.floor(Math.random() * 1e8); break
          case 'high': case 'low': val = Math.round((10 + Math.random() * 990) * 100) / 100; break
          case 'pe': val = Math.round((5 + Math.random() * 95) * 10) / 10; break
          default: val = 0
        }
        await db.update('stocks').set(field, val).where(col('id').eq(id)).exec()
        totalUpdates++
      }
      const elapsed = performance.now() - startTime
      setLiveUps(Math.round(totalUpdates / (elapsed / 1000)))
      await new Promise(r => setTimeout(r, 0))
    }

    unsub?.()
    const elapsed = performance.now() - startTime
    return { mode, totalUpdates, elapsed, ups: Math.round(totalUpdates / (elapsed / 1000)), notifications }
  }, [priceThreshold])

  const handleRunBench = useCallback(async () => {
    // Stop live updates during benchmark
    updatingRef.current = false
    setBenchRunning(true)
    setResults([])

    setCurrentMode('ivm')
    setLiveUps(0)
    setLiveNotifications(0)
    const ivmResult = await runBench('ivm', benchDuration)
    setResults(r => [...r, ivmResult])

    await new Promise(r => setTimeout(r, 500))

    setCurrentMode('requery')
    setLiveUps(0)
    setLiveNotifications(0)
    const reqResult = await runBench('requery', benchDuration)
    setResults(r => [...r, reqResult])

    setCurrentMode(null)
    setBenchRunning(false)
  }, [runBench, benchDuration])

  const handleInsert = async (count: number) => {
    setInserting(true)
    // Pause live subscription during bulk insert to avoid per-batch dataflow overhead
    unsubLiveRef.current?.()
    unsubLiveRef.current = null
    const t0 = performance.now()
    await insertStocks(count)
    const dt = performance.now() - t0
    setTotalCount(getStockCount())
    // Re-subscribe to pick up new data
    await subscribeLive()
    setInserting(false)
    alert(`Inserted ${count.toLocaleString()} rows in ${(dt / 1000).toFixed(2)}s — total: ${getStockCount().toLocaleString()}`)
  }

  if (loading) {
    return <div className="loading">Initializing Cynos Database...</div>
  }

  const ivmResult = results.find(r => r.mode === 'ivm')
  const reqResult = results.find(r => r.mode === 'requery')

  return (
    <div className="app">
      <h1>IVM Live Query Demo</h1>

      <div className="section">
        <div className="form-row">
          <select value={priceThreshold} onChange={e => setPriceThreshold(Number(e.target.value))} disabled={benchRunning}>
            {PRICE_THRESHOLDS.map(t => <option key={t} value={t}>price &gt; {t}</option>)}
          </select>
          <button className={updating ? 'primary' : 'secondary'} onClick={updating ? stopUpdates : startUpdates} disabled={benchRunning}>
            {updating ? 'Stop Updates' : 'Start Updates'}
          </button>
          {updating && <span style={{ color: '#aaa' }}>{updateUps} ups</span>}
          <span style={{ color: '#aaa' }}>Total: {totalCount.toLocaleString()} rows</span>
          <button className="secondary" onClick={() => handleInsert(100_000)} disabled={inserting || benchRunning}>
            {inserting ? 'Inserting...' : '+100K Rows'}
          </button>
          <button className="secondary" onClick={() => handleInsert(500_000)} disabled={inserting || benchRunning}>
            {inserting ? 'Inserting...' : '+500K Rows'}
          </button>
        </div>
      </div>

      {/* Live data table — delta-driven via IVM trace() */}
      <div className="section">
        <h2>
          Live Data <span className="badge live">IVM trace()</span>
          <span style={{ color: '#aaa', fontSize: 14, fontWeight: 'normal', marginLeft: 8 }}>
            {stocks.length > 0 ? `showing ${Math.min(20, stockMapRef.current.size)} of ${stockMapRef.current.size} rows where price > ${priceThreshold}` : ''}
          </span>
        </h2>
        <div className="table-container" style={{ maxHeight: 460 }}>
          <table className="stock-table">
            <thead>
              <tr>{DISPLAY_COLS.map(c => <th key={c}>{c}</th>)}</tr>
            </thead>
            <tbody>
              {stocks.map(stock => (
                <tr key={stock.id}>
                  {DISPLAY_COLS.map(c => {
                    const key = `${stock.id}-${c}`
                    const tick = changedCellsRef.current.get(key)
                    return (
                      <td key={tick != null ? `${key}-${tick}` : key} className={tick != null ? 'highlight' : ''}>
                        {fmtVal(c, stock[c])}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Benchmark section */}
      <div className="section">
        <h2>Benchmark</h2>
        <p style={{ color: '#aaa', margin: '0 0 12px', fontSize: 13 }}>
          Runs DML for {benchDuration}s with IVM, then {benchDuration}s with Re-query.
          IVM subscribe = O(delta), Re-query subscribe = O(result_set).
        </p>
        <div className="form-row">
          <select value={benchDuration} onChange={e => setBenchDuration(Number(e.target.value))} disabled={benchRunning}>
            <option value={2}>2s per mode</option>
            <option value={3}>3s per mode</option>
            <option value={5}>5s per mode</option>
            <option value={10}>10s per mode</option>
          </select>
          <button className="primary" onClick={handleRunBench} disabled={benchRunning}>
            {benchRunning ? `Running ${currentMode === 'ivm' ? 'IVM' : 'Re-query'}...` : 'Run Benchmark'}
          </button>
          {currentMode && (
            <span style={{ color: '#aaa' }}>
              UPS: {liveUps.toLocaleString()} | Notifications: {liveNotifications}
            </span>
          )}
        </div>
      </div>

      {results.length > 0 && (
        <div className="section">
          <h2>Results</h2>
          <div style={{ display: 'flex', gap: 24 }}>
            {ivmResult && (
              <div style={{ flex: 1 }} className="info-box">
                <h3 style={{ margin: '0 0 8px' }}>
                  IVM (trace) <span className="badge live">O(delta)</span>
                </h3>
                <div>Updates/sec: <strong>{ivmResult.ups.toLocaleString()}</strong></div>
                <div>Total updates: {ivmResult.totalUpdates.toLocaleString()}</div>
                <div>Notifications: {ivmResult.notifications}</div>
              </div>
            )}
            {reqResult && (
              <div style={{ flex: 1 }} className="info-box">
                <h3 style={{ margin: '0 0 8px' }}>
                  Re-query (observe) <span className="badge">O(result)</span>
                </h3>
                <div>Updates/sec: <strong>{reqResult.ups.toLocaleString()}</strong></div>
                <div>Total updates: {reqResult.totalUpdates.toLocaleString()}</div>
                <div>Notifications: {reqResult.notifications}</div>
              </div>
            )}
          </div>
          {ivmResult && reqResult && (
            <div className="info-box" style={{ marginTop: 12, textAlign: 'center', fontSize: 18 }}>
              IVM is <strong>{(ivmResult.ups / reqResult.ups).toFixed(1)}x</strong> faster
              ({ivmResult.ups.toLocaleString()} vs {reqResult.ups.toLocaleString()} updates/sec)
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function fmtVal(col: keyof Stock, value: Stock[keyof Stock]) {
  if (col === 'price' || col === 'high' || col === 'low' || col === 'open')
    return `$${(value as number).toFixed(2)}`
  if (col === 'change')
    return (value as number) >= 0 ? `+${(value as number).toFixed(2)}` : (value as number).toFixed(2)
  if (col === 'volume' || col === 'marketCap')
    return (value as number).toLocaleString()
  return String(value)
}
