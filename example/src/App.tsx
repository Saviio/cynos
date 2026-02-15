import { useState, useEffect, useCallback, useRef } from 'react'
import {
  type Stock,
  STOCK_COLUMNS,
  getDatabase,
  getStockCount,
  insertStocks,
  createStockLiveQuery,
  startContinuousUpdates,
  queryStocksWithBinary,
  queryStocksWithJson,
} from './db'

type CellKey = `${number}-${string}`

const DISPLAY_LIMIT = 100 // Only display first 100 rows for performance

export default function App() {
  const [stocks, setStocks] = useState<Stock[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [inserting, setInserting] = useState(false)
  const [liveEnabled, setLiveEnabled] = useState(false)
  const [autoUpdate, setAutoUpdate] = useState(false)
  const [updateCount, setUpdateCount] = useState(0)
  const [batchSize, setBatchSize] = useState(10)
  const [updatesPerSec, setUpdatesPerSec] = useState(0)
  const [highlightedCells, setHighlightedCells] = useState<Set<CellKey>>(new Set())
  const prevStocksRef = useRef<Map<number, Stock>>(new Map())

  const loadStocks = useCallback(async () => {
    await getDatabase()
    const db = await getDatabase()
    const result = await db.select('*').from('stocks').limit(DISPLAY_LIMIT).exec() as Stock[]
    setStocks(result)
    setTotalCount(getStockCount())
    result.forEach(s => prevStocksRef.current.set(s.id, { ...s }))
    setLoading(false)
  }, [])

  useEffect(() => {
    loadStocks()
  }, [loadStocks])

  // Detect changed cells and highlight them
  const detectChanges = useCallback((newStocks: Stock[]) => {
    const changedCells = new Set<CellKey>()

    newStocks.forEach(stock => {
      const prev = prevStocksRef.current.get(stock.id)
      if (prev) {
        STOCK_COLUMNS.forEach(col => {
          if (prev[col] !== stock[col]) {
            changedCells.add(`${stock.id}-${col}`)
          }
        })
      }
      prevStocksRef.current.set(stock.id, { ...stock })
    })

    if (changedCells.size > 0) {
      setHighlightedCells(prev => new Set([...prev, ...changedCells]))
      setTimeout(() => {
        setHighlightedCells(prev => {
          const next = new Set(prev)
          changedCells.forEach(c => next.delete(c))
          return next
        })
      }, 300)
    }
  }, [])

  // Live query subscription
  useEffect(() => {
    if (!liveEnabled) return

    let unsubscribe: (() => void) | undefined

    createStockLiveQuery(DISPLAY_LIMIT).then(observable => {
      const initial = observable.getResult() as Stock[]
      setStocks(initial)
      initial.forEach(s => prevStocksRef.current.set(s.id, { ...s }))

      // Throttle React renders to avoid UI bottleneck
      let pendingData: Stock[] | null = null
      let rafId: number | null = null
      let pendingCount = 0

      const unsub = observable.subscribe((data: Stock[]) => {
        pendingData = data
        pendingCount++

        // Only render once per animation frame
        if (rafId === null) {
          rafId = requestAnimationFrame(() => {
            if (pendingData) {
              detectChanges(pendingData)
              setStocks(pendingData)
              setUpdateCount(c => c + pendingCount)
              pendingCount = 0
            }
            rafId = null
          })
        }
      })
      unsubscribe = () => unsub()
    })

    return () => unsubscribe?.()
  }, [liveEnabled, detectChanges])

  // Auto update using continuous loop
  useEffect(() => {
    if (!autoUpdate || !liveEnabled) return

    const stop = startContinuousUpdates(DISPLAY_LIMIT, batchSize, (stats) => {
      setUpdateCount(stats.updates)
      setUpdatesPerSec(Math.round(stats.ups))
    })

    return () => stop()
  }, [autoUpdate, liveEnabled, batchSize])

  const handleInsert100k = async () => {
    setInserting(true)
    const start = performance.now()
    await insertStocks(100000)
    const time = performance.now() - start
    setTotalCount(getStockCount())
    setInserting(false)
    alert(`Inserted 100,000 stocks in ${(time / 1000).toFixed(2)}s`)
  }

  const formatValue = (col: keyof Stock, value: Stock[keyof Stock]) => {
    if (col === 'price' || col === 'high' || col === 'low' || col === 'open')
      return `$${(value as number).toFixed(2)}`
    if (col === 'change')
      return (value as number) >= 0 ? `+${(value as number).toFixed(2)}` : (value as number).toFixed(2)
    if (col === 'changePercent')
      return (value as number) >= 0 ? `+${(value as number).toFixed(2)}%` : `${(value as number).toFixed(2)}%`
    if (col === 'volume' || col === 'marketCap')
      return (value as number).toLocaleString()
    if (col === 'pe')
      return (value as number).toFixed(1)
    return String(value)
  }

  const getChangeClass = (stock: Stock, col: keyof Stock) => {
    if (col === 'change' || col === 'changePercent') {
      return stock.change >= 0 ? 'positive' : 'negative'
    }
    return ''
  }

  if (loading) {
    return <div className="loading">Initializing Cynos Database...</div>
  }

  return (
    <div className="app">
      <h1>Cynos Live Query Demo</h1>

      <div className="section">
        <div className="stats">
          <div className="stat-card">
            <h3>Total Rows</h3>
            <div className="value">{totalCount.toLocaleString()}</div>
          </div>
          <div className="stat-card">
            <h3>Displaying</h3>
            <div className="value">{stocks.length}</div>
          </div>
          <div className="stat-card">
            <h3>Columns</h3>
            <div className="value">{STOCK_COLUMNS.length}</div>
          </div>
          <div className="stat-card">
            <h3>Updates</h3>
            <div className="value">{updateCount.toLocaleString()}</div>
          </div>
          <div className="stat-card">
            <h3>Updates/sec</h3>
            <div className="value">{updatesPerSec.toLocaleString()}</div>
          </div>
          <div className="stat-card">
            <h3>Batch Size</h3>
            <div className="value">{batchSize}</div>
          </div>
        </div>
      </div>

      <div className="section">
        <h2>
          Live Query
          <span className={`badge ${liveEnabled ? 'live' : ''}`} style={{ marginLeft: 10 }}>
            {liveEnabled ? 'LIVE' : 'OFF'}
          </span>
        </h2>

        <div className="form-row">
          <button
            className={liveEnabled ? 'danger' : 'primary'}
            onClick={() => {
              if (liveEnabled) setAutoUpdate(false)
              setLiveEnabled(!liveEnabled)
            }}
          >
            {liveEnabled ? 'Disable Live Query' : 'Enable Live Query'}
          </button>

          {liveEnabled && (
            <>
              <button
                className={autoUpdate ? 'danger' : 'secondary'}
                onClick={() => setAutoUpdate(!autoUpdate)}
              >
                {autoUpdate ? 'Stop Updates' : 'Start Auto Updates'}
              </button>

              <select
                value={batchSize}
                onChange={e => setBatchSize(Number(e.target.value))}
              >
                <option value={1}>1 row/frame</option>
                <option value={5}>5 rows/frame</option>
                <option value={10}>10 rows/frame</option>
                <option value={20}>20 rows/frame</option>
                <option value={50}>50 rows/frame</option>
                <option value={100}>100 rows/frame</option>
              </select>
            </>
          )}
        </div>

        <div className="form-row">
          <button
            className="secondary"
            onClick={handleInsert100k}
            disabled={inserting}
          >
            {inserting ? 'Inserting...' : 'Insert 100K Stocks'}
          </button>

          <button
            className="secondary"
            onClick={async () => {
              const limit = Math.min(1000, totalCount)
              const json = await queryStocksWithJson(limit)
              const binary = await queryStocksWithBinary(limit)
              alert(`Query ${limit} rows:\nJSON: ${json.time.toFixed(2)}ms\nBinary: ${binary.time.toFixed(2)}ms\nSpeedup: ${(json.time / binary.time).toFixed(1)}x`)
            }}
          >
            Compare Protocols
          </button>

          <button
            className="secondary"
            onClick={async () => {
              const { runPerfTest } = await import('./perf-test')
              console.clear()
              await runPerfTest()
              alert('Performance test complete! Check browser console for results.')
            }}
          >
            Run Perf Test
          </button>
        </div>

        {liveEnabled && autoUpdate && (
          <div className="info-box">
            Continuous updates: {batchSize} rows/frame, actual throughput: {updatesPerSec.toLocaleString()} updates/sec
          </div>
        )}

        <div className="table-container">
          <table className="stock-table">
            <thead>
              <tr>
                {STOCK_COLUMNS.map(col => (
                  <th key={col}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {stocks.map(stock => (
                <tr key={stock.id}>
                  {STOCK_COLUMNS.map(col => (
                    <td
                      key={col}
                      className={`
                        ${getChangeClass(stock, col)}
                        ${highlightedCells.has(`${stock.id}-${col}`) ? 'highlight' : ''}
                      `}
                    >
                      {formatValue(col, stock[col])}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
