import { useState, useEffect, useCallback, useRef, memo } from 'react'
import { type Stock, STOCK_COLUMNS } from './db'
import { useDbWorker } from './useDbWorker'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Loader2, Play, Square, Database, Zap, Cpu } from 'lucide-react'
import { cn } from '@/lib/utils'

interface PerfResult {
  name: string
  totalRows: number
  batchSize: number
  totalUpdates: number
  durationMs: number
  updatesPerSec: number
  avgUpdateMs: number
  notifications?: number
}

type CellKey = `${number}-${string}`

const DISPLAY_LIMIT = 100

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

// Memoized table row for better performance
const StockRow = memo(function StockRow({
  stock,
  highlightedCells
}: {
  stock: Stock
  highlightedCells: Set<CellKey>
}) {
  return (
    <tr className="border-b border-white/5 hover:bg-white/[0.02]">
      {STOCK_COLUMNS.map(col => (
        <td
          key={col}
          className={cn(
            "font-mono text-xs whitespace-nowrap py-2 px-4",
            col === 'id' || col === 'symbol' || col === 'name' || col === 'sector' ? 'text-left' : 'text-right',
            col === 'id' && 'text-white/30',
            col === 'symbol' && 'font-medium',
            col === 'name' && 'text-white/50',
            col === 'sector' && 'text-white/40',
            (col === 'change' || col === 'changePercent') && (stock.change >= 0 ? 'text-white' : 'text-white/50'),
            highlightedCells.has(`${stock.id}-${col}`) && 'animate-cell-flash'
          )}
        >
          {formatValue(col, stock[col])}
        </td>
      ))}
    </tr>
  )
})

export default function App() {
  const {
    ready,
    stocks,
    stockCount,
    updateCount,
    updatesPerSec,
    inserting,
    subscribe,
    unsubscribe,
    startUpdates,
    stopUpdates,
    insertStocks,
  } = useDbWorker()

  const [liveEnabled, setLiveEnabled] = useState(false)
  const [autoUpdate, setAutoUpdate] = useState(false)
  const [batchSize, setBatchSize] = useState('10')
  const [highlightedCells, setHighlightedCells] = useState<Set<CellKey>>(new Set())
  const [perfResults, setPerfResults] = useState<PerfResult[] | null>(null)
  const [perfRunning, setPerfRunning] = useState(false)
  const prevStocksRef = useRef<Map<number, Stock>>(new Map())

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

  // Detect changes when stocks update
  useEffect(() => {
    if (stocks.length > 0) {
      detectChanges(stocks)
    }
  }, [stocks, detectChanges])

  // Handle live query toggle
  useEffect(() => {
    if (liveEnabled) {
      subscribe(DISPLAY_LIMIT)
    } else {
      unsubscribe()
    }
  }, [liveEnabled, subscribe, unsubscribe])

  // Handle auto update toggle
  useEffect(() => {
    if (autoUpdate && liveEnabled) {
      startUpdates(DISPLAY_LIMIT, Number(batchSize))
    } else {
      stopUpdates()
    }
  }, [autoUpdate, liveEnabled, batchSize, startUpdates, stopUpdates])

  if (!ready) {
    return (
      <div className="flex flex-col items-center justify-center min-h-[60vh] gap-4">
        <Loader2 className="w-8 h-8 animate-spin" />
        <p className="text-white/40 text-sm tracking-wider uppercase">Initializing Database (Worker)...</p>
      </div>
    )
  }

  return (
    <div className="container mx-auto px-4 py-8 max-w-7xl">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center gap-3 mb-2">
          <h1 className="text-2xl font-bold tracking-tight uppercase">Live Query Demo</h1>
          <div className={cn(
            "flex items-center gap-2 px-3 py-1 text-xs tracking-widest uppercase border",
            liveEnabled ? "border-white/40 bg-white/5" : "border-white/20"
          )}>
            <span className={cn("w-2 h-2", liveEnabled ? "bg-white animate-pulse" : "bg-white/30")} />
            {liveEnabled ? 'LIVE' : 'OFFLINE'}
          </div>
          <div className="flex items-center gap-2 px-3 py-1 text-xs tracking-widest uppercase border border-white/20 bg-white/5">
            <Cpu className="w-3 h-3" />
            WORKER
          </div>
        </div>
        <p className="text-white/40 text-sm">Real-time stock data with Web Worker architecture</p>
      </div>

      {/* Stats Bar */}
      <div className="grid grid-cols-3 md:grid-cols-6 gap-px mb-8 bg-white/10">
        <div className="bg-background p-4">
          <div className="text-xs text-white/40 uppercase tracking-wider mb-1">Rows</div>
          <div className="text-xl font-mono">{stockCount.toLocaleString()}</div>
        </div>
        <div className="bg-background p-4">
          <div className="text-xs text-white/40 uppercase tracking-wider mb-1">Display</div>
          <div className="text-xl font-mono">{stocks.length}</div>
        </div>
        <div className="bg-background p-4">
          <div className="text-xs text-white/40 uppercase tracking-wider mb-1">Columns</div>
          <div className="text-xl font-mono">{STOCK_COLUMNS.length}</div>
        </div>
        <div className="bg-background p-4">
          <div className="text-xs text-white/40 uppercase tracking-wider mb-1">Updates</div>
          <div className="text-xl font-mono">{updateCount.toLocaleString()}</div>
        </div>
        <div className="bg-background p-4">
          <div className="text-xs text-white/40 uppercase tracking-wider mb-1">UPS</div>
          <div className="text-xl font-mono">{updatesPerSec.toLocaleString()}</div>
        </div>
        <div className="bg-background p-4">
          <div className="text-xs text-white/40 uppercase tracking-wider mb-1">Batch</div>
          <div className="text-xl font-mono">{batchSize}</div>
        </div>
      </div>

      {/* Control Panel */}
      <div className="border border-white/10 mb-8">
        <div className="border-b border-white/10 px-4 py-3">
          <span className="text-xs tracking-widest uppercase text-white/40">// CONTROL PANEL</span>
        </div>
        <div className="p-4 space-y-4">
          {/* Primary Controls */}
          <div className="flex flex-wrap gap-3">
            <Button
              variant={liveEnabled ? 'default' : 'outline'}
              onClick={() => {
                if (liveEnabled) setAutoUpdate(false)
                setLiveEnabled(!liveEnabled)
              }}
              className={cn(
                "uppercase tracking-wider text-xs gap-2",
                liveEnabled && "bg-white text-black hover:bg-white/90"
              )}
            >
              {liveEnabled ? <Square className="w-3 h-3" /> : <Play className="w-3 h-3" />}
              {liveEnabled ? 'STOP QUERY' : 'START QUERY'}
            </Button>

            {liveEnabled && (
              <>
                <Button
                  variant={autoUpdate ? 'default' : 'outline'}
                  onClick={() => setAutoUpdate(!autoUpdate)}
                  className={cn(
                    "uppercase tracking-wider text-xs gap-2",
                    autoUpdate && "bg-white text-black hover:bg-white/90"
                  )}
                >
                  {autoUpdate ? <Square className="w-3 h-3" /> : <Play className="w-3 h-3" />}
                  {autoUpdate ? 'STOP UPDATES' : 'AUTO UPDATE'}
                </Button>

                <Select value={batchSize} onValueChange={setBatchSize} disabled={autoUpdate}>
                  <SelectTrigger className={cn("w-[140px] uppercase text-xs tracking-wider", autoUpdate && "opacity-50")}>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="1">1 / FRAME</SelectItem>
                    <SelectItem value="5">5 / FRAME</SelectItem>
                    <SelectItem value="10">10 / FRAME</SelectItem>
                    <SelectItem value="20">20 / FRAME</SelectItem>
                    <SelectItem value="50">50 / FRAME</SelectItem>
                    <SelectItem value="100">100 / FRAME</SelectItem>
                    <SelectItem value="200">200 / FRAME</SelectItem>
                  </SelectContent>
                </Select>
              </>
            )}
          </div>

          {/* Secondary Controls */}
          <div className="flex flex-wrap gap-3">
            <Button
              variant="outline"
              onClick={() => insertStocks(100000)}
              disabled={inserting}
              className="uppercase tracking-wider text-xs gap-2"
            >
              {inserting ? <Loader2 className="w-3 h-3 animate-spin" /> : <Database className="w-3 h-3" />}
              {inserting ? 'INSERTING...' : '+100K ROWS'}
            </Button>

            <Button
              variant="outline"
              onClick={async () => {
                setPerfRunning(true)
                const { runPerfTest } = await import('./perf-test')
                const results = await runPerfTest()
                setPerfResults(results)
                setPerfRunning(false)
              }}
              disabled={perfRunning}
              className="uppercase tracking-wider text-xs gap-2"
            >
              <Zap className="w-3 h-3" />
              {perfRunning ? 'RUNNING...' : 'PERF TEST'}
            </Button>
          </div>

          {/* Status */}
          {liveEnabled && autoUpdate && (
            <div className="flex items-center gap-3 px-4 py-3 border border-white/20 bg-white/[0.02] text-sm">
              <span className="w-2 h-2 bg-white animate-pulse" />
              <span className="text-white/60">
                CONTINUOUS: {batchSize} rows/frame · {updatesPerSec.toLocaleString()} updates/sec
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Data Table - Using native table for better performance */}
      <div className="border border-white/10">
        <div className="border-b border-white/10 px-4 py-3 flex items-center justify-between">
          <span className="text-xs tracking-widest uppercase text-white/40">// DATA STREAM</span>
          <span className="text-xs text-white/30">{stocks.length} RECORDS</span>
        </div>
        <div className="max-h-[600px] overflow-auto">
          <table className="w-full">
            <thead className="sticky top-0 bg-background">
              <tr className="border-b border-white/10">
                {STOCK_COLUMNS.map(col => (
                  <th
                    key={col}
                    className={cn(
                      "text-[10px] uppercase tracking-wider font-normal text-white/40 whitespace-nowrap py-3 px-4",
                      col === 'id' || col === 'symbol' || col === 'name' || col === 'sector' ? 'text-left' : 'text-right'
                    )}
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {stocks.map(stock => (
                <StockRow key={stock.id} stock={stock} highlightedCells={highlightedCells} />
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Perf Test Results Modal */}
      <Dialog open={perfResults !== null} onOpenChange={() => setPerfResults(null)}>
        <DialogContent className="w-full max-w-3xl" onClose={() => setPerfResults(null)}>
          <DialogHeader>
            <DialogTitle>Performance Test Results</DialogTitle>
          </DialogHeader>
          <div className="p-6 space-y-6">
            {perfResults && (() => {
              const rawTests = perfResults.filter(r => r.name.includes('Raw'))
              const noSubTests = perfResults.filter(r => r.name.includes('no subscribe'))
              const liveTests = perfResults.filter(r => r.name.includes('Live Query Update'))

              const renderGroup = (title: string, tests: PerfResult[]) => (
                <div key={title}>
                  <h3 className="text-xs uppercase tracking-widest text-white/40 mb-3">// {title}</h3>
                  <div className="border border-white/10">
                    <table className="w-full">
                      <thead>
                        <tr className="border-b border-white/10 text-[10px] uppercase tracking-wider text-white/40">
                          <th className="text-left py-2 px-3">Rows</th>
                          <th className="text-left py-2 px-3">Batch</th>
                          <th className="text-right py-2 px-3">Updates/sec</th>
                          <th className="text-right py-2 px-3">Avg (ms)</th>
                          {tests.some(t => t.notifications !== undefined) && (
                            <th className="text-right py-2 px-3">Notifications</th>
                          )}
                        </tr>
                      </thead>
                      <tbody>
                        {tests.map((r, i) => (
                          <tr key={i} className="border-b border-white/5 hover:bg-white/[0.02]">
                            <td className="py-2 px-3 font-mono text-sm">{r.totalRows.toLocaleString()}</td>
                            <td className="py-2 px-3 font-mono text-sm">{r.batchSize}</td>
                            <td className="py-2 px-3 font-mono text-sm text-right">{r.updatesPerSec.toFixed(0)}</td>
                            <td className="py-2 px-3 font-mono text-sm text-right text-white/50">{r.avgUpdateMs.toFixed(3)}</td>
                            {tests.some(t => t.notifications !== undefined) && (
                              <td className="py-2 px-3 font-mono text-sm text-right text-white/50">{r.notifications ?? '-'}</td>
                            )}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )

              return (
                <>
                  {rawTests.length > 0 && renderGroup('Raw Update (No Live Query)', rawTests)}
                  {noSubTests.length > 0 && renderGroup('Live Query (No Subscribe)', noSubTests)}
                  {liveTests.length > 0 && renderGroup('Live Query with Subscribe', liveTests)}
                </>
              )
            })()}
          </div>
          <div className="border-t border-white/10 px-6 py-4 flex justify-end">
            <Button variant="outline" onClick={() => setPerfResults(null)} className="uppercase tracking-wider text-xs">
              Close
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
