/**
 * LovefieldComparison - Side-by-side comparison of Cynos vs Lovefield
 */

import { useState, useEffect, useCallback, useRef } from 'react'
import { useDbWorker } from './useDbWorker'
import { useLovefieldWorker } from './useLovefieldWorker'
import { STOCK_COLUMNS, type Stock } from './db'
import { Button } from '@/components/ui/button'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Loader2, Play, Pause, Square } from 'lucide-react'
import { cn } from '@/lib/utils'

type CellKey = `${number}-${string}`

const DISPLAY_LIMIT = 100
const VISIBLE_COLUMNS: (keyof Stock)[] = ['id', 'symbol', 'name', 'price', 'change', 'changePercent']

interface DatabasePanelProps {
  name: string
  badge: string
  badgeColor: string
  ready: boolean
  stocks: Stock[]
  stockCount: number
  updateCount: number
  updatesPerSec: number
  subscribe: (limit: number) => void
  unsubscribe: () => void
  startUpdates: (displayLimit: number, batchSize: number) => void
  stopUpdates: () => void
}

function DatabasePanel({
  name,
  badge,
  badgeColor,
  ready,
  stocks,
  stockCount,
  updateCount,
  updatesPerSec,
  subscribe,
  unsubscribe,
  startUpdates,
  stopUpdates,
}: DatabasePanelProps) {
  const [liveEnabled, setLiveEnabled] = useState(false)
  const [autoUpdate, setAutoUpdate] = useState(false)
  const [batchSize, setBatchSize] = useState('10')
  const [highlightedCells, setHighlightedCells] = useState<Set<CellKey>>(new Set())
  const prevStocksRef = useRef<Map<number, Stock>>(new Map())

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

  useEffect(() => {
    if (stocks.length > 0) {
      detectChanges(stocks)
    }
  }, [stocks, detectChanges])

  useEffect(() => {
    if (liveEnabled) {
      subscribe(DISPLAY_LIMIT)
    } else {
      unsubscribe()
    }
  }, [liveEnabled, subscribe, unsubscribe])

  useEffect(() => {
    if (autoUpdate && liveEnabled) {
      startUpdates(DISPLAY_LIMIT, Number(batchSize))
    } else {
      stopUpdates()
    }
  }, [autoUpdate, liveEnabled, batchSize, startUpdates, stopUpdates])

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

  if (!ready) {
    return (
      <div className="border border-white/10 p-6 flex flex-col items-center justify-center min-h-[400px] gap-3">
        <Loader2 className="w-6 h-6 animate-spin text-white/40" />
        <p className="text-white/40 text-xs tracking-wider uppercase">Initializing {name}...</p>
      </div>
    )
  }

  return (
    <div className="border border-white/10 min-w-0">
      {/* Header */}
      <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3 flex items-center justify-between">
        <div className="flex items-center gap-2 sm:gap-3">
          <span className="text-xs sm:text-sm font-bold tracking-wider uppercase">{name}</span>
          <span
            className="text-[10px] px-1.5 py-0.5 tracking-wider uppercase"
            style={{ background: badgeColor, color: '#fff' }}
          >
            {badge}
          </span>
          <div className={cn(
            "flex items-center gap-1.5 px-2 py-0.5 text-[10px] tracking-widest uppercase border",
            liveEnabled ? "border-white/40 bg-white/5" : "border-white/20"
          )}>
            <span className={cn("w-1.5 h-1.5", liveEnabled ? "bg-white animate-pulse" : "bg-white/30")} />
            {liveEnabled ? 'LIVE' : 'OFF'}
          </div>
        </div>
      </div>

      {/* Stats */}
      <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3 flex flex-wrap gap-3 sm:gap-6 text-[10px] sm:text-xs">
        <div className="flex items-center gap-1.5">
          <span className="text-white/40">ROWS</span>
          <span className="font-mono font-bold">{stockCount.toLocaleString()}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-white/40">UPDATES</span>
          <span className="font-mono font-bold">{updateCount.toLocaleString()}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-white/40">UPS</span>
          <span className="font-mono font-bold text-sm sm:text-base">{updatesPerSec.toLocaleString()}</span>
        </div>
      </div>

      {/* Controls */}
      <div className="border-b border-white/10 px-3 sm:px-4 py-2 sm:py-3 flex flex-wrap gap-2">
        <Button
          variant={liveEnabled ? 'default' : 'outline'}
          size="sm"
          onClick={() => {
            if (liveEnabled) setAutoUpdate(false)
            setLiveEnabled(!liveEnabled)
          }}
          className={cn(
            "text-[10px] sm:text-xs uppercase tracking-wider gap-1.5",
            liveEnabled && "bg-white text-black hover:bg-white/90"
          )}
        >
          {liveEnabled ? <Square className="w-3 h-3" /> : <Play className="w-3 h-3" />}
          {liveEnabled ? 'STOP' : 'START'}
        </Button>

        {liveEnabled && (
          <>
            <Button
              variant={autoUpdate ? 'default' : 'outline'}
              size="sm"
              onClick={() => setAutoUpdate(!autoUpdate)}
              className={cn(
                "text-[10px] sm:text-xs uppercase tracking-wider gap-1.5",
                autoUpdate && "bg-white text-black hover:bg-white/90"
              )}
            >
              {autoUpdate ? <Pause className="w-3 h-3" /> : <Play className="w-3 h-3" />}
              {autoUpdate ? 'PAUSE' : 'UPDATE'}
            </Button>

            <Select value={batchSize} onValueChange={setBatchSize}>
              <SelectTrigger className="w-[90px] sm:w-[100px] text-[10px] sm:text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="1">1/batch</SelectItem>
                <SelectItem value="5">5/batch</SelectItem>
                <SelectItem value="10">10/batch</SelectItem>
                <SelectItem value="20">20/batch</SelectItem>
                <SelectItem value="50">50/batch</SelectItem>
                <SelectItem value="100">100/batch</SelectItem>
              </SelectContent>
            </Select>
          </>
        )}
      </div>

      {/* Table */}
      <div className="max-h-[300px] sm:max-h-[350px] overflow-auto">
        <table className="w-full">
          <thead className="sticky top-0 bg-background">
            <tr className="border-b border-white/10">
              {VISIBLE_COLUMNS.map(col => (
                <th
                  key={col}
                  className="text-[8px] sm:text-[10px] uppercase tracking-wider font-normal text-white/40 whitespace-nowrap py-2 px-2 sm:px-3 text-left"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {stocks.slice(0, 20).map(stock => (
              <tr key={stock.id} className="border-b border-white/5 hover:bg-white/[0.02]">
                {VISIBLE_COLUMNS.map(col => (
                  <td
                    key={col}
                    className={cn(
                      "font-mono text-[10px] sm:text-xs whitespace-nowrap py-1.5 px-2 sm:px-3",
                      col === 'id' && 'text-white/30',
                      col === 'symbol' && 'font-medium',
                      col === 'name' && 'text-white/50',
                      (col === 'change' || col === 'changePercent') && (stock.change >= 0 ? 'text-white' : 'text-white/50'),
                      highlightedCells.has(`${stock.id}-${col}`) && 'animate-cell-flash'
                    )}
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
  )
}

export default function LovefieldComparison() {
  const cynos = useDbWorker()
  const lovefield = useLovefieldWorker()

  return (
    <div className="container mx-auto px-4 py-6 sm:py-8 max-w-7xl">
      {/* Header */}
      <div className="mb-6 sm:mb-8">
        <h1 className="text-xl sm:text-2xl font-bold tracking-tight uppercase mb-2">
          Cynos vs LF
        </h1>
        <p className="text-white/40 text-xs sm:text-sm">
          Worker-based performance comparison · Live Query throughput test
        </p>
      </div>

      {/* Comparison Grid */}
      <div className="grid lg:grid-cols-2 gap-4 sm:gap-6">
        <DatabasePanel
          name="Cynos"
          badge="WASM"
          badgeColor="#7c3aed"
          ready={cynos.ready}
          stocks={cynos.stocks}
          stockCount={cynos.stockCount}
          updateCount={cynos.updateCount}
          updatesPerSec={cynos.updatesPerSec}
          subscribe={cynos.subscribe}
          unsubscribe={cynos.unsubscribe}
          startUpdates={cynos.startUpdates}
          stopUpdates={cynos.stopUpdates}
        />
        <DatabasePanel
          name="LF"
          badge="JS"
          badgeColor="#059669"
          ready={lovefield.ready}
          stocks={lovefield.stocks}
          stockCount={lovefield.stockCount}
          updateCount={lovefield.updateCount}
          updatesPerSec={lovefield.updatesPerSec}
          subscribe={lovefield.subscribe}
          unsubscribe={lovefield.unsubscribe}
          startUpdates={lovefield.startUpdates}
          stopUpdates={lovefield.stopUpdates}
        />
      </div>

      {/* Instructions */}
      <div className="mt-6 sm:mt-8 border border-white/10 px-3 sm:px-4 py-2 sm:py-3">
        <p className="text-[10px] sm:text-xs text-white/40">
          <span className="text-white/60 font-bold">HOW TO TEST:</span>{' '}
          Click "Start" on both panels to enable Live Query, then click "Update" to begin continuous updates.
          Compare the UPS (Updates Per Second) metric to see the performance difference.
        </p>
      </div>
    </div>
  )
}
