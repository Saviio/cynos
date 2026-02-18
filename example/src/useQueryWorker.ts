/**
 * useQueryWorker - Hook for Query Builder to interact with the database worker
 */

import { useEffect, useRef, useState, useCallback } from 'react'
import type { Stock } from './db'
import type { WorkerMessage, MainMessage, WhereClause } from './db.worker'
import { createDecoder } from './binary-decoder'

export type { WhereClause }

export function useQueryWorker() {
  const workerRef = useRef<Worker | null>(null)
  const decoderRef = useRef<{ key: string; decode: (buffer: ArrayBuffer) => Stock[] } | null>(null)
  const requestStartRef = useRef<number>(0)
  const [ready, setReady] = useState(false)
  const [stockCount, setStockCount] = useState(0)
  const [executing, setExecuting] = useState(false)
  const [results, setResults] = useState<Stock[]>([])
  const [execTime, setExecTime] = useState<number | null>(null)
  const [latencyTime, setLatencyTime] = useState<number | null>(null)
  const [decodeTime, setDecodeTime] = useState<number | null>(null)
  const [affectedRows, setAffectedRows] = useState<number | null>(null)
  const [explainPlan, setExplainPlan] = useState<{ logical: string; optimized: string; physical: string } | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const worker = new Worker(
      new URL('./db.worker.ts', import.meta.url),
      { type: 'module' }
    )
    workerRef.current = worker

    worker.onmessage = (e: MessageEvent<MainMessage>) => {
      const msg = e.data

      switch (msg.type) {
        case 'ready':
          setStockCount(msg.stockCount)
          setReady(true)
          break

        case 'stockCount':
          setStockCount(msg.count)
          break

        case 'queryResult':
          setResults(msg.stocks)
          setExecTime(msg.execTime)
          setLatencyTime(null)
          setDecodeTime(null)
          setAffectedRows(msg.affectedRows)
          setStockCount(msg.stockCount)
          setExecuting(false)
          break

        case 'queryBinaryResult': {
          // Decode on main thread using pure JS decoder
          const decodeStart = performance.now()

          // Create decoder if layout changed (keyed by column names)
          const layoutKey = msg.layout.columnNames.join(',')
          if (!decoderRef.current || decoderRef.current.key !== layoutKey) {
            decoderRef.current = { key: layoutKey, decode: createDecoder(msg.layout) }
          }
          const stocks = decoderRef.current.decode(msg.buffer)
          const decode = performance.now() - decodeStart

          // Calculate latency: total round-trip time - query time - decode time
          const totalTime = performance.now() - requestStartRef.current
          const latency = totalTime - msg.queryTime - decode

          setResults(stocks)
          setExecTime(msg.queryTime)
          setLatencyTime(latency)
          setDecodeTime(decode)
          setAffectedRows(stocks.length)
          setStockCount(msg.stockCount)
          setExecuting(false)
          break
        }

        case 'queryExplainResult':
          setExplainPlan(msg.plan)
          break

        case 'error':
          setError(msg.message)
          setExecuting(false)
          break
      }
    }

    worker.postMessage({ type: 'init' } as WorkerMessage)

    return () => {
      worker.terminate()
    }
  }, [])

  const querySelect = useCallback((
    fields: (keyof Stock)[],
    where: WhereClause[],
    orderBy?: { field: keyof Stock; dir: 'Asc' | 'Desc' },
    limit?: number
  ) => {
    setExecuting(true)
    setError(null)
    setResults([])
    setAffectedRows(null)
    requestStartRef.current = performance.now()
    workerRef.current?.postMessage({
      type: 'querySelect',
      fields,
      where,
      orderBy,
      limit
    } as WorkerMessage)
  }, [])

  const queryExplain = useCallback((
    fields: (keyof Stock)[],
    where: WhereClause[],
    orderBy?: { field: keyof Stock; dir: 'Asc' | 'Desc' },
    limit?: number
  ) => {
    workerRef.current?.postMessage({
      type: 'queryExplain',
      fields,
      where,
      orderBy,
      limit
    } as WorkerMessage)
  }, [])

  const queryInsert = useCallback((count: number) => {
    setExecuting(true)
    setError(null)
    setResults([])
    setAffectedRows(null)
    workerRef.current?.postMessage({
      type: 'queryInsert',
      count
    } as WorkerMessage)
  }, [])

  const queryUpdate = useCallback((
    field: keyof Stock,
    value: string | number,
    where: WhereClause[]
  ) => {
    setExecuting(true)
    setError(null)
    setResults([])
    setAffectedRows(null)
    workerRef.current?.postMessage({
      type: 'queryUpdate',
      field,
      value,
      where
    } as WorkerMessage)
  }, [])

  const queryDelete = useCallback((where: WhereClause[]) => {
    setExecuting(true)
    setError(null)
    setResults([])
    setAffectedRows(null)
    workerRef.current?.postMessage({
      type: 'queryDelete',
      where
    } as WorkerMessage)
  }, [])

  const clearResults = useCallback(() => {
    setResults([])
    setExecTime(null)
    setLatencyTime(null)
    setDecodeTime(null)
    setAffectedRows(null)
    setExplainPlan(null)
    setError(null)
  }, [])

  return {
    ready,
    stockCount,
    executing,
    results,
    execTime,
    latencyTime,
    decodeTime,
    affectedRows,
    explainPlan,
    error,
    querySelect,
    queryExplain,
    queryInsert,
    queryUpdate,
    queryDelete,
    clearResults,
  }
}
