/**
 * useQueryWorker - Hook for Query Builder to interact with the database worker
 */

import { useEffect, useRef, useState, useCallback } from 'react'
import type { Stock } from './db'
import type { WorkerMessage, MainMessage, WhereClause } from './db.worker'

export type { WhereClause }

export function useQueryWorker() {
  const workerRef = useRef<Worker | null>(null)
  const [ready, setReady] = useState(false)
  const [stockCount, setStockCount] = useState(0)
  const [executing, setExecuting] = useState(false)
  const [results, setResults] = useState<Stock[]>([])
  const [execTime, setExecTime] = useState<number | null>(null)
  const [affectedRows, setAffectedRows] = useState<number | null>(null)
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
          setAffectedRows(msg.affectedRows)
          setStockCount(msg.stockCount)
          setExecuting(false)
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
    workerRef.current?.postMessage({
      type: 'querySelect',
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
    setAffectedRows(null)
    setError(null)
  }, [])

  return {
    ready,
    stockCount,
    executing,
    results,
    execTime,
    affectedRows,
    error,
    querySelect,
    queryInsert,
    queryUpdate,
    queryDelete,
    clearResults,
  }
}
