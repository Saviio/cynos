/**
 * useDbWorker - Hook to interact with the database worker
 */

import { useEffect, useRef, useState, useCallback } from 'react'
import type { Stock } from './db'
import type { WorkerMessage, MainMessage, SerializedSchemaLayout } from './db.worker'
import { createDecoder } from './binary-decoder'

export function useDbWorker() {
  const workerRef = useRef<Worker | null>(null)
  const [ready, setReady] = useState(false)
  const [stocks, setStocks] = useState<Stock[]>([])
  const [stockCount, setStockCount] = useState(0)
  const [updateCount, setUpdateCount] = useState(0)
  const [updatesPerSec, setUpdatesPerSec] = useState(0)
  const [inserting, setInserting] = useState(false)

  // Throttle UI updates with RAF
  const pendingStocksRef = useRef<Stock[] | null>(null)
  const rafIdRef = useRef<number | null>(null)

  // Binary decoder (created when schema layout is received)
  const decoderRef = useRef<((buffer: ArrayBuffer) => Stock[]) | null>(null)

  useEffect(() => {
    // Create worker
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

        case 'schemaLayout':
          // Create decoder from schema layout
          decoderRef.current = createDecoder(msg.layout)
          break

        case 'data':
          // Throttle with RAF to avoid overwhelming React
          pendingStocksRef.current = msg.stocks
          if (rafIdRef.current === null) {
            rafIdRef.current = requestAnimationFrame(() => {
              if (pendingStocksRef.current) {
                setStocks(pendingStocksRef.current)
              }
              rafIdRef.current = null
            })
          }
          break

        case 'binaryData':
          // Decode binary data and update
          if (decoderRef.current) {
            const decoded = decoderRef.current(msg.buffer)
            pendingStocksRef.current = decoded
            if (rafIdRef.current === null) {
              rafIdRef.current = requestAnimationFrame(() => {
                if (pendingStocksRef.current) {
                  setStocks(pendingStocksRef.current)
                }
                rafIdRef.current = null
              })
            }
          }
          break

        case 'stats':
          setUpdateCount(msg.updates)
          setUpdatesPerSec(Math.round(msg.ups))
          break

        case 'insertComplete':
          setStockCount(msg.stockCount)
          setInserting(false)
          break

        case 'error':
          console.error('Worker error:', msg.message)
          break
      }
    }

    // Initialize database
    worker.postMessage({ type: 'init' } as WorkerMessage)

    return () => {
      if (rafIdRef.current !== null) {
        cancelAnimationFrame(rafIdRef.current)
      }
      worker.terminate()
    }
  }, [])

  const subscribe = useCallback((limit: number) => {
    workerRef.current?.postMessage({ type: 'subscribe', limit } as WorkerMessage)
  }, [])

  const unsubscribe = useCallback(() => {
    workerRef.current?.postMessage({ type: 'unsubscribe' } as WorkerMessage)
  }, [])

  const startUpdates = useCallback((displayLimit: number, batchSize: number) => {
    workerRef.current?.postMessage({ type: 'startUpdates', displayLimit, batchSize } as WorkerMessage)
  }, [])

  const stopUpdates = useCallback(() => {
    workerRef.current?.postMessage({ type: 'stopUpdates' } as WorkerMessage)
  }, [])

  const insertStocks = useCallback((count: number) => {
    setInserting(true)
    workerRef.current?.postMessage({ type: 'insertStocks', count } as WorkerMessage)
  }, [])

  return {
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
  }
}
