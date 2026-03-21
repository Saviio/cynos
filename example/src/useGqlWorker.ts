import { useState, useEffect, useCallback, useRef } from 'react'
import type { GqlWorkerMessage, GqlMainMessage } from './gql.worker'

export function useGqlWorker() {
  const [ready, setReady] = useState(false)
  const [schema, setSchema] = useState<string | null>(null)
  const [userCount, setUserCount] = useState(0)
  const [postCount, setPostCount] = useState(0)
  const [result, setResult] = useState<unknown>(null)
  const [execTime, setExecTime] = useState<number | null>(null)
  const [subscriptionData, setSubscriptionData] = useState<unknown>(null)
  const [error, setError] = useState<string | null>(null)
  const [executing, setExecuting] = useState(false)

  const workerRef = useRef<Worker | null>(null)

  useEffect(() => {
    const worker = new Worker(new URL('./gql.worker.ts', import.meta.url), { type: 'module' })
    workerRef.current = worker

    worker.onmessage = (e: MessageEvent<GqlMainMessage>) => {
      const msg = e.data

      switch (msg.type) {
        case 'ready':
          setReady(true)
          setUserCount(msg.userCount)
          setPostCount(msg.postCount)
          break
        case 'schema':
          setSchema(msg.sdl)
          break
        case 'result':
          setResult(msg.data)
          setExecTime(msg.execTime)
          setExecuting(false)
          setError(null)
          break
        case 'subscriptionData':
          setSubscriptionData(msg.data)
          break
        case 'counts':
          setUserCount(msg.userCount)
          setPostCount(msg.postCount)
          break
        case 'error':
          setError(msg.message)
          setExecuting(false)
          break
      }
    }

    const initMsg: GqlWorkerMessage = { type: 'init' }
    worker.postMessage(initMsg)

    return () => {
      worker.terminate()
    }
  }, [])

  const getSchema = useCallback(() => {
    if (!workerRef.current) return
    const msg: GqlWorkerMessage = { type: 'getSchema' }
    workerRef.current.postMessage(msg)
  }, [])

  const execute = useCallback((query: string, variables?: Record<string, unknown>) => {
    if (!workerRef.current) return
    setExecuting(true)
    setError(null)
    const msg: GqlWorkerMessage = { type: 'execute', query, variables }
    workerRef.current.postMessage(msg)
  }, [])

  const subscribe = useCallback((query: string) => {
    if (!workerRef.current) return
    setError(null)
    const msg: GqlWorkerMessage = { type: 'subscribe', query }
    workerRef.current.postMessage(msg)
  }, [])

  const unsubscribe = useCallback(() => {
    if (!workerRef.current) return
    const msg: GqlWorkerMessage = { type: 'unsubscribe' }
    workerRef.current.postMessage(msg)
    setSubscriptionData(null)
  }, [])

  const clearResult = useCallback(() => {
    setResult(null)
    setExecTime(null)
    setError(null)
  }, [])

  return {
    ready,
    schema,
    userCount,
    postCount,
    result,
    execTime,
    subscriptionData,
    error,
    executing,
    getSchema,
    execute,
    subscribe,
    unsubscribe,
    clearResult,
  }
}
