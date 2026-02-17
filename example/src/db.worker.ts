/**
 * Database Worker
 *
 * Runs the entire database in a Web Worker to avoid blocking the main thread.
 * All updates and Live Query re-queries happen here.
 */

import {
  initCynos,
  createDatabase,
  JsDataType,
  ColumnOptions,
  col,
  type Database,
} from '@cynos/core'

export type Stock = {
  id: number
  symbol: string
  name: string
  price: number
  change: number
  changePercent: number
  volume: number
  high: number
  low: number
  open: number
  marketCap: number
  pe: number
  sector: string
}

let db: Database | null = null
let stockCount = 0

const SECTORS = ['Technology', 'Finance', 'Healthcare', 'Consumer', 'Energy', 'Industrial', 'Materials', 'Utilities', 'Real Estate', 'Telecom']
const PREFIXES = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
const SUFFIXES = ['Corp', 'Inc', 'Ltd', 'Group', 'Holdings', 'Tech', 'Systems', 'Solutions', 'Global', 'International']

function generateSymbol(id: number): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
  let symbol = ''
  let n = id
  for (let i = 0; i < 4; i++) {
    symbol = chars[n % 26] + symbol
    n = Math.floor(n / 26)
  }
  return symbol
}

function generateCompanyName(id: number): string {
  const prefix = PREFIXES[id % PREFIXES.length]
  const suffix = SUFFIXES[Math.floor(id / PREFIXES.length) % SUFFIXES.length]
  return `${prefix}${id} ${suffix}`
}

function generateStock(id: number): Stock {
  const basePrice = 10 + Math.random() * 990
  const change = (Math.random() - 0.5) * basePrice * 0.1
  return {
    id,
    symbol: generateSymbol(id),
    name: generateCompanyName(id),
    price: Math.round(basePrice * 100) / 100,
    change: Math.round(change * 100) / 100,
    changePercent: Math.round((change / basePrice) * 10000) / 100,
    volume: Math.floor(Math.random() * 100000000),
    high: Math.round((basePrice + Math.abs(change) + Math.random() * 10) * 100) / 100,
    low: Math.round((basePrice - Math.abs(change) - Math.random() * 10) * 100) / 100,
    open: Math.round((basePrice + (Math.random() - 0.5) * 20) * 100) / 100,
    marketCap: Math.floor(Math.random() * 3000000000000),
    pe: Math.round((5 + Math.random() * 95) * 10) / 10,
    sector: SECTORS[id % SECTORS.length],
  }
}

const numericFields: (keyof Stock)[] = ['price', 'change', 'changePercent', 'volume', 'high', 'low', 'pe']

function randomValue(field: keyof Stock): number {
  switch (field) {
    case 'price': return Math.round((10 + Math.random() * 990) * 100) / 100
    case 'change': return Math.round((Math.random() - 0.5) * 100 * 100) / 100
    case 'changePercent': return Math.round((Math.random() - 0.5) * 10 * 100) / 100
    case 'volume': return Math.floor(Math.random() * 100000000)
    case 'high':
    case 'low': return Math.round((10 + Math.random() * 990) * 100) / 100
    case 'pe': return Math.round((5 + Math.random() * 95) * 10) / 10
    default: return 0
  }
}

// Message types
export type WorkerMessage =
  | { type: 'init' }
  | { type: 'subscribe'; limit: number }
  | { type: 'unsubscribe' }
  | { type: 'startUpdates'; displayLimit: number; batchSize: number }
  | { type: 'stopUpdates' }
  | { type: 'insertStocks'; count: number }

export type MainMessage =
  | { type: 'ready'; stockCount: number }
  | { type: 'data'; stocks: Stock[] }
  | { type: 'binaryData'; buffer: ArrayBuffer }
  | { type: 'schemaLayout'; layout: SerializedSchemaLayout }
  | { type: 'stats'; updates: number; elapsed: number; ups: number }
  | { type: 'insertComplete'; count: number; time: number; stockCount: number }
  | { type: 'error'; message: string }

// Serializable schema layout for transfer to main thread
export type SerializedSchemaLayout = {
  columnCount: number
  columnNames: string[]
  columnTypes: number[]
  columnOffsets: number[]
  nullMaskSize: number
}

function postToMain(msg: MainMessage, transfer?: Transferable[]) {
  if (transfer) {
    ;(self.postMessage as (message: MainMessage, transfer: Transferable[]) => void)(msg, transfer)
  } else {
    self.postMessage(msg)
  }
}

let unsubscribe: (() => void) | null = null
let updateRunning = false

async function initDatabase() {
  await initCynos()
  db = createDatabase('demo-worker')

  if (!db.tableNames().includes('stocks')) {
    const stocksTable = db.createTable('stocks')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('symbol', JsDataType.String, null)
      .column('name', JsDataType.String, null)
      .column('price', JsDataType.Float64, null)
      .column('change', JsDataType.Float64, null)
      .column('changePercent', JsDataType.Float64, null)
      .column('volume', JsDataType.Int64, null)
      .column('high', JsDataType.Float64, null)
      .column('low', JsDataType.Float64, null)
      .column('open', JsDataType.Float64, null)
      .column('marketCap', JsDataType.Int64, null)
      .column('pe', JsDataType.Float64, null)
      .column('sector', JsDataType.String, null)

    db.registerTable(stocksTable)

    const initialStocks = Array.from({ length: 100 }, (_, i) => generateStock(i + 1))
    await db.insert('stocks').values(initialStocks).exec()
    stockCount = 100
  } else {
    stockCount = db.totalRowCount()
  }

  postToMain({ type: 'ready', stockCount })
}

function subscribeToLiveQuery(limit: number) {
  if (!db) return

  // Unsubscribe previous
  if (unsubscribe) {
    unsubscribe()
    unsubscribe = null
  }

  const stream = db.select('*').from('stocks').limit(limit).changes()

  // Get and send schema layout once
  const layout = stream.getSchemaLayout()
  const colCount = layout.columnCount()
  const serializedLayout: SerializedSchemaLayout = {
    columnCount: colCount,
    columnNames: [],
    columnTypes: [],
    columnOffsets: [],
    nullMaskSize: layout.nullMaskSize(),
  }
  for (let i = 0; i < colCount; i++) {
    serializedLayout.columnNames.push(layout.columnName(i) ?? '')
    serializedLayout.columnTypes.push(layout.columnType(i) ?? 0)
    serializedLayout.columnOffsets.push(layout.columnOffset(i) ?? 0)
  }
  postToMain({ type: 'schemaLayout', layout: serializedLayout })

  // Throttle postMessage to avoid overwhelming main thread
  let sendScheduled = false

  const unsub = stream.subscribe(() => {
    if (!sendScheduled) {
      sendScheduled = true
      setTimeout(() => {
        // Get binary result and transfer it
        const binaryResult = stream.getResultBinary()
        const view = binaryResult.asView()
        // Copy to transferable ArrayBuffer
        const buffer = view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength)
        binaryResult.free()

        postToMain({ type: 'binaryData', buffer }, [buffer])
        sendScheduled = false
      }, 0)
    }
  })

  unsubscribe = () => unsub()
}

function unsubscribeFromLiveQuery() {
  if (unsubscribe) {
    unsubscribe()
    unsubscribe = null
  }
}

async function startContinuousUpdates(displayLimit: number, batchSize: number) {
  if (!db || updateRunning) return

  updateRunning = true
  let totalUpdates = 0
  const startTime = performance.now()
  let lastReportTime = startTime
  let loopCount = 0

  async function updateLoop() {
    if (!updateRunning || !db) return

    // Run updates in tight batches
    for (let i = 0; i < batchSize && updateRunning; i++) {
      const id = Math.floor(Math.random() * displayLimit) + 1
      const field = numericFields[Math.floor(Math.random() * numericFields.length)]
      await db.update('stocks').set(field, randomValue(field)).where(col('id').eq(id)).exec()
      totalUpdates++
    }
    loopCount++

    // Report stats periodically
    const now = performance.now()
    if (now - lastReportTime >= 100) {
      const elapsed = now - startTime
      const ups = totalUpdates / (elapsed / 1000)
      postToMain({ type: 'stats', updates: totalUpdates, elapsed, ups })
      lastReportTime = now
    }

    // Yield to event loop every 3 batches for smoother UI updates
    if (updateRunning) {
      if (loopCount % 3 === 0) {
        setTimeout(updateLoop, 0)
      } else {
        queueMicrotask(updateLoop)
      }
    }
  }

  updateLoop()
}

function stopUpdates() {
  updateRunning = false
}

async function insertStocks(count: number) {
  if (!db) return

  const start = performance.now()
  const batchSize = 10000
  let inserted = 0

  for (let i = 0; i < count; i += batchSize) {
    const batch = Math.min(batchSize, count - i)
    const stocks = Array.from({ length: batch }, (_, j) => generateStock(stockCount + i + j + 1))
    await db.insert('stocks').values(stocks).exec()
    inserted += batch
  }

  stockCount += count
  const time = performance.now() - start
  postToMain({ type: 'insertComplete', count: inserted, time, stockCount })
}

// Handle messages from main thread
self.onmessage = async (e: MessageEvent<WorkerMessage>) => {
  const msg = e.data

  try {
    switch (msg.type) {
      case 'init':
        await initDatabase()
        break
      case 'subscribe':
        subscribeToLiveQuery(msg.limit)
        break
      case 'unsubscribe':
        unsubscribeFromLiveQuery()
        break
      case 'startUpdates':
        await startContinuousUpdates(msg.displayLimit, msg.batchSize)
        break
      case 'stopUpdates':
        stopUpdates()
        break
      case 'insertStocks':
        await insertStocks(msg.count)
        break
    }
  } catch (err) {
    postToMain({ type: 'error', message: String(err) })
  }
}
