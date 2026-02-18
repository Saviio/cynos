/**
 * Lovefield Database Worker
 *
 * Runs Lovefield database in a Web Worker for comparison with Cynos.
 * Uses classic worker mode since Lovefield doesn't support ES modules.
 */

// Load lovefield via importScripts (classic worker)
declare function importScripts(...urls: string[]): void
importScripts('/lovefield.min.js')

declare const lf: any

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

let db: any = null
let stockTable: any = null
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
  | { type: 'stats'; updates: number; elapsed: number; ups: number }
  | { type: 'insertComplete'; count: number; time: number; stockCount: number }
  | { type: 'error'; message: string }

function postToMain(msg: MainMessage) {
  self.postMessage(msg)
}

let observeQuery: any = null
let currentObserveHandler: ((changes: any[]) => void) | null = null
let updateRunning = false

async function initDatabase() {
  const schemaBuilder = lf.schema.create('lovefield-demo', 1)

  schemaBuilder.createTable('stocks')
    .addColumn('id', lf.Type.INTEGER)
    .addColumn('symbol', lf.Type.STRING)
    .addColumn('name', lf.Type.STRING)
    .addColumn('price', lf.Type.NUMBER)
    .addColumn('change', lf.Type.NUMBER)
    .addColumn('changePercent', lf.Type.NUMBER)
    .addColumn('volume', lf.Type.INTEGER)
    .addColumn('high', lf.Type.NUMBER)
    .addColumn('low', lf.Type.NUMBER)
    .addColumn('open', lf.Type.NUMBER)
    .addColumn('marketCap', lf.Type.INTEGER)
    .addColumn('pe', lf.Type.NUMBER)
    .addColumn('sector', lf.Type.STRING)
    .addPrimaryKey(['id'])

  db = await schemaBuilder.connect({ storeType: lf.schema.DataStoreType.MEMORY })
  stockTable = db.getSchema().table('stocks')

  // Insert initial 100 stocks
  const initialStocks = Array.from({ length: 100 }, (_, i) => {
    const stock = generateStock(i + 1)
    return stockTable.createRow(stock)
  })
  await db.insert().into(stockTable).values(initialStocks).exec()
  stockCount = 100

  postToMain({ type: 'ready', stockCount })
}

function subscribeToQuery(limit: number) {
  if (!db) return

  // Unsubscribe previous
  if (observeQuery && currentObserveHandler) {
    db.unobserve(observeQuery, currentObserveHandler)
    observeQuery = null
    currentObserveHandler = null
  }

  observeQuery = db.select().from(stockTable).limit(limit)

  // Throttle postMessage
  let sendScheduled = false

  currentObserveHandler = function observeHandler(changes: any[]) {
    if (!sendScheduled && changes.length > 0) {
      sendScheduled = true
      setTimeout(() => {
        const fullResults = changes[0]?.object || []
        const stocks: Stock[] = fullResults.map((row: any) => ({
          id: row.id,
          symbol: row.symbol,
          name: row.name,
          price: row.price,
          change: row.change,
          changePercent: row.changePercent,
          volume: row.volume,
          high: row.high,
          low: row.low,
          open: row.open,
          marketCap: row.marketCap,
          pe: row.pe,
          sector: row.sector,
        }))
        postToMain({ type: 'data', stocks })
        sendScheduled = false
      }, 0)
    }
  }

  db.observe(observeQuery, currentObserveHandler)

  // Send initial data
  observeQuery.exec().then((results: any[]) => {
    const stocks: Stock[] = results.map((row: any) => ({
      id: row.id,
      symbol: row.symbol,
      name: row.name,
      price: row.price,
      change: row.change,
      changePercent: row.changePercent,
      volume: row.volume,
      high: row.high,
      low: row.low,
      open: row.open,
      marketCap: row.marketCap,
      pe: row.pe,
      sector: row.sector,
    }))
    postToMain({ type: 'data', stocks })
  })
}

function unsubscribeFromQuery() {
  if (observeQuery && db && currentObserveHandler) {
    db.unobserve(observeQuery, currentObserveHandler)
    observeQuery = null
    currentObserveHandler = null
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

    for (let i = 0; i < batchSize && updateRunning; i++) {
      const id = Math.floor(Math.random() * displayLimit) + 1
      const field = numericFields[Math.floor(Math.random() * numericFields.length)]
      const value = randomValue(field)

      await db.update(stockTable)
        .set(stockTable[field], value)
        .where(stockTable.id.eq(id))
        .exec()

      totalUpdates++
    }
    loopCount++

    const now = performance.now()
    if (now - lastReportTime >= 100) {
      const elapsed = now - startTime
      const ups = totalUpdates / (elapsed / 1000)
      postToMain({ type: 'stats', updates: totalUpdates, elapsed, ups })
      lastReportTime = now
    }

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
    const stocks = Array.from({ length: batch }, (_, j) => {
      const stock = generateStock(stockCount + i + j + 1)
      return stockTable.createRow(stock)
    })
    await db.insert().into(stockTable).values(stocks).exec()
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
        subscribeToQuery(msg.limit)
        break
      case 'unsubscribe':
        unsubscribeFromQuery()
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
