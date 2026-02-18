import {
  initCynos,
  createDatabase,
  JsDataType,
  ColumnOptions,
  col,
  ResultSet,
  type Database,
  type JsObservableQuery,
  type JsIvmObservableQuery,
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

export const STOCK_COLUMNS: (keyof Stock)[] = [
  'id', 'symbol', 'name', 'price', 'change', 'changePercent',
  'volume', 'high', 'low', 'open', 'marketCap', 'pe', 'sector'
]

let db: Database | null = null
let dbPromise: Promise<Database> | null = null
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

export async function getDatabase(): Promise<Database> {
  if (db) return db
  if (dbPromise) return dbPromise

  dbPromise = (async () => {
    await initCynos()
    const database = createDatabase('demo')

    if (!database.tableNames().includes('stocks')) {
      const stocksTable = database.createTable('stocks')
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
        .index('idx_price', 'price')
        .index('idx_symbol', 'symbol')
        .index('idx_sector', 'sector')

      database.registerTable(stocksTable)

      // Insert initial 100 stocks
      const initialStocks = Array.from({ length: 100 }, (_, i) => generateStock(i + 1))
      await database.insert('stocks').values(initialStocks).exec()
      stockCount = 100
    } else {
      stockCount = database.totalRowCount()
    }

    db = database
    return database
  })()

  return dbPromise
}

export function getStockCount(): number {
  return stockCount
}

export async function insertStocks(count: number): Promise<number> {
  const database = await getDatabase()
  const batchSize = 10000
  let inserted = 0

  for (let i = 0; i < count; i += batchSize) {
    const batch = Math.min(batchSize, count - i)
    const stocks = Array.from({ length: batch }, (_, j) => generateStock(stockCount + i + j + 1))
    await database.insert('stocks').values(stocks).exec()
    inserted += batch
  }

  stockCount += count
  return inserted
}

export async function getAllStocks(): Promise<Stock[]> {
  const database = await getDatabase()
  return database.select('*').from('stocks').exec() as Promise<Stock[]>
}

export async function getStocksPage(offset: number, limit: number): Promise<Stock[]> {
  const database = await getDatabase()
  return database.select('*').from('stocks').offset(offset).limit(limit).exec() as Promise<Stock[]>
}

export async function updateStock(id: number, field: keyof Stock, value: number): Promise<void> {
  const database = await getDatabase()
  await database.update('stocks').set(field, value).where(col('id').eq(id)).exec()
}

export async function createStockLiveQuery(limit?: number): Promise<JsObservableQuery> {
  const database = await getDatabase()
  let query = database.select('*').from('stocks')
  if (limit) {
    query = query.limit(limit)
  }
  return query.observe()
}

// Fast random update - only updates within display limit for visibility
// batchSize controls how many rows to update per call
export async function randomStockUpdate(displayLimit?: number, batchSize: number = 1): Promise<void> {
  const database = await getDatabase()
  const maxId = displayLimit ? Math.min(displayLimit, stockCount) : stockCount
  const numericFields: (keyof Stock)[] = ['price', 'change', 'changePercent', 'volume', 'high', 'low', 'pe']

  for (let i = 0; i < batchSize; i++) {
    const id = Math.floor(Math.random() * maxId) + 1
    const field = numericFields[Math.floor(Math.random() * numericFields.length)]

    let newValue: number
    switch (field) {
      case 'price':
        newValue = Math.round((10 + Math.random() * 990) * 100) / 100
        break
      case 'change':
        newValue = Math.round((Math.random() - 0.5) * 100 * 100) / 100
        break
      case 'changePercent':
        newValue = Math.round((Math.random() - 0.5) * 10 * 100) / 100
        break
      case 'volume':
        newValue = Math.floor(Math.random() * 100000000)
        break
      case 'high':
      case 'low':
        newValue = Math.round((10 + Math.random() * 990) * 100) / 100
        break
      case 'pe':
        newValue = Math.round((5 + Math.random() * 95) * 10) / 10
        break
      default:
        newValue = 0
    }

    await database.update('stocks').set(field, newValue).where(col('id').eq(id)).exec()
  }
}

// Binary protocol demo
export async function queryStocksWithBinary(limit?: number): Promise<{ data: Stock[], time: number }> {
  const database = await getDatabase()
  const start = performance.now()
  let query = database.select('*').from('stocks')
  if (limit) query = query.limit(limit)
  const layout = query.getSchemaLayout()
  const binaryResult = await query.execBinary()
  const rs = new ResultSet(binaryResult, layout)
  const data = rs.toArray() as Stock[]
  rs.free()
  const time = performance.now() - start
  return { data, time }
}

export async function queryStocksWithJson(limit?: number): Promise<{ data: Stock[], time: number }> {
  const database = await getDatabase()
  const start = performance.now()
  let query = database.select('*').from('stocks')
  if (limit) query = query.limit(limit)
  const data = await query.exec() as Stock[]
  const time = performance.now() - start
  return { data, time }
}

// Continuous update loop for maximum throughput
// Returns a stop function
export function startContinuousUpdates(
  displayLimit: number,
  batchSize: number,
  onStats: (stats: { updates: number, elapsed: number, ups: number }) => void
): () => void {
  let running = true
  let totalUpdates = 0
  const startTime = performance.now()
  let lastReportTime = startTime

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

  async function updateLoop() {
    if (!running || !db) return

    // Tight loop - do many updates before yielding
    const loopStart = performance.now()
    while (running && performance.now() - loopStart < 16) { // Run for ~16ms (one frame)
      for (let i = 0; i < batchSize && running; i++) {
        const id = Math.floor(Math.random() * displayLimit) + 1
        const field = numericFields[Math.floor(Math.random() * numericFields.length)]
        await db.update('stocks').set(field, randomValue(field)).where(col('id').eq(id)).exec()
        totalUpdates++
      }
    }

    // Report stats periodically
    const now = performance.now()
    if (now - lastReportTime >= 100) { // Update UI every 100ms
      const elapsed = now - startTime
      const ups = totalUpdates / (elapsed / 1000)
      onStats({ updates: totalUpdates, elapsed, ups })
      lastReportTime = now
    }

    // Yield to event loop for UI updates, then continue
    if (running) {
      setTimeout(updateLoop, 0)
    }
  }

  // Start the loop
  getDatabase().then(() => {
    updateLoop()
  })

  return () => { running = false }
}

// IVM-based live query (O(delta) incremental updates)
// Only works with incrementalizable queries (no ORDER BY / LIMIT)
export async function createStockIvmQuery(priceThreshold?: number): Promise<JsIvmObservableQuery> {
  const database = await getDatabase()
  let query = database.select('*').from('stocks')
  if (priceThreshold != null) {
    query = query.where(col('price').gt(priceThreshold))
  }
  return query.trace()
}

// Re-query based live query for comparison (O(result_set) on every change)
export async function createStockReQueryQuery(priceThreshold?: number): Promise<JsObservableQuery> {
  const database = await getDatabase()
  let query = database.select('*').from('stocks')
  if (priceThreshold != null) {
    query = query.where(col('price').gt(priceThreshold))
  }
  return query.observe()
}

// Clear all stocks and reset count
export async function clearAllStocks(): Promise<void> {
  const database = await getDatabase()
  await database.delete('stocks').exec()
  stockCount = 0
}

// Delete stocks keeping only first N rows
export async function deleteStocksKeepFirst(keepCount: number): Promise<void> {
  const database = await getDatabase()
  if (stockCount > keepCount) {
    await database.delete('stocks').where(col('id').gt(keepCount)).exec()
    stockCount = keepCount
  }
}
