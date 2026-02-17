/**
 * High Frequency Update Performance Test
 *
 * Run this in browser console after the app loads:
 * import('/src/perf-test.ts').then(m => m.runPerfTest())
 */

import {
  initCynos,
  createDatabase,
  JsDataType,
  ColumnOptions,
  col,
  type Database,
} from '@cynos/core'

const SECTORS = ['Tech', 'Finance', 'Healthcare', 'Consumer', 'Energy']

function generateStocks(count: number) {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    symbol: `STK${i}`,
    name: `Stock ${i}`,
    price: 100 + Math.random() * 900,
    change: (Math.random() - 0.5) * 50,
    changePercent: (Math.random() - 0.5) * 5,
    volume: Math.floor(Math.random() * 100000000),
    high: 100 + Math.random() * 950,
    low: 50 + Math.random() * 900,
    open: 100 + Math.random() * 900,
    marketCap: Math.floor(Math.random() * 3000000000000),
    pe: 5 + Math.random() * 95,
    sector: SECTORS[i % SECTORS.length],
  }))
}

const numericFields = ['price', 'change', 'changePercent', 'volume', 'high', 'low', 'pe']

function randomField() {
  return numericFields[Math.floor(Math.random() * numericFields.length)]
}

function randomValue(field: string): number {
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

async function createTestDb(name: string): Promise<Database> {
  await initCynos()
  const db = createDatabase(name)

  const stocksBuilder = db.createTable('stocks')
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

  db.registerTable(stocksBuilder)
  return db
}

interface TestResult {
  name: string
  totalRows: number
  batchSize: number
  totalUpdates: number
  durationMs: number
  updatesPerSec: number
  avgUpdateMs: number
  notifications?: number
  canSustainTarget?: boolean
}

const results: TestResult[] = []

async function testRawUpdate(totalRows: number, batchSize: number): Promise<TestResult> {
  const db = await createTestDb(`raw_${totalRows}_${batchSize}_${Date.now()}`)
  await db.insert('stocks').values(generateStocks(totalRows)).exec()

  const displayLimit = 100
  const iterations = 100
  const totalUpdates = iterations * batchSize

  const start = performance.now()
  for (let i = 0; i < iterations; i++) {
    for (let j = 0; j < batchSize; j++) {
      const id = Math.floor(Math.random() * displayLimit) + 1
      const field = randomField()
      await db.update('stocks').set(field, randomValue(field)).where(col('id').eq(id)).exec()
    }
  }
  const duration = performance.now() - start

  return {
    name: 'Raw Update (no live query)',
    totalRows,
    batchSize,
    totalUpdates,
    durationMs: duration,
    updatesPerSec: totalUpdates / (duration / 1000),
    avgUpdateMs: duration / totalUpdates,
  }
}

// Test with live query created but NOT subscribed
async function testLiveQueryNoSubscribe(totalRows: number, batchSize: number): Promise<TestResult> {
  const db = await createTestDb(`livenosub_${totalRows}_${batchSize}_${Date.now()}`)
  await db.insert('stocks').values(generateStocks(totalRows)).exec()

  const displayLimit = 100

  // Create live query but don't subscribe
  db.select('*').from('stocks').limit(displayLimit).observe()

  const iterations = 100
  const totalUpdates = iterations * batchSize

  const start = performance.now()
  for (let i = 0; i < iterations; i++) {
    for (let j = 0; j < batchSize; j++) {
      const id = Math.floor(Math.random() * displayLimit) + 1
      const field = randomField()
      await db.update('stocks').set(field, randomValue(field)).where(col('id').eq(id)).exec()
    }
  }
  const duration = performance.now() - start

  return {
    name: 'Update (live query exists, no subscribe)',
    totalRows,
    batchSize,
    totalUpdates,
    durationMs: duration,
    updatesPerSec: totalUpdates / (duration / 1000),
    avgUpdateMs: duration / totalUpdates,
  }
}

async function testLiveQueryUpdate(totalRows: number, batchSize: number): Promise<TestResult> {
  const db = await createTestDb(`live_${totalRows}_${batchSize}_${Date.now()}`)
  await db.insert('stocks').values(generateStocks(totalRows)).exec()

  const displayLimit = 100
  let notificationCount = 0

  const observable = db.select('*').from('stocks').limit(displayLimit).observe()
  const unsubscribe = observable.subscribe(() => {
    notificationCount++
  })

  const iterations = 100
  const totalUpdates = iterations * batchSize

  const start = performance.now()
  for (let i = 0; i < iterations; i++) {
    for (let j = 0; j < batchSize; j++) {
      const id = Math.floor(Math.random() * displayLimit) + 1
      const field = randomField()
      await db.update('stocks').set(field, randomValue(field)).where(col('id').eq(id)).exec()
    }
  }
  const duration = performance.now() - start

  unsubscribe()

  return {
    name: 'Live Query Update',
    totalRows,
    batchSize,
    totalUpdates,
    durationMs: duration,
    updatesPerSec: totalUpdates / (duration / 1000),
    avgUpdateMs: duration / totalUpdates,
    notifications: notificationCount,
  }
}

export async function runPerfTest() {
  console.log('='.repeat(60))
  console.log('Cynos High Frequency Update Performance Test')
  console.log('='.repeat(60))

  const testConfigs = [
    { rows: 1000, batch: 1 },
    { rows: 1000, batch: 10 },
    { rows: 1000, batch: 50 },
    { rows: 1000, batch: 100 },
    { rows: 100000, batch: 1 },
    { rows: 100000, batch: 10 },
    { rows: 100000, batch: 50 },
    { rows: 100000, batch: 100 },
  ]

  console.log('\n--- Test 1: Raw Update (No Live Query) ---')
  for (const { rows, batch } of testConfigs) {
    const result = await testRawUpdate(rows, batch)
    results.push(result)
    console.log(`[${rows} rows, batch=${batch}] ${result.updatesPerSec.toFixed(0)} updates/sec, avg ${result.avgUpdateMs.toFixed(3)}ms`)
  }

  console.log('\n--- Test 2: Live Query exists but NOT subscribed ---')
  for (const { rows, batch } of testConfigs) {
    const result = await testLiveQueryNoSubscribe(rows, batch)
    results.push(result)
    console.log(`[${rows} rows, batch=${batch}] ${result.updatesPerSec.toFixed(0)} updates/sec, avg ${result.avgUpdateMs.toFixed(3)}ms`)
  }

  console.log('\n--- Test 3: Live Query with subscribe ---')
  for (const { rows, batch } of testConfigs) {
    const result = await testLiveQueryUpdate(rows, batch)
    results.push(result)
    console.log(`[${rows} rows, batch=${batch}] ${result.updatesPerSec.toFixed(0)} updates/sec, avg ${result.avgUpdateMs.toFixed(3)}ms, ${result.notifications} notifications`)
  }

  console.log('\n' + '='.repeat(60))
  console.log('Summary')
  console.log('='.repeat(60))

  return results
}

// Auto-run if imported directly
if (import.meta.hot) {
  console.log('Performance test module loaded. Run: runPerfTest()')
}
