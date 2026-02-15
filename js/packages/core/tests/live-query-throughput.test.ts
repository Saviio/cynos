/**
 * Live Query Throughput Test
 *
 * Tests the maximum update throughput for Live Query with subscribe
 * to understand the ~2,500 updates/sec limitation observed in the example app.
 */

import { describe, it, expect, beforeAll } from 'vitest';
import init, {
  Database,
  JsDataType,
  ColumnOptions,
  col,
} from '../wasm/cynos_database.js';

beforeAll(async () => {
  await init();
});

function createStockDb(name: string) {
  const db = new Database(name);
  const builder = db.createTable('stocks')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('symbol', JsDataType.String, null)
    .column('price', JsDataType.Float64, null)
    .column('volume', JsDataType.Int64, null)
    .index('idx_symbol', 'symbol');
  db.registerTable(builder);
  return db;
}

function generateStocks(count: number) {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    symbol: `STK${i}`,
    price: 100 + Math.random() * 100,
    volume: Math.floor(Math.random() * 1000000),
  }));
}

describe('Live Query Throughput', () => {
  // Test raw update speed without live query
  it('Raw Update throughput (no live query)', async () => {
    const db = createStockDb(`raw_update_${Date.now()}`);
    await db.insert('stocks').values(generateStocks(100000)).exec();

    const iterations = 1000;
    const start = performance.now();

    for (let i = 0; i < iterations; i++) {
      const id = Math.floor(Math.random() * 100000) + 1;
      await db.update('stocks')
        .set('price', 100 + Math.random() * 100)
        .where(col('id').eq(id))
        .exec();
    }

    const duration = performance.now() - start;
    const throughput = iterations / (duration / 1000);

    console.log(`\nRaw Update (100K rows):`);
    console.log(`  ${iterations} updates in ${duration.toFixed(2)}ms`);
    console.log(`  Throughput: ${Math.round(throughput).toLocaleString()} updates/sec`);
    console.log(`  Avg latency: ${(duration / iterations).toFixed(3)}ms`);

    expect(throughput).toBeGreaterThan(10000); // Should be >10K/sec after fix
  });

  // Test live query with subscribe - measure notification overhead
  for (const rowCount of [1000, 10000, 100000]) {
    it(`Live Query throughput with ${rowCount.toLocaleString()} rows`, async () => {
      const db = createStockDb(`live_${rowCount}_${Date.now()}`);
      await db.insert('stocks').values(generateStocks(rowCount)).exec();

      let notificationCount = 0;
      let lastData: any[] = [];

      // Create live query with subscribe
      const stream = db.select('*')
        .from('stocks')
        .where(col('id').lte(100)) // Only watch first 100 rows
        .changes();

      const unsubscribe = stream.subscribe((data: any[]) => {
        notificationCount++;
        lastData = data;
      });

      // Initial notification
      expect(notificationCount).toBe(1);

      // Run updates
      const iterations = 500;
      const start = performance.now();

      for (let i = 0; i < iterations; i++) {
        // Update within watched range
        const id = Math.floor(Math.random() * 100) + 1;
        await db.update('stocks')
          .set('price', 100 + Math.random() * 100)
          .where(col('id').eq(id))
          .exec();
      }

      const duration = performance.now() - start;
      const throughput = iterations / (duration / 1000);

      console.log(`\nLive Query (${rowCount.toLocaleString()} rows, watching 100):`);
      console.log(`  ${iterations} updates in ${duration.toFixed(2)}ms`);
      console.log(`  Notifications received: ${notificationCount - 1}`);
      console.log(`  Throughput: ${Math.round(throughput).toLocaleString()} updates/sec`);
      console.log(`  Avg latency: ${(duration / iterations).toFixed(3)}ms`);

      unsubscribe();

      // Live query should still be reasonably fast
      expect(throughput).toBeGreaterThan(500);
    });
  }

  // Test the re-query overhead specifically
  it('Re-query overhead analysis', async () => {
    const db = createStockDb(`requery_${Date.now()}`);
    await db.insert('stocks').values(generateStocks(100000)).exec();

    // Measure query execution time
    const queryIterations = 100;
    const queryStart = performance.now();
    for (let i = 0; i < queryIterations; i++) {
      await db.select('*')
        .from('stocks')
        .where(col('id').lte(100))
        .exec();
    }
    const queryDuration = performance.now() - queryStart;
    const avgQueryTime = queryDuration / queryIterations;

    console.log(`\nRe-query overhead analysis (100K rows):`);
    console.log(`  Query (SELECT WHERE id <= 100): ${avgQueryTime.toFixed(3)}ms avg`);
    console.log(`  Max theoretical throughput: ${Math.round(1000 / avgQueryTime).toLocaleString()} queries/sec`);
    console.log(`  This is the bottleneck for Live Query with re-query strategy`);

    expect(avgQueryTime).toBeLessThan(10); // Should be fast with index
  });

  // Test batch updates with live query
  it('Batch updates with Live Query', async () => {
    const db = createStockDb(`batch_live_${Date.now()}`);
    await db.insert('stocks').values(generateStocks(100000)).exec();

    let notificationCount = 0;

    const stream = db.select('*')
      .from('stocks')
      .where(col('id').lte(100))
      .changes();

    const unsubscribe = stream.subscribe(() => {
      notificationCount++;
    });

    // Batch updates - multiple updates per "tick"
    const batches = 100;
    const batchSize = 10;
    const start = performance.now();

    for (let batch = 0; batch < batches; batch++) {
      for (let i = 0; i < batchSize; i++) {
        const id = Math.floor(Math.random() * 100) + 1;
        await db.update('stocks')
          .set('price', 100 + Math.random() * 100)
          .where(col('id').eq(id))
          .exec();
      }
    }

    const duration = performance.now() - start;
    const totalUpdates = batches * batchSize;
    const throughput = totalUpdates / (duration / 1000);

    console.log(`\nBatch Updates with Live Query:`);
    console.log(`  ${batches} batches x ${batchSize} updates = ${totalUpdates} total`);
    console.log(`  Duration: ${duration.toFixed(2)}ms`);
    console.log(`  Notifications: ${notificationCount - 1}`);
    console.log(`  Update throughput: ${Math.round(throughput).toLocaleString()} updates/sec`);

    unsubscribe();
  });
});
