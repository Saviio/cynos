/**
 * Cynos Database - Performance Tests
 *
 * These tests measure the performance of various database operations.
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import init, {
  Database,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  col,
  
} from '../wasm/cynos_database.js';

// Initialize WASM before all tests
beforeAll(async () => {
  await init();
  
});

describe('Performance: Insert Operations', () => {
  it('should insert 1000 rows efficiently', async () => {
    const db = new Database('perf_insert');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('value', JsDataType.Int32, null);
    db.registerTable(builder);

    const rows = Array.from({ length: 1000 }, (_, i) => ({
      id: i + 1,
      name: `Item ${i + 1}`,
      value: Math.floor(Math.random() * 1000),
    }));

    const start = performance.now();
    await db.insert('items').values(rows).exec();
    const duration = performance.now() - start;

    console.log(`Insert 1000 rows: ${duration.toFixed(2)}ms`);
    expect(db.totalRowCount()).toBe(1000);
    expect(duration).toBeLessThan(1000); // Should complete in under 1 second
  });

  it('should insert 10000 rows efficiently', async () => {
    const db = new Database('perf_insert_large');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('value', JsDataType.Int32, null);
    db.registerTable(builder);

    const rows = Array.from({ length: 10000 }, (_, i) => ({
      id: i + 1,
      name: `Item ${i + 1}`,
      value: Math.floor(Math.random() * 1000),
    }));

    const start = performance.now();
    await db.insert('items').values(rows).exec();
    const duration = performance.now() - start;

    console.log(`Insert 10000 rows: ${duration.toFixed(2)}ms`);
    expect(db.totalRowCount()).toBe(10000);
    expect(duration).toBeLessThan(5000); // Should complete in under 5 seconds
  });
});

describe('Performance: Query Operations', () => {
  let db: Database;

  beforeEach(async () => {
    db = new Database('perf_query');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('category', JsDataType.String, null)
      .column('value', JsDataType.Int32, null)
      .index('idx_category', 'category');
    db.registerTable(builder);

    const categories = ['A', 'B', 'C', 'D', 'E'];
    const rows = Array.from({ length: 5000 }, (_, i) => ({
      id: i + 1,
      name: `Item ${i + 1}`,
      category: categories[i % categories.length],
      value: Math.floor(Math.random() * 1000),
    }));

    await db.insert('items').values(rows).exec();
  });

  it('should perform full table scan efficiently', async () => {
    const start = performance.now();
    const results = await db.select('*').from('items').exec();
    const duration = performance.now() - start;

    console.log(`Full scan 5000 rows: ${duration.toFixed(2)}ms`);
    expect(results).toHaveLength(5000);
    expect(duration).toBeLessThan(500);
  });

  it('should filter with WHERE efficiently', async () => {
    const start = performance.now();
    const results = await db.select('*')
      .from('items')
      .where(col('category').eq('A'))
      .exec();
    const duration = performance.now() - start;

    console.log(`Filter by category: ${duration.toFixed(2)}ms`);
    expect(results).toHaveLength(1000);
    expect(duration).toBeLessThan(200);
  });

  it('should sort efficiently', async () => {
    const start = performance.now();
    const results = await db.select('*')
      .from('items')
      .orderBy('value', JsSortOrder.Desc)
      .exec();
    const duration = performance.now() - start;

    console.log(`Sort 5000 rows: ${duration.toFixed(2)}ms`);
    expect(results).toHaveLength(5000);
    expect(duration).toBeLessThan(500);
  });

  it('should paginate efficiently', async () => {
    const start = performance.now();
    const results = await db.select('*')
      .from('items')
      .orderBy('id', JsSortOrder.Asc)
      .offset(2500)
      .limit(100)
      .exec();
    const duration = performance.now() - start;

    console.log(`Paginate (offset 2500, limit 100): ${duration.toFixed(2)}ms`);
    expect(results).toHaveLength(100);
    expect(duration).toBeLessThan(200);
  });

  it('should handle complex queries efficiently', async () => {
    const start = performance.now();
    const results = await db.select('*')
      .from('items')
      .where(col('category').eq('A').or(col('category').eq('B')))
      .orderBy('value', JsSortOrder.Desc)
      .limit(50)
      .exec();
    const duration = performance.now() - start;

    console.log(`Complex query: ${duration.toFixed(2)}ms`);
    expect(results).toHaveLength(50);
    expect(duration).toBeLessThan(300);
  });
});

describe('Performance: Update Operations', () => {
  let db: Database;

  beforeEach(async () => {
    db = new Database('perf_update');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('value', JsDataType.Int32, null);
    db.registerTable(builder);

    const rows = Array.from({ length: 1000 }, (_, i) => ({
      id: i + 1,
      value: i,
    }));

    await db.insert('items').values(rows).exec();
  });

  it('should update single row efficiently', async () => {
    const start = performance.now();
    await db.update('items')
      .set('value', 999)
      .where(col('id').eq(500))
      .exec();
    const duration = performance.now() - start;

    console.log(`Update single row: ${duration.toFixed(2)}ms`);
    expect(duration).toBeLessThan(50);
  });

  it('should update multiple rows efficiently', async () => {
    const start = performance.now();
    await db.update('items')
      .set('value', 0)
      .where(col('value').lt(500))
      .exec();
    const duration = performance.now() - start;

    console.log(`Update 500 rows: ${duration.toFixed(2)}ms`);
    expect(duration).toBeLessThan(500);
  });
});

describe('Performance: Delete Operations', () => {
  let db: Database;

  beforeEach(async () => {
    db = new Database('perf_delete');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('value', JsDataType.Int32, null);
    db.registerTable(builder);

    const rows = Array.from({ length: 1000 }, (_, i) => ({
      id: i + 1,
      value: i,
    }));

    await db.insert('items').values(rows).exec();
  });

  it('should delete single row efficiently', async () => {
    const start = performance.now();
    await db.delete('items')
      .where(col('id').eq(500))
      .exec();
    const duration = performance.now() - start;

    console.log(`Delete single row: ${duration.toFixed(2)}ms`);
    expect(duration).toBeLessThan(50);
  });

  it('should delete multiple rows efficiently', async () => {
    const start = performance.now();
    await db.delete('items')
      .where(col('value').lt(500))
      .exec();
    const duration = performance.now() - start;

    console.log(`Delete 500 rows: ${duration.toFixed(2)}ms`);
    expect(duration).toBeLessThan(500);
  });
});

describe('Performance: Transaction Operations', () => {
  it('should handle transaction with many operations', async () => {
    const db = new Database('perf_tx');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('value', JsDataType.Int32, null);
    db.registerTable(builder);

    const tx = db.transaction();

    const start = performance.now();

    // Insert 100 rows in transaction
    for (let i = 0; i < 100; i++) {
      tx.insert('items', [{ id: i + 1, value: i }]);
    }

    tx.commit();
    const duration = performance.now() - start;

    console.log(`Transaction with 100 inserts: ${duration.toFixed(2)}ms`);
    expect(db.totalRowCount()).toBe(100);
    expect(duration).toBeLessThan(500);
  });
});

describe('Performance: Reactive Queries', () => {
  it('should create observable query efficiently', async () => {
    const db = new Database('perf_reactive');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('value', JsDataType.Int32, null);
    db.registerTable(builder);

    const rows = Array.from({ length: 1000 }, (_, i) => ({
      id: i + 1,
      value: i,
    }));

    await db.insert('items').values(rows).exec();

    const start = performance.now();
    const observable = db.select('*').from('items').observe();
    const duration = performance.now() - start;

    console.log(`Create observable (1000 rows): ${duration.toFixed(2)}ms`);
    expect(observable.length).toBe(1000);
    expect(duration).toBeLessThan(200);
  });
});
