import { describe, it, expect, beforeAll } from 'vitest';
import init, { Database, JsDataType, ColumnOptions, col } from '../wasm/cynos_database.js';

describe('IN Query Index Optimization', () => {
  let db: Database;

  beforeAll(async () => {
    await init();
    db = new Database('in_test');

    // Create table with index on category
    const builder = db.createTable('products')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('category', JsDataType.String, null)
      .column('name', JsDataType.String, null)
      .index('idx_category', 'category');

    db.registerTable(builder);

    // Insert test data
    const products = [];
    const categories = ['Electronics', 'Books', 'Sports', 'Home', 'Garden'];
    for (let i = 0; i < 1000; i++) {
      products.push({
        id: i,
        category: categories[i % 5],
        name: `Product ${i}`,
      });
    }
    await db.insert('products').values(products).exec();
  });

  it('should show IndexInGet in explain for IN query with indexed column', () => {
    const query = db.select('*')
      .from('products')
      .where(col('category').in(['Electronics', 'Books', 'Sports']));

    const plan = query.explain();

    console.log('=== IN Query Explain ===');
    console.log('Logical Plan:', plan.logical);
    console.log('Optimized Plan:', plan.optimized);
    console.log('Physical Plan:', plan.physical);

    // Check if IndexInGet is used in the optimized plan
    expect(plan.optimized).toContain('IndexInGet');
    expect(plan.physical).toContain('IndexInGet');
  });

  it('should show Filter (full scan) for IN query without index', async () => {
    // Create another table without index on the queried column
    const builder2 = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('type', JsDataType.String, null);

    db.registerTable(builder2);

    await db.insert('items').values([
      { id: 1, type: 'A' },
      { id: 2, type: 'B' },
      { id: 3, type: 'C' },
    ]).exec();

    const query = db.select('*')
      .from('items')
      .where(col('type').in(['A', 'B']));

    const plan = query.explain();

    console.log('=== IN Query Without Index ===');
    console.log('Logical Plan:', plan.logical);
    console.log('Optimized Plan:', plan.optimized);
    console.log('Physical Plan:', plan.physical);

    // Without index, should remain as Filter (not IndexInGet)
    expect(plan.optimized).toContain('Filter');
    expect(plan.optimized).not.toContain('IndexInGet');
  });

  it('should compare performance with and without index optimization', async () => {
    // Query with IN on indexed column
    const query = db.select('*')
      .from('products')
      .where(col('category').in(['Electronics', 'Books', 'Sports']));

    // Warm up
    await query.exec();

    // Measure
    const iterations = 100;
    const start = performance.now();
    for (let i = 0; i < iterations; i++) {
      await query.exec();
    }
    const elapsed = performance.now() - start;

    console.log(`IN query (${iterations} iterations): ${elapsed.toFixed(2)}ms`);
    console.log(`Average: ${(elapsed / iterations).toFixed(3)}ms per query`);

    const result = await query.exec();
    console.log(`Result count: ${result.length} rows`);

    // Should return 600 rows (3 categories * 200 each)
    expect(result.length).toBe(600);
  });
});
