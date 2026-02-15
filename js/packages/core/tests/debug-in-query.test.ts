import { describe, it, expect, beforeAll } from 'vitest';
import init, { Database, JsDataType, ColumnOptions, col } from '../wasm/cynos_database.js';

// Replicate the exact setup from comprehensive-perf.test.ts
function createTestDb(name: string) {
  const db = new Database(name);

  const productsBuilder = db.createTable('products')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('name', JsDataType.String, null)
    .column('category', JsDataType.String, null)
    .column('price', JsDataType.Int64, null)
    .column('stock', JsDataType.Int32, null)
    .column('rating', JsDataType.Float64, null)
    .column('description', JsDataType.String, new ColumnOptions().setNullable(true))
    .column('is_active', JsDataType.Bool, null)
    .index('idx_category', 'category')
    .index('idx_price', 'price')
    .index('idx_stock', 'stock');
  db.registerTable(productsBuilder);

  return db;
}

const CATEGORIES = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports'];

function generateProducts(count: number) {
  const products = [];
  for (let i = 0; i < count; i++) {
    products.push({
      id: i,
      name: `Product ${i}`,
      category: CATEGORIES[i % CATEGORIES.length],
      price: Math.floor(Math.random() * 1000) + 1,
      stock: Math.floor(Math.random() * 100),
      rating: Math.random() * 5,
      description: i % 10 === 0 ? null : `Description for product ${i}`,
      is_active: i % 3 !== 0,
    });
  }
  return products;
}

describe('Debug IN Query Plan', () => {
  beforeAll(async () => {
    await init();
  });

  it('should check if IN query uses index in comprehensive-perf setup', async () => {
    const db = createTestDb('debug_in');
    await db.insert('products').values(generateProducts(10000)).exec();

    // Build the exact same query as comprehensive-perf
    const query = db.select('*').from('products')
      .where(col('category').in(['Electronics', 'Books', 'Sports']));

    // Check the plan
    const plan = query.explain();

    console.log('=== comprehensive-perf IN Query Plan ===');
    console.log('Optimized Plan:', plan.optimized);
    console.log('Physical Plan:', plan.physical);

    // Check if IndexInGet is used
    const usesIndex = plan.optimized.includes('IndexInGet');
    console.log('Uses IndexInGet:', usesIndex);

    // Run the query and measure
    const warmup = await query.exec();
    console.log('Result count:', warmup.length);

    const iterations = 100;
    const start = performance.now();
    for (let i = 0; i < iterations; i++) {
      await query.exec();
    }
    const elapsed = performance.now() - start;
    console.log(`${iterations} iterations: ${elapsed.toFixed(2)}ms`);
    console.log(`Average: ${(elapsed / iterations).toFixed(3)}ms per query`);

    expect(usesIndex).toBe(true);
  });

  it('should compare IN query with and without LIMIT', async () => {
    const db = createTestDb('debug_in_limit');
    await db.insert('products').values(generateProducts(10000)).exec();

    // Without LIMIT - returns 6000 rows
    const queryNoLimit = db.select('*').from('products')
      .where(col('category').in(['Electronics', 'Books', 'Sports']));

    // With LIMIT 10
    const queryWithLimit = db.select('*').from('products')
      .where(col('category').in(['Electronics', 'Books', 'Sports']))
      .limit(10);

    // Warmup
    await queryNoLimit.exec();
    await queryWithLimit.exec();

    const iterations = 100;

    // Measure without LIMIT
    let start = performance.now();
    for (let i = 0; i < iterations; i++) {
      await queryNoLimit.exec();
    }
    const noLimitTime = performance.now() - start;

    // Measure with LIMIT
    start = performance.now();
    for (let i = 0; i < iterations; i++) {
      await queryWithLimit.exec();
    }
    const withLimitTime = performance.now() - start;

    console.log('=== IN Query: LIMIT comparison ===');
    console.log(`Without LIMIT (6000 rows): ${(noLimitTime / iterations).toFixed(3)}ms avg`);
    console.log(`With LIMIT 10: ${(withLimitTime / iterations).toFixed(3)}ms avg`);
    console.log(`Speedup: ${(noLimitTime / withLimitTime).toFixed(1)}x`);
  });
});
