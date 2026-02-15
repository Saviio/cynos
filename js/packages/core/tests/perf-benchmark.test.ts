/**
 * Cynos Database - Comprehensive Performance Benchmark
 *
 * Tests: Simple Query, Complex Query (sort + filter + project), Live Query
 * Data sizes: 100, 1000, 10000 rows
 */

import { describe, it, expect, beforeAll } from 'vitest';
import init, {
  Database,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  col,
  
} from '../wasm/cynos_database.js';

beforeAll(async () => {
  await init();
  
});

// Helper to create test database with users and orders tables
function createTestDb(name: string) {
  const db = new Database(name);

  // Users table
  const usersBuilder = db.createTable('users')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('name', JsDataType.String, null)
    .column('age', JsDataType.Int32, null)
    .column('department', JsDataType.String, null)
    .column('salary', JsDataType.Int64, null)
    .index('idx_age', 'age')
    .index('idx_department', 'department');
  db.registerTable(usersBuilder);

  // Orders table (for join tests)
  const ordersBuilder = db.createTable('orders')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('user_id', JsDataType.Int64, null)
    .column('amount', JsDataType.Int64, null)
    .column('status', JsDataType.String, null)
    .index('idx_user_id', 'user_id');
  db.registerTable(ordersBuilder);

  return db;
}

// Generate test data
function generateUsers(count: number) {
  const departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'];
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    name: `User ${i + 1}`,
    age: 20 + (i % 50),
    department: departments[i % departments.length],
    salary: 50000 + (i % 100) * 1000,
  }));
}

function generateOrders(count: number, maxUserId: number) {
  const statuses = ['pending', 'completed', 'cancelled'];
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    user_id: (i % maxUserId) + 1,
    amount: 100 + (i % 1000) * 10,
    status: statuses[i % statuses.length],
  }));
}

// Benchmark helper
interface BenchmarkResult {
  operation: string;
  dataSize: number;
  duration: number;
  rowsProcessed: number;
}

const results: BenchmarkResult[] = [];

function recordResult(operation: string, dataSize: number, duration: number, rowsProcessed: number) {
  results.push({ operation, dataSize, duration, rowsProcessed });
  console.log(`[${operation}] ${dataSize} rows: ${duration.toFixed(2)}ms (${rowsProcessed} rows processed)`);
}

// ============================================================================
// Simple Query Tests
// ============================================================================
describe('Performance: Simple Query (SELECT * FROM users)', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should query ${size} rows efficiently`, async () => {
      const db = createTestDb(`simple_query_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const start = performance.now();
      const results = await db.select('*').from('users').exec();
      const duration = performance.now() - start;

      recordResult('Simple Query', size, duration, results.length);
      expect(results).toHaveLength(size);
    });
  }
});

// ============================================================================
// Filter Query Tests
// ============================================================================
describe('Performance: Filter Query (WHERE age > 30)', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should filter ${size} rows efficiently`, async () => {
      const db = createTestDb(`filter_query_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const start = performance.now();
      const results = await db.select('*')
        .from('users')
        .where(col('age').gt(30))
        .exec();
      const duration = performance.now() - start;

      recordResult('Filter Query', size, duration, results.length);
      expect(results.length).toBeGreaterThan(0);
    });
  }
});

// ============================================================================
// Sort Query Tests
// ============================================================================
describe('Performance: Sort Query (ORDER BY salary DESC)', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should sort ${size} rows efficiently`, async () => {
      const db = createTestDb(`sort_query_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const start = performance.now();
      const results = await db.select('*')
        .from('users')
        .orderBy('salary', JsSortOrder.Desc)
        .exec();
      const duration = performance.now() - start;

      recordResult('Sort Query', size, duration, results.length);
      expect(results).toHaveLength(size);
      // Verify sorted
      for (let i = 1; i < results.length; i++) {
        expect(results[i - 1].salary).toBeGreaterThanOrEqual(results[i].salary);
      }
    });
  }
});

// ============================================================================
// Complex Query Tests (Filter + Sort + Limit)
// ============================================================================
describe('Performance: Complex Query (Filter + Sort + Limit)', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should handle complex query on ${size} rows`, async () => {
      const db = createTestDb(`complex_query_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const start = performance.now();
      const results = await db.select('*')
        .from('users')
        .where(col('age').gt(25).and(col('department').eq('Engineering')))
        .orderBy('salary', JsSortOrder.Desc)
        .limit(50)
        .exec();
      const duration = performance.now() - start;

      recordResult('Complex Query', size, duration, results.length);
      expect(results.length).toBeLessThanOrEqual(50);
    });
  }
});

// ============================================================================
// Multi-condition Filter Tests
// ============================================================================
describe('Performance: Multi-condition Filter (OR + AND)', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should handle multi-condition filter on ${size} rows`, async () => {
      const db = createTestDb(`multi_filter_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const start = performance.now();
      const results = await db.select('*')
        .from('users')
        .where(
          col('department').eq('Engineering')
            .or(col('department').eq('Sales'))
            .and(col('age').gte(30))
        )
        .exec();
      const duration = performance.now() - start;

      recordResult('Multi-condition Filter', size, duration, results.length);
      expect(results.length).toBeGreaterThan(0);
    });
  }
});

// ============================================================================
// Pagination Tests
// ============================================================================
describe('Performance: Pagination (OFFSET + LIMIT)', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should paginate ${size} rows efficiently`, async () => {
      const db = createTestDb(`pagination_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const pageSize = 20;
      const offset = Math.floor(size / 2);

      const start = performance.now();
      const results = await db.select('*')
        .from('users')
        .orderBy('id', JsSortOrder.Asc)
        .offset(offset)
        .limit(pageSize)
        .exec();
      const duration = performance.now() - start;

      recordResult('Pagination', size, duration, results.length);
      expect(results.length).toBeLessThanOrEqual(pageSize);
    });
  }
});

// ============================================================================
// Live Query Tests (Observable)
// ============================================================================
describe('Performance: Live Query (observe)', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should create observable for ${size} rows`, async () => {
      const db = createTestDb(`live_query_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const start = performance.now();
      const observable = db.select('*').from('users').observe();
      const duration = performance.now() - start;

      recordResult('Live Query (create)', size, duration, observable.length);
      expect(observable.length).toBe(size);
    });
  }
});

describe('Performance: Live Query with Filter', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should create filtered observable for ${size} rows`, async () => {
      const db = createTestDb(`live_filter_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const start = performance.now();
      const observable = db.select('*')
        .from('users')
        .where(col('age').gt(30))
        .observe();
      const duration = performance.now() - start;

      recordResult('Live Query (filtered)', size, duration, observable.length);
      expect(observable.length).toBeGreaterThan(0);
    });
  }
});

// ============================================================================
// Changes Stream Tests
// ============================================================================
describe('Performance: Changes Stream (subscribe)', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should subscribe to changes for ${size} rows`, async () => {
      const db = createTestDb(`changes_stream_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      let receivedData: any[] = [];
      const start = performance.now();

      const stream = db.select('*')
        .from('users')
        .where(col('age').gt(25))
        .changes();

      const unsubscribe = stream.subscribe((data: any[]) => {
        receivedData = data;
      });

      const duration = performance.now() - start;

      recordResult('Changes Stream (subscribe)', size, duration, receivedData.length);
      expect(receivedData.length).toBeGreaterThan(0);

      // Cleanup
      unsubscribe();
    });
  }
});

// ============================================================================
// Live Query Update Propagation Tests
// ============================================================================
describe('Performance: Live Query Update Propagation', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should propagate updates for ${size} rows`, async () => {
      // Use unique database name with timestamp to avoid conflicts
      const db = createTestDb(`live_update_${size}_${Date.now()}`);
      await db.insert('users').values(generateUsers(size)).exec();

      let updateCount = 0;
      let lastData: any[] = [];

      const stream = db.select('*')
        .from('users')
        .where(col('department').eq('Engineering'))
        .changes();

      const unsubscribe = stream.subscribe((data: any[]) => {
        updateCount++;
        lastData = data;
      });

      // Initial subscription triggers first callback
      expect(updateCount).toBe(1);
      const initialCount = lastData.length;

      // Insert new Engineering user
      const start = performance.now();
      await db.insert('users').values([{
        id: 999999999,
        name: 'New Engineer',
        age: 28,
        department: 'Engineering',
        salary: 80000,
      }]).exec();
      const duration = performance.now() - start;

      recordResult('Live Update Propagation', size, duration, lastData.length);

      // Should have received update
      expect(updateCount).toBe(2);
      expect(lastData.length).toBe(initialCount + 1);

      unsubscribe();
    });
  }
});

// ============================================================================
// Insert Performance Tests
// ============================================================================
describe('Performance: Bulk Insert', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should insert ${size} rows efficiently`, async () => {
      const db = createTestDb(`bulk_insert_${size}`);
      const users = generateUsers(size);

      const start = performance.now();
      await db.insert('users').values(users).exec();
      const duration = performance.now() - start;

      recordResult('Bulk Insert', size, duration, size);
      expect(db.totalRowCount()).toBe(size);
    });
  }
});

// ============================================================================
// Update Performance Tests
// ============================================================================
describe('Performance: Bulk Update', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should update rows in ${size} row table`, async () => {
      const db = createTestDb(`bulk_update_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const start = performance.now();
      await db.update('users')
        .set('salary', 100000)
        .where(col('department').eq('Engineering'))
        .exec();
      const duration = performance.now() - start;

      // Verify update
      const updated = await db.select('*')
        .from('users')
        .where(col('department').eq('Engineering'))
        .exec();

      recordResult('Bulk Update', size, duration, updated.length);
      expect(updated.every((u: any) => u.salary === 100000)).toBe(true);
    });
  }
});

// ============================================================================
// Delete Performance Tests
// ============================================================================
describe('Performance: Bulk Delete', () => {
  for (const size of [100, 1000, 10000]) {
    it(`should delete rows from ${size} row table`, async () => {
      const db = createTestDb(`bulk_delete_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      const initialCount = db.totalRowCount();

      const start = performance.now();
      await db.delete('users')
        .where(col('age').lt(30))
        .exec();
      const duration = performance.now() - start;

      const remaining = db.totalRowCount();
      const deleted = initialCount - remaining;

      recordResult('Bulk Delete', size, duration, deleted);
      expect(remaining).toBeLessThan(initialCount);
    });
  }
});

// ============================================================================
// Plan Cache Test: Same Query Repeated 1000 Times
// ============================================================================
describe('Performance: Plan Cache (Same Query x1000)', () => {
  for (const size of [1000, 10000, 50000]) {
    it(`should benefit from plan cache with ${size} rows`, async () => {
      const db = createTestDb(`plan_cache_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      // Warm up - first query compiles the plan
      await db.select('*')
        .from('users')
        .where(col('age').gt(30).and(col('department').eq('Engineering')))
        .exec();

      // Run same query 1000 times
      const iterations = 1000;
      const start = performance.now();
      for (let i = 0; i < iterations; i++) {
        await db.select('*')
          .from('users')
          .where(col('age').gt(30).and(col('department').eq('Engineering')))
          .exec();
      }
      const duration = performance.now() - start;

      const avgPerQuery = duration / iterations;
      recordResult('Plan Cache (1000x same query)', size, duration, iterations);
      console.log(`  Average per query: ${avgPerQuery.toFixed(3)}ms`);
      expect(avgPerQuery).toBeLessThan(50); // Should be fast with cache
    });
  }
});

// ============================================================================
// Random Point Query Test: BTree Binary Search
// ============================================================================
describe('Performance: Random Point Queries (BTree)', () => {
  for (const size of [1000, 10000, 50000]) {
    it(`should handle random point queries on ${size} rows`, async () => {
      const db = createTestDb(`point_query_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      // Generate random IDs to query
      const queryCount = 1000;
      const randomIds = Array.from({ length: queryCount }, () =>
        Math.floor(Math.random() * size) + 1
      );

      const start = performance.now();
      let found = 0;
      for (const id of randomIds) {
        const results = await db.select('*')
          .from('users')
          .where(col('id').eq(id))
          .exec();
        if (results.length > 0) found++;
      }
      const duration = performance.now() - start;

      const avgPerQuery = duration / queryCount;
      recordResult('Random Point Query (1000x)', size, duration, found);
      console.log(`  Average per query: ${avgPerQuery.toFixed(3)}ms, Found: ${found}/${queryCount}`);
      expect(found).toBe(queryCount); // All should be found
    });
  }
});

// ============================================================================
// Range Query Test: BTree Range Scan
// ============================================================================
describe('Performance: Range Queries (BTree)', () => {
  for (const size of [1000, 10000, 50000]) {
    it(`should handle range queries on ${size} rows`, async () => {
      const db = createTestDb(`range_query_${size}`);
      await db.insert('users').values(generateUsers(size)).exec();

      // Run 100 range queries with different ranges
      const queryCount = 100;
      const start = performance.now();
      let totalRows = 0;
      for (let i = 0; i < queryCount; i++) {
        const minAge = 20 + (i % 30);
        const maxAge = minAge + 10;
        const results = await db.select('*')
          .from('users')
          .where(col('age').gte(minAge).and(col('age').lte(maxAge)))
          .exec();
        totalRows += results.length;
      }
      const duration = performance.now() - start;

      const avgPerQuery = duration / queryCount;
      recordResult('Range Query (100x)', size, duration, totalRows);
      console.log(`  Average per query: ${avgPerQuery.toFixed(3)}ms, Total rows: ${totalRows}`);
      expect(totalRows).toBeGreaterThan(0);
    });
  }
});

// ============================================================================
// Summary
// ============================================================================
describe('Performance Summary', () => {
  it('should print summary', () => {
    console.log('\n========================================');
    console.log('PERFORMANCE BENCHMARK SUMMARY');
    console.log('========================================\n');

    const grouped = results.reduce((acc, r) => {
      if (!acc[r.operation]) acc[r.operation] = [];
      acc[r.operation].push(r);
      return acc;
    }, {} as Record<string, BenchmarkResult[]>);

    for (const [op, data] of Object.entries(grouped)) {
      console.log(`${op}:`);
      for (const r of data) {
        const throughput = r.rowsProcessed / (r.duration / 1000);
        console.log(`  ${r.dataSize.toString().padStart(5)} rows: ${r.duration.toFixed(2).padStart(8)}ms (${Math.round(throughput).toLocaleString()} rows/sec)`);
      }
      console.log('');
    }

    expect(true).toBe(true);
  });
});
