/**
 * Binary Protocol 性能对比测试
 *
 * 测试目标：对比 exec() (JSON) vs execBinary() (Binary) 的性能差异
 * 覆盖场景：点查询、范围查询、JSONB 查询、有/无 LIMIT
 *
 * 参考 comprehensive-perf.test.ts 的结构
 */

import { describe, it, expect, beforeAll } from 'vitest';
import init, {
  Database,
  JsDataType,
  ColumnOptions,
  col,
} from '../wasm/cynos_database.js';
import { ResultSet } from '../src/result-set.js';

beforeAll(async () => {
  await init();
});

// =============================================================================
// 测试配置
// =============================================================================

/** 固定 10 万行数据 */
const DATA_SIZE = 100000;

/** 迭代次数配置 - 根据操作复杂度调整 */
const ITERATIONS = {
  point: 50,      // 点查询
  range: 20,      // 范围查询
  full: 5,        // 全表扫描
  lazy: 10,       // 懒加载
};

// =============================================================================
// 性能记录工具
// =============================================================================

interface BenchmarkResult {
  category: string;
  operation: string;
  jsonDuration: number;
  binaryDuration: number;
  rowCount: number;
  speedup: number;  // jsonDuration / binaryDuration
}

const benchmarkResults: BenchmarkResult[] = [];

/**
 * 记录性能对比结果
 */
function record(
  category: string,
  operation: string,
  jsonDuration: number,
  binaryDuration: number,
  rowCount: number
) {
  const speedup = jsonDuration / binaryDuration;
  benchmarkResults.push({ category, operation, jsonDuration, binaryDuration, rowCount, speedup });
}

// =============================================================================
// 测试数据生成
// =============================================================================

/**
 * 创建测试数据库
 */
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
    .column('is_active', JsDataType.Boolean, null)
    .index('idx_category', 'category')
    .index('idx_price', 'price')
    .index('idx_stock', 'stock');
  db.registerTable(productsBuilder);

  return db;
}

/**
 * 生成测试产品数据
 */
function generateProducts(count: number) {
  const categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'];
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    name: `Product ${i + 1}`,
    category: categories[i % categories.length],
    price: 1000 + (i % 1000) * 100,
    stock: i % 500,
    rating: 1.0 + (i % 50) / 10,
    description: i % 10 === 0 ? null : `Description for product ${i + 1}`,
    is_active: i % 3 !== 0,
  }));
}

/**
 * 创建带 JSONB 列的测试数据库
 */
function createJsonbTestDb(name: string) {
  const db = new Database(name);
  const builder = db.createTable('documents')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('title', JsDataType.String, null)
    .column('metadata', JsDataType.Jsonb, null)
    .index('idx_metadata', 'metadata');
  db.registerTable(builder);
  return db;
}

/**
 * 生成 JSONB 测试数据
 */
function generateJsonbDocuments(count: number) {
  const categories = ['tech', 'business', 'science', 'health', 'sports'];
  const statuses = ['published', 'draft', 'archived'];
  const priorities = [1, 2, 3, 4, 5];

  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    title: `Document ${i + 1}`,
    metadata: {
      category: categories[i % categories.length],
      status: statuses[i % statuses.length],
      priority: priorities[i % priorities.length],
      views: i * 10,
      author: `Author ${i % 100}`,
    },
  }));
}

// =============================================================================
// SECTION 1: 点查询对比
// =============================================================================
describe('1. Point Query: JSON vs Binary', () => {
  /**
   * 1.1 主键查询 (1 row)
   */
  it('1.1 Primary Key Lookup', async () => {
    const db = createTestDb('bench_pk');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.point;
    const layout = db.select('*').from('products').where(col('id').eq(1)).getSchemaLayout();

    // JSON 方式
    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const targetId = (i % DATA_SIZE) + 1;
      await db.select('*').from('products').where(col('id').eq(targetId)).exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    // Binary 方式
    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const targetId = (i % DATA_SIZE) + 1;
      const result = await db.select('*').from('products').where(col('id').eq(targetId)).execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Point Query', 'PK Lookup (1 row)', jsonDuration, binaryDuration, 1);
    expect(binaryDuration).toBeLessThan(jsonDuration * 2);
  });

  /**
   * 1.2 索引列查询 + LIMIT 10
   */
  it('1.2 Indexed Column + LIMIT 10', async () => {
    const db = createTestDb('bench_idx_limit');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'];
    const iterations = ITERATIONS.point;
    const layout = db.select('*').from('products')
      .where(col('category').eq('Electronics'))
      .limit(10)
      .getSchemaLayout();

    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      await db.select('*').from('products')
        .where(col('category').eq(categories[i % 5]))
        .limit(10)
        .exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const result = await db.select('*').from('products')
        .where(col('category').eq(categories[i % 5]))
        .limit(10)
        .execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Point Query', 'Indexed + LIMIT 10', jsonDuration, binaryDuration, 10);
  });

  /**
   * 1.3 索引列查询 无 LIMIT (~20% rows)
   */
  it('1.3 Indexed Column No LIMIT (~20%)', async () => {
    const db = createTestDb('bench_idx_no_limit');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.full;
    const layout = db.select('*').from('products')
      .where(col('category').eq('Electronics'))
      .getSchemaLayout();

    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      await db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const result = await db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Point Query', 'Indexed No LIMIT (~20%)', jsonDuration, binaryDuration, DATA_SIZE / 5);
  });
});

// =============================================================================
// SECTION 2: 范围查询对比
// =============================================================================
describe('2. Range Query: JSON vs Binary', () => {
  /**
   * 2.1 BETWEEN + LIMIT 10
   */
  it('2.1 BETWEEN + LIMIT 10', async () => {
    const db = createTestDb('bench_between_limit');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.range;
    const layout = db.select('*').from('products')
      .where(col('price').between(5000, 10000))
      .limit(10)
      .getSchemaLayout();

    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      await db.select('*').from('products')
        .where(col('price').between(5000, 10000))
        .limit(10)
        .exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const result = await db.select('*').from('products')
        .where(col('price').between(5000, 10000))
        .limit(10)
        .execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Range Query', 'BETWEEN + LIMIT 10', jsonDuration, binaryDuration, 10);
  });

  /**
   * 2.2 BETWEEN 无 LIMIT (~45% rows)
   */
  it('2.2 BETWEEN No LIMIT (~45%)', async () => {
    const db = createTestDb('bench_between_no_limit');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.full;
    const layout = db.select('*').from('products')
      .where(col('price').between(5000, 50000))
      .getSchemaLayout();

    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      await db.select('*').from('products')
        .where(col('price').between(5000, 50000))
        .exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const result = await db.select('*').from('products')
        .where(col('price').between(5000, 50000))
        .execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Range Query', 'BETWEEN No LIMIT (~45%)', jsonDuration, binaryDuration, Math.floor(DATA_SIZE * 0.45));
  });

  /**
   * 2.3 比较运算符 gt (~10%)
   */
  it('2.3 Comparison gt (~10%)', async () => {
    const db = createTestDb('bench_compare');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.range;
    const layout = db.select('*').from('products')
      .where(col('price').gt(90000))
      .getSchemaLayout();

    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      await db.select('*').from('products')
        .where(col('price').gt(90000))
        .exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const result = await db.select('*').from('products')
        .where(col('price').gt(90000))
        .execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Range Query', 'gt (~10%)', jsonDuration, binaryDuration, Math.floor(DATA_SIZE * 0.1));
  });
});

// =============================================================================
// SECTION 3: JSONB 查询对比
// =============================================================================
describe('3. JSONB Query: JSON vs Binary', () => {
  /**
   * 3.1 单谓词 GIN 查询 (~20%)
   */
  it('3.1 Single Predicate (~20%)', async () => {
    const db = createJsonbTestDb('bench_gin_single');
    await db.insert('documents').values(generateJsonbDocuments(DATA_SIZE)).exec();

    const iterations = ITERATIONS.full;
    const layout = db.select('*').from('documents')
      .where(col('metadata').get('$.category').eq('tech'))
      .getSchemaLayout();

    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      await db.select('*').from('documents')
        .where(col('metadata').get('$.category').eq('tech'))
        .exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const result = await db.select('*').from('documents')
        .where(col('metadata').get('$.category').eq('tech'))
        .execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('JSONB Query', 'Single Predicate (~20%)', jsonDuration, binaryDuration, DATA_SIZE / 5);
  });

  /**
   * 3.2 多谓词 AND (~7%)
   */
  it('3.2 Multi-Predicate AND (~7%)', async () => {
    const db = createJsonbTestDb('bench_gin_multi');
    await db.insert('documents').values(generateJsonbDocuments(DATA_SIZE)).exec();

    const iterations = ITERATIONS.full;
    const layout = db.select('*').from('documents')
      .where(
        col('metadata').get('$.category').eq('tech')
          .and(col('metadata').get('$.status').eq('published'))
      )
      .getSchemaLayout();

    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      await db.select('*').from('documents')
        .where(
          col('metadata').get('$.category').eq('tech')
            .and(col('metadata').get('$.status').eq('published'))
        )
        .exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const result = await db.select('*').from('documents')
        .where(
          col('metadata').get('$.category').eq('tech')
            .and(col('metadata').get('$.status').eq('published'))
        )
        .execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('JSONB Query', '2 Predicates AND (~7%)', jsonDuration, binaryDuration, Math.floor(DATA_SIZE * 0.067));
  });

  /**
   * 3.3 JSONB + LIMIT 10
   */
  it('3.3 JSONB + LIMIT 10', async () => {
    const db = createJsonbTestDb('bench_gin_limit');
    await db.insert('documents').values(generateJsonbDocuments(DATA_SIZE)).exec();

    const iterations = ITERATIONS.range;
    const layout = db.select('*').from('documents')
      .where(col('metadata').get('$.category').eq('tech'))
      .limit(10)
      .getSchemaLayout();

    const jsonStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      await db.select('*').from('documents')
        .where(col('metadata').get('$.category').eq('tech'))
        .limit(10)
        .exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let i = 0; i < iterations; i++) {
      const result = await db.select('*').from('documents')
        .where(col('metadata').get('$.category').eq('tech'))
        .limit(10)
        .execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('JSONB Query', 'JSONB + LIMIT 10', jsonDuration, binaryDuration, 10);
  });
});

// =============================================================================
// SECTION 4: 结果集大小影响
// =============================================================================
describe('4. Result Set Size Impact', () => {
  const RESULT_SIZES = [10, 100, 1000, 10000, 50000];

  for (const resultSize of RESULT_SIZES) {
    it(`${resultSize} rows result set`, async () => {
      const db = createTestDb(`bench_result_size_${resultSize}`);
      await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

      const iterations = resultSize <= 1000 ? ITERATIONS.range : ITERATIONS.full;
      const layout = db.select('*').from('products')
        .limit(resultSize)
        .getSchemaLayout();

      const jsonStart = performance.now();
      for (let i = 0; i < iterations; i++) {
        await db.select('*').from('products')
          .limit(resultSize)
          .exec();
      }
      const jsonDuration = (performance.now() - jsonStart) / iterations;

      const binaryStart = performance.now();
      for (let i = 0; i < iterations; i++) {
        const result = await db.select('*').from('products')
          .limit(resultSize)
          .execBinary();
        const rs = new ResultSet(result, layout);
        rs.toArray();
        rs.free();
      }
      const binaryDuration = (performance.now() - binaryStart) / iterations;

      record('Result Size', `${resultSize.toLocaleString()} rows`, jsonDuration, binaryDuration, resultSize);
    });
  }
});

// =============================================================================
// SECTION 5: Lazy Access (Zero-Copy) 场景
// =============================================================================
describe('5. Lazy Access (Zero-Copy)', () => {
  /**
   * 5.1 数值列聚合 - 只读取数值列，不创建对象
   */
  it('5.1 Numeric Aggregation (SUM)', async () => {
    const db = createTestDb('bench_agg');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.lazy;
    const layout = db.select('*').from('products').getSchemaLayout();

    const jsonStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      const rows = await db.select('*').from('products').exec();
      let sum = 0;
      for (const row of rows as any[]) {
        sum += row.price ?? 0;
      }
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const priceColIdx = 3;
    const binaryStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      const result = await db.select('*').from('products').execBinary();
      const rs = new ResultSet(result, layout);
      let sum = 0;
      for (let i = 0; i < rs.length; i++) {
        sum += rs.getNumber(i, priceColIdx) ?? 0;
      }
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Lazy Access', 'Numeric Aggregation (SUM)', jsonDuration, binaryDuration, DATA_SIZE);
  });

  /**
   * 5.2 只访问第一行
   */
  it('5.2 First Row Only', async () => {
    const db = createTestDb('bench_first');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.point;
    const layout = db.select('*').from('products').getSchemaLayout();

    const jsonStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      const rows = await db.select('*').from('products').exec();
      const first = rows[0];
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      const result = await db.select('*').from('products').execBinary();
      const rs = new ResultSet(result, layout);
      const first = rs.get(0);
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Lazy Access', 'First Row Only', jsonDuration, binaryDuration, 1);
  });

  /**
   * 5.3 条件过滤 (~50%)
   */
  it('5.3 Conditional Filter (~50%)', async () => {
    const db = createTestDb('bench_filter');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.lazy;
    const layout = db.select('*').from('products').getSchemaLayout();
    const priceColIdx = 3;
    const threshold = 50000;

    const jsonStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      const rows = await db.select('*').from('products').exec();
      const filtered = (rows as any[]).filter(r => r.price > threshold);
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      const result = await db.select('*').from('products').execBinary();
      const rs = new ResultSet(result, layout);
      const filtered: any[] = [];
      for (let i = 0; i < rs.length; i++) {
        const price = rs.getNumber(i, priceColIdx);
        if (price !== null && price > threshold) {
          filtered.push(rs.get(i));
        }
      }
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Lazy Access', 'Conditional Filter (~50%)', jsonDuration, binaryDuration, Math.floor(DATA_SIZE * 0.5));
  });

  /**
   * 5.4 多列数值计算
   */
  it('5.4 Multi-Column Calc (price*stock)', async () => {
    const db = createTestDb('bench_multi');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.lazy;
    const layout = db.select('*').from('products').getSchemaLayout();
    const priceColIdx = 3;
    const stockColIdx = 4;

    const jsonStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      const rows = await db.select('*').from('products').exec();
      let totalValue = 0;
      for (const row of rows as any[]) {
        totalValue += (row.price ?? 0) * (row.stock ?? 0);
      }
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      const result = await db.select('*').from('products').execBinary();
      const rs = new ResultSet(result, layout);
      let totalValue = 0;
      for (let i = 0; i < rs.length; i++) {
        const price = rs.getNumber(i, priceColIdx) ?? 0;
        const stock = rs.getInt32(i, stockColIdx) ?? 0;
        totalValue += price * stock;
      }
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Lazy Access', 'Multi-Column Calc (price*stock)', jsonDuration, binaryDuration, DATA_SIZE);
  });

  /**
   * 5.5 纯数值列 toArray
   */
  it('5.5 Numeric-Only toArray', async () => {
    const db = createTestDb('bench_numeric_only');
    await db.insert('products').values(generateProducts(DATA_SIZE)).exec();

    const iterations = ITERATIONS.full;
    const layout = db.select('id', 'price', 'stock', 'rating', 'is_active')
      .from('products')
      .getSchemaLayout();

    const jsonStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      await db.select('id', 'price', 'stock', 'rating', 'is_active')
        .from('products')
        .exec();
    }
    const jsonDuration = (performance.now() - jsonStart) / iterations;

    const binaryStart = performance.now();
    for (let iter = 0; iter < iterations; iter++) {
      const result = await db.select('id', 'price', 'stock', 'rating', 'is_active')
        .from('products')
        .execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    }
    const binaryDuration = (performance.now() - binaryStart) / iterations;

    record('Lazy Access', 'Numeric-Only toArray', jsonDuration, binaryDuration, DATA_SIZE);
  });
});

// =============================================================================
// SECTION 6: 性能总结
// =============================================================================
describe('6. Performance Summary', () => {
  it('should print comprehensive summary', () => {
    console.log('\n');
    console.log('┌' + '─'.repeat(98) + '┐');
    console.log('│' + ' BINARY PROTOCOL BENCHMARK (100K rows)'.padEnd(98) + '│');
    console.log('├' + '─'.repeat(35) + '┬' + '─'.repeat(10) + '┬' + '─'.repeat(12) + '┬' + '─'.repeat(14) + '┬' + '─'.repeat(12) + '┬' + '─'.repeat(10) + '┤');
    console.log('│' + ' Operation'.padEnd(35) + '│' + ' Rows'.padStart(9) + ' │' + ' JSON(ms)'.padStart(11) + ' │' + ' Binary(ms)'.padStart(13) + ' │' + ' Speedup'.padStart(11) + ' │' + ' Winner'.padStart(9) + ' │');
    console.log('├' + '─'.repeat(35) + '┼' + '─'.repeat(10) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(14) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(10) + '┤');

    const grouped = benchmarkResults.reduce((acc, r) => {
      const key = r.category;
      if (!acc[key]) acc[key] = [];
      acc[key].push(r);
      return acc;
    }, {} as Record<string, BenchmarkResult[]>);

    for (const [category, results] of Object.entries(grouped)) {
      // Category header
      console.log('│ ' + `[${category}]`.padEnd(96) + ' │');

      for (const r of results) {
        const speedupStr = r.speedup >= 1
          ? `${r.speedup.toFixed(2)}x`
          : `${(1/r.speedup).toFixed(2)}x slow`;
        const winner = r.speedup >= 1 ? 'Binary' : 'JSON';
        const winnerIcon = r.speedup >= 2 ? '✓✓' : (r.speedup >= 1 ? '✓' : '✗');

        console.log(
          '│ ' + r.operation.padEnd(34) +
          '│' + r.rowCount.toLocaleString().padStart(9) + ' │' +
          r.jsonDuration.toFixed(2).padStart(11) + ' │' +
          r.binaryDuration.toFixed(2).padStart(13) + ' │' +
          speedupStr.padStart(11) + ' │' +
          (winnerIcon + ' ' + winner).padStart(9) + ' │'
        );
      }
      console.log('├' + '─'.repeat(35) + '┼' + '─'.repeat(10) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(14) + '┼' + '─'.repeat(12) + '┼' + '─'.repeat(10) + '┤');
    }

    // Summary stats
    const avgSpeedup = benchmarkResults.reduce((sum, r) => sum + r.speedup, 0) / benchmarkResults.length;
    const maxResult = benchmarkResults.reduce((max, r) => r.speedup > max.speedup ? r : max);
    const minResult = benchmarkResults.reduce((min, r) => r.speedup < min.speedup ? r : min);
    const binaryWins = benchmarkResults.filter(r => r.speedup >= 1).length;

    console.log('│' + ' SUMMARY'.padEnd(98) + '│');
    console.log('├' + '─'.repeat(98) + '┤');
    console.log('│' + `  Average Speedup: ${avgSpeedup.toFixed(2)}x`.padEnd(98) + '│');
    console.log('│' + `  Best Case: ${maxResult.speedup.toFixed(2)}x (${maxResult.operation})`.padEnd(98) + '│');
    console.log('│' + `  Worst Case: ${minResult.speedup.toFixed(2)}x (${minResult.operation})`.padEnd(98) + '│');
    console.log('│' + `  Binary Wins: ${binaryWins}/${benchmarkResults.length} tests`.padEnd(98) + '│');
    console.log('└' + '─'.repeat(98) + '┘');
    console.log('\n');
  });
});
