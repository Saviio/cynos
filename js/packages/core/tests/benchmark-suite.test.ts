/**
 * Cynos Database - 完整性能基准测试套件
 *
 * 测试分类：
 * 1. 写入性能 - 单行插入、批量插入、并发插入、长时间稳定性
 * 2. 查询性能 - 点查询、范围查询、排序+LIMIT、复杂条件、空结果集
 * 3. 实时查询 - 小/大结果集、多查询共存、高频更新、取消订阅
 * 4. 混合工作负载 - 读写混合、峰值冲击、后台同步模拟
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

// ============================================================================
// 测试结果收集
// ============================================================================
interface TestResult {
  category: string;
  testCase: string;
  metric: string;
  value: string | number;
}

const testResults: TestResult[] = [];

function addResult(category: string, testCase: string, metric: string, value: string | number) {
  testResults.push({ category, testCase, metric, value });
}

function printTable(category: string) {
  const results = testResults.filter(r => r.category === category);
  if (results.length === 0) return;

  console.log(`\n${'='.repeat(100)}`);
  console.log(`${category}`);
  console.log(`${'='.repeat(100)}`);
  console.log(`| ${'测试用例'.padEnd(30)} | ${'指标'.padEnd(25)} | ${'结果'.padStart(20)} |`);
  console.log(`|${'-'.repeat(32)}|${'-'.repeat(27)}|${'-'.repeat(22)}|`);

  for (const r of results) {
    console.log(`| ${r.testCase.padEnd(30)} | ${r.metric.padEnd(25)} | ${String(r.value).padStart(20)} |`);
  }
}

// ============================================================================
// 辅助函数
// ============================================================================

// 窄表 (5列)
function createNarrowDb(name: string) {
  const db = new Database(name);
  const builder = db.createTable('items')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('name', JsDataType.String, null)
    .column('value', JsDataType.Int64, null)
    .column('active', JsDataType.Bool, null)
    .column('score', JsDataType.Float64, null)
    .index('idx_value', 'value')
    .index('idx_score', 'score');
  db.registerTable(builder);
  return db;
}

// 宽表 (20列)
function createWideDb(name: string) {
  const db = new Database(name);
  let builder = db.createTable('items')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('name', JsDataType.String, null)
    .column('category', JsDataType.String, null)
    .column('status', JsDataType.String, null)
    .column('priority', JsDataType.Int32, null)
    .column('value1', JsDataType.Int64, null)
    .column('value2', JsDataType.Int64, null)
    .column('value3', JsDataType.Int64, null)
    .column('score1', JsDataType.Float64, null)
    .column('score2', JsDataType.Float64, null)
    .column('active', JsDataType.Bool, null)
    .column('verified', JsDataType.Bool, null)
    .column('desc1', JsDataType.String, null)
    .column('desc2', JsDataType.String, null)
    .column('desc3', JsDataType.String, null)
    .column('tag1', JsDataType.String, null)
    .column('tag2', JsDataType.String, null)
    .column('count1', JsDataType.Int32, null)
    .column('count2', JsDataType.Int32, null)
    .column('rating', JsDataType.Float64, null)
    .index('idx_category', 'category')
    .index('idx_value1', 'value1');
  db.registerTable(builder);
  return db;
}

function generateNarrowRow(id: number) {
  return {
    id,
    name: `Item ${id}`,
    value: id * 100,
    active: id % 2 === 0,
    score: id * 0.1,
  };
}

function generateWideRow(id: number) {
  return {
    id,
    name: `Item ${id}`,
    category: ['A', 'B', 'C', 'D', 'E'][id % 5],
    status: ['active', 'pending', 'done'][id % 3],
    priority: id % 10,
    value1: id * 100,
    value2: id * 200,
    value3: id * 300,
    score1: id * 0.1,
    score2: id * 0.2,
    active: id % 2 === 0,
    verified: id % 3 === 0,
    desc1: `Description 1 for item ${id}`,
    desc2: `Description 2 for item ${id}`,
    desc3: `Description 3 for item ${id}`,
    tag1: `tag-${id % 100}`,
    tag2: `tag-${id % 50}`,
    count1: id % 1000,
    count2: id % 500,
    rating: (id % 50) / 10,
  };
}

function percentile(arr: number[], p: number): number {
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

// 格式化时间，对于亚毫秒级操作使用微秒
function formatTime(ms: number): string {
  if (ms < 0.01) {
    return `${(ms * 1000).toFixed(1)}μs`;
  } else if (ms < 1) {
    return `${(ms * 1000).toFixed(0)}μs`;
  } else if (ms < 10) {
    return `${ms.toFixed(2)}ms`;
  } else if (ms < 100) {
    return `${ms.toFixed(1)}ms`;
  } else if (ms < 1000) {
    return `${ms.toFixed(0)}ms`;
  } else {
    return `${(ms / 1000).toFixed(2)}s`;
  }
}

// ============================================================================
// 1. 写入性能测试
// ============================================================================
describe('1. 写入性能', () => {

  it('1.1 单行插入延迟分布', async () => {
    const db = createNarrowDb('write_single_latency');
    const latencies: number[] = [];

    for (let i = 1; i <= 1000; i++) {
      const start = performance.now();
      await db.insert('items').values([generateNarrowRow(i)]).exec();
      latencies.push(performance.now() - start);
    }

    const p50 = percentile(latencies, 50);
    const p95 = percentile(latencies, 95);
    const p99 = percentile(latencies, 99);
    const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    const throughput = 1000 / (latencies.reduce((a, b) => a + b, 0) / 1000);

    addResult('1. 写入性能', '单行插入延迟', 'P50', formatTime(p50));
    addResult('1. 写入性能', '单行插入延迟', 'P95', formatTime(p95));
    addResult('1. 写入性能', '单行插入延迟', 'P99', formatTime(p99));
    addResult('1. 写入性能', '单行插入延迟', '平均延迟', formatTime(avg));
    addResult('1. 写入性能', '单行插入延迟', '吞吐量', `${throughput.toFixed(0)} rows/s`);
  });

  it('1.2 批量插入 - 不同批次大小', async () => {
    const batchSizes = [10, 100, 1000, 10000];
    const totalRows = 100000;

    for (const batchSize of batchSizes) {
      const db = createNarrowDb(`write_batch_${batchSize}`);
      const batches = Math.ceil(totalRows / batchSize);

      const start = performance.now();
      for (let b = 0; b < batches; b++) {
        const rows = [];
        for (let i = 0; i < batchSize && (b * batchSize + i) < totalRows; i++) {
          rows.push(generateNarrowRow(b * batchSize + i + 1));
        }
        await db.insert('items').values(rows).exec();
      }
      const duration = performance.now() - start;
      const throughput = totalRows / (duration / 1000);

      addResult('1. 写入性能', `批量插入 (batch=${batchSize})`, '总耗时', formatTime(duration));
      addResult('1. 写入性能', `批量插入 (batch=${batchSize})`, '吞吐量', `${throughput.toFixed(0)} rows/s`);
    }
  }, 120000);

  it('1.3 数据大小影响 - 窄表 vs 宽表', async () => {
    const rowCount = 10000;

    // 窄表 (5列)
    const narrowDb = createNarrowDb('write_narrow');
    const narrowRows = Array.from({ length: rowCount }, (_, i) => generateNarrowRow(i + 1));
    const narrowStart = performance.now();
    await narrowDb.insert('items').values(narrowRows).exec();
    const narrowDuration = performance.now() - narrowStart;

    // 宽表 (20列)
    const wideDb = createWideDb('write_wide');
    const wideRows = Array.from({ length: rowCount }, (_, i) => generateWideRow(i + 1));
    const wideStart = performance.now();
    await wideDb.insert('items').values(wideRows).exec();
    const wideDuration = performance.now() - wideStart;

    addResult('1. 写入性能', '窄表 (5列)', '插入 1万行耗时', formatTime(narrowDuration));
    addResult('1. 写入性能', '窄表 (5列)', '吞吐量', `${(rowCount / narrowDuration * 1000).toFixed(0)} rows/s`);
    addResult('1. 写入性能', '宽表 (20列)', '插入 1万行耗时', formatTime(wideDuration));
    addResult('1. 写入性能', '宽表 (20列)', '吞吐量', `${(rowCount / wideDuration * 1000).toFixed(0)} rows/s`);
    addResult('1. 写入性能', '宽表 vs 窄表', '耗时比', `${(wideDuration / narrowDuration).toFixed(2)}x`);
  });

  it('1.4 并发插入', async () => {
    // 测试1: 100个并发请求，每个插入1行
    const db1 = createNarrowDb('write_concurrent_single');
    await db1.insert('items').values([generateNarrowRow(1)]).exec(); // 预热

    const stream1 = db1.select('*').from('items').changes();
    let notifyCount1 = 0;
    const unsub1 = stream1.subscribe(() => { notifyCount1++; });

    const start1 = performance.now();
    const promises1 = [];
    for (let i = 0; i < 100; i++) {
      promises1.push(db1.insert('items').values([generateNarrowRow(100 + i)]).exec());
    }
    await Promise.all(promises1);
    await new Promise(r => setTimeout(r, 20));
    const duration1 = performance.now() - start1;
    unsub1();

    // 测试2: 100个并发请求，每个插入100行
    const db2 = createNarrowDb('write_concurrent_batch');
    await db2.insert('items').values([generateNarrowRow(1)]).exec();

    const stream2 = db2.select('*').from('items').changes();
    let notifyCount2 = 0;
    const unsub2 = stream2.subscribe(() => { notifyCount2++; });

    const start2 = performance.now();
    const promises2 = [];
    for (let i = 0; i < 100; i++) {
      const rows = Array.from({ length: 100 }, (_, j) => generateNarrowRow(1000 + i * 100 + j));
      promises2.push(db2.insert('items').values(rows).exec());
    }
    await Promise.all(promises2);
    await new Promise(r => setTimeout(r, 20));
    const duration2 = performance.now() - start2;
    unsub2();

    addResult('1. 写入性能', '并发插入 (100x1行)', '总耗时', formatTime(duration1));
    addResult('1. 写入性能', '并发插入 (100x1行)', '通知次数', `${notifyCount1}`);
    addResult('1. 写入性能', '并发插入 (100x100行)', '总耗时', formatTime(duration2));
    addResult('1. 写入性能', '并发插入 (100x100行)', '通知次数', `${notifyCount2}`);
    addResult('1. 写入性能', '并发插入 (100x100行)', '吞吐量', `${(10000 / duration2 * 1000).toFixed(0)} rows/s`);
  });

  it('1.5 长时间写入稳定性', async () => {
    const db = createNarrowDb('write_stability');
    const totalRows = 500000; // 50万行
    const batchSize = 10000;
    const batches = totalRows / batchSize;

    const batchTimes: number[] = [];
    const totalStart = performance.now();

    for (let b = 0; b < batches; b++) {
      const rows = Array.from({ length: batchSize }, (_, i) => generateNarrowRow(b * batchSize + i + 1));
      const batchStart = performance.now();
      await db.insert('items').values(rows).exec();
      batchTimes.push(performance.now() - batchStart);
    }

    const totalDuration = performance.now() - totalStart;
    const firstBatchAvg = batchTimes.slice(0, 5).reduce((a, b) => a + b, 0) / 5;
    const lastBatchAvg = batchTimes.slice(-5).reduce((a, b) => a + b, 0) / 5;

    addResult('1. 写入性能', '长时间写入 (50万行)', '总耗时', formatTime(totalDuration));
    addResult('1. 写入性能', '长时间写入 (50万行)', '平均吞吐量', `${(totalRows / totalDuration * 1000).toFixed(0)} rows/s`);
    addResult('1. 写入性能', '长时间写入 (50万行)', '前5批平均耗时', formatTime(firstBatchAvg));
    addResult('1. 写入性能', '长时间写入 (50万行)', '后5批平均耗时', formatTime(lastBatchAvg));
    addResult('1. 写入性能', '长时间写入 (50万行)', '性能衰减比', `${(lastBatchAvg / firstBatchAvg).toFixed(2)}x`);
  }, 300000);

});

// ============================================================================
// 2. 查询性能测试
// ============================================================================
describe('2. 查询性能', () => {

  it('2.1 点查询 - 不同数据规模', async () => {
    const sizes = [10000, 100000, 500000];

    for (const size of sizes) {
      const db = createNarrowDb(`query_point_${size}`);
      // 批量插入
      const batchSize = 10000;
      for (let b = 0; b < size / batchSize; b++) {
        const rows = Array.from({ length: batchSize }, (_, i) => generateNarrowRow(b * batchSize + i + 1));
        await db.insert('items').values(rows).exec();
      }

      // 点查询测试 (100次)
      const latencies: number[] = [];
      for (let i = 0; i < 100; i++) {
        const targetId = Math.floor(Math.random() * size) + 1;
        const start = performance.now();
        await db.select('*').from('items').where(col('id').eq(targetId)).exec();
        latencies.push(performance.now() - start);
      }

      const avg = latencies.reduce((a, b) => a + b, 0) / latencies.length;
      addResult('2. 查询性能', `点查询 (${(size/1000)}K行)`, '平均延迟', formatTime(avg));
    }
  }, 300000);

  it('2.2 范围查询 - 不同选择度', async () => {
    const db = createNarrowDb('query_range');
    const size = 100000;

    // 插入数据
    for (let b = 0; b < size / 10000; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateNarrowRow(b * 10000 + i + 1));
      await db.insert('items').values(rows).exec();
    }

    // 0.1% 选择度
    let start = performance.now();
    let result = await db.select('*').from('items').where(col('value').gt(9990000)).exec();
    let duration = performance.now() - start;
    addResult('2. 查询性能', '范围查询 (0.1%选择度)', '返回行数', result.length);
    addResult('2. 查询性能', '范围查询 (0.1%选择度)', '耗时', formatTime(duration));

    // 1% 选择度
    start = performance.now();
    result = await db.select('*').from('items').where(col('value').gt(9900000)).exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '范围查询 (1%选择度)', '返回行数', result.length);
    addResult('2. 查询性能', '范围查询 (1%选择度)', '耗时', formatTime(duration));

    // 10% 选择度
    start = performance.now();
    result = await db.select('*').from('items').where(col('value').gt(9000000)).exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '范围查询 (10%选择度)', '返回行数', result.length);
    addResult('2. 查询性能', '范围查询 (10%选择度)', '耗时', formatTime(duration));

    // 50% 选择度
    start = performance.now();
    result = await db.select('*').from('items').where(col('value').gt(5000000)).exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '范围查询 (50%选择度)', '返回行数', result.length);
    addResult('2. 查询性能', '范围查询 (50%选择度)', '耗时', formatTime(duration));

    // 90% 选择度
    start = performance.now();
    result = await db.select('*').from('items').where(col('value').gt(1000000)).exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '范围查询 (90%选择度)', '返回行数', result.length);
    addResult('2. 查询性能', '范围查询 (90%选择度)', '耗时', formatTime(duration));
  }, 120000);

  it('2.2.1 序列化开销分析', async () => {
    const db = createNarrowDb('query_serialization_overhead');
    const size = 100000;

    // 插入数据
    for (let b = 0; b < size / 10000; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateNarrowRow(b * 10000 + i + 1));
      await db.insert('items').values(rows).exec();
    }

    // 使用 benchmarkRangeQuery 测量序列化开销
    // 0.1% 选择度 (100 行)
    let benchmark = db.benchmarkRangeQuery('items', 'value', 9990000);
    addResult('2. 查询性能', '序列化开销 (0.1%/100行)', 'WASM查询', formatTime(benchmark.query_ms));
    addResult('2. 查询性能', '序列化开销 (0.1%/100行)', '序列化', formatTime(benchmark.serialize_ms));
    addResult('2. 查询性能', '序列化开销 (0.1%/100行)', '序列化占比', `${benchmark.serialization_overhead_pct.toFixed(1)}%`);

    // 1% 选择度 (1000 行)
    benchmark = db.benchmarkRangeQuery('items', 'value', 9900000);
    addResult('2. 查询性能', '序列化开销 (1%/1000行)', 'WASM查询', formatTime(benchmark.query_ms));
    addResult('2. 查询性能', '序列化开销 (1%/1000行)', '序列化', formatTime(benchmark.serialize_ms));
    addResult('2. 查询性能', '序列化开销 (1%/1000行)', '序列化占比', `${benchmark.serialization_overhead_pct.toFixed(1)}%`);

    // 10% 选择度 (10000 行)
    benchmark = db.benchmarkRangeQuery('items', 'value', 9000000);
    addResult('2. 查询性能', '序列化开销 (10%/1万行)', 'WASM查询', formatTime(benchmark.query_ms));
    addResult('2. 查询性能', '序列化开销 (10%/1万行)', '序列化', formatTime(benchmark.serialize_ms));
    addResult('2. 查询性能', '序列化开销 (10%/1万行)', '序列化占比', `${benchmark.serialization_overhead_pct.toFixed(1)}%`);

    // 50% 选择度 (50000 行)
    benchmark = db.benchmarkRangeQuery('items', 'value', 5000000);
    addResult('2. 查询性能', '序列化开销 (50%/5万行)', 'WASM查询', formatTime(benchmark.query_ms));
    addResult('2. 查询性能', '序列化开销 (50%/5万行)', '序列化', formatTime(benchmark.serialize_ms));
    addResult('2. 查询性能', '序列化开销 (50%/5万行)', '序列化占比', `${benchmark.serialization_overhead_pct.toFixed(1)}%`);
  }, 120000);

  it('2.3 排序 + LIMIT', async () => {
    const db = createNarrowDb('query_sort_limit');
    const size = 100000;

    for (let b = 0; b < size / 10000; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateNarrowRow(b * 10000 + i + 1));
      await db.insert('items').values(rows).exec();
    }

    // LIMIT 10
    let start = performance.now();
    await db.select('*').from('items').orderBy('score', JsSortOrder.Desc).limit(10).exec();
    let duration = performance.now() - start;
    addResult('2. 查询性能', '排序+LIMIT 10', '耗时', formatTime(duration));

    // LIMIT 100
    start = performance.now();
    await db.select('*').from('items').orderBy('score', JsSortOrder.Desc).limit(100).exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '排序+LIMIT 100', '耗时', formatTime(duration));

    // LIMIT 1000
    start = performance.now();
    await db.select('*').from('items').orderBy('score', JsSortOrder.Desc).limit(1000).exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '排序+LIMIT 1000', '耗时', formatTime(duration));

    // 无 LIMIT (全量排序)
    start = performance.now();
    await db.select('*').from('items').orderBy('score', JsSortOrder.Desc).exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '排序 (无LIMIT)', '耗时', formatTime(duration));
  }, 120000);

  it('2.4 复杂条件查询', async () => {
    const db = createWideDb('query_complex');
    const size = 50000;

    for (let b = 0; b < size / 10000; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateWideRow(b * 10000 + i + 1));
      await db.insert('items').values(rows).exec();
    }

    // AND 条件
    let start = performance.now();
    let result = await db.select('*').from('items')
      .where(col('category').eq('A').and(col('priority').gt(5)))
      .exec();
    let duration = performance.now() - start;
    addResult('2. 查询性能', '复杂查询 (AND)', '返回行数', result.length);
    addResult('2. 查询性能', '复杂查询 (AND)', '耗时', formatTime(duration));

    // OR 条件
    start = performance.now();
    result = await db.select('*').from('items')
      .where(col('category').eq('A').or(col('category').eq('B')))
      .exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '复杂查询 (OR)', '返回行数', result.length);
    addResult('2. 查询性能', '复杂查询 (OR)', '耗时', formatTime(duration));

    // LIKE 模糊匹配
    start = performance.now();
    result = await db.select('*').from('items')
      .where(col('name').like('%100%'))
      .exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '复杂查询 (LIKE)', '返回行数', result.length);
    addResult('2. 查询性能', '复杂查询 (LIKE)', '耗时', formatTime(duration));

    // 多条件组合
    start = performance.now();
    result = await db.select('*').from('items')
      .where(
        col('category').eq('A')
          .and(col('active').eq(true))
          .and(col('value1').gt(100000))
          .and(col('priority').lt(5))
      )
      .exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '复杂查询 (多条件组合)', '返回行数', result.length);
    addResult('2. 查询性能', '复杂查询 (多条件组合)', '耗时', formatTime(duration));
  }, 120000);

  it('2.5 空结果集查询', async () => {
    // 空表查询
    const emptyDb = createNarrowDb('query_empty_table');
    let start = performance.now();
    let result = await emptyDb.select('*').from('items').where(col('id').eq(999)).exec();
    let duration = performance.now() - start;
    addResult('2. 查询性能', '空结果集 (空表)', '耗时', formatTime(duration));

    // 大表查询无匹配
    const largeDb = createNarrowDb('query_empty_result');
    const size = 100000;
    for (let b = 0; b < size / 10000; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateNarrowRow(b * 10000 + i + 1));
      await largeDb.insert('items').values(rows).exec();
    }

    start = performance.now();
    result = await largeDb.select('*').from('items').where(col('id').eq(999999999)).exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '空结果集 (10万行表)', '耗时', formatTime(duration));

    // 范围查询无匹配
    start = performance.now();
    result = await largeDb.select('*').from('items').where(col('value').gt(999999999)).exec();
    duration = performance.now() - start;
    addResult('2. 查询性能', '空结果集 (范围查询)', '耗时', formatTime(duration));
  }, 120000);

});

// ============================================================================
// 3. 实时查询性能测试
// ============================================================================
describe('3. 实时查询', () => {

  it('3.1 小结果集实时查询', async () => {
    const db = createNarrowDb('live_small');
    await db.insert('items').values(Array.from({ length: 1000 }, (_, i) => generateNarrowRow(i + 1))).exec();

    const stream = db.select('*').from('items').where(col('active').eq(true)).limit(10).changes();
    let notifyCount = 0;
    const unsub = stream.subscribe(() => { notifyCount++; });

    // 插入 1000 行
    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      const rows = Array.from({ length: 10 }, (_, j) => generateNarrowRow(10000 + i * 10 + j));
      await db.insert('items').values(rows).exec();
    }
    const duration = performance.now() - start;
    unsub();

    addResult('3. 实时查询', '小结果集 (LIMIT 10)', '插入1000行总耗时', formatTime(duration));
    addResult('3. 实时查询', '小结果集 (LIMIT 10)', '通知次数', notifyCount);
    addResult('3. 实时查询', '小结果集 (LIMIT 10)', '平均每次插入耗时', formatTime(duration / 100));
  });

  it('3.2 大结果集实时查询', async () => {
    const db = createNarrowDb('live_large');
    // 插入 2万行初始数据
    for (let b = 0; b < 2; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateNarrowRow(b * 10000 + i + 1));
      await db.insert('items').values(rows).exec();
    }

    const stream = db.select('*').from('items').where(col('active').eq(true)).changes();
    let notifyCount = 0;
    const unsub = stream.subscribe(() => { notifyCount++; });

    const initialNotify = notifyCount;

    // 插入 100 行
    const latencies: number[] = [];
    for (let i = 0; i < 100; i++) {
      const start = performance.now();
      await db.insert('items').values([generateNarrowRow(100000 + i)]).exec();
      latencies.push(performance.now() - start);
    }
    unsub();

    const totalDuration = latencies.reduce((a, b) => a + b, 0);
    const avgLatency = totalDuration / 100;

    addResult('3. 实时查询', '大结果集 (2万行)', '插入100行总耗时', formatTime(totalDuration));
    addResult('3. 实时查询', '大结果集 (2万行)', '通知次数', notifyCount - initialNotify);
    addResult('3. 实时查询', '大结果集 (2万行)', '平均每次插入耗时', formatTime(avgLatency));
  }, 60000);

  it('3.3 多实时查询共存', async () => {
    const db = createWideDb('live_multi');
    // 插入初始数据
    for (let b = 0; b < 2; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateWideRow(b * 10000 + i + 1));
      await db.insert('items').values(rows).exec();
    }

    // 创建 5 个不同的实时查询
    const streams: { unsub: () => void; count: number }[] = [];
    const categories = ['A', 'B', 'C', 'D', 'E'];

    for (const cat of categories) {
      const stream = db.select('*').from('items').where(col('category').eq(cat)).limit(100).changes();
      const state = { unsub: () => {}, count: 0 };
      state.unsub = stream.subscribe(() => { state.count++; });
      streams.push(state);
    }

    // 执行 100 次插入
    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      await db.insert('items').values([generateWideRow(100000 + i)]).exec();
    }
    const duration = performance.now() - start;

    const totalNotifications = streams.reduce((sum, s) => sum + s.count, 0);
    streams.forEach(s => s.unsub());

    addResult('3. 实时查询', '多查询共存 (5个)', '插入100行总耗时', formatTime(duration));
    addResult('3. 实时查询', '多查询共存 (5个)', '总通知次数', totalNotifications);
    addResult('3. 实时查询', '多查询共存 (5个)', '平均每次插入耗时', formatTime(duration / 100));
  }, 60000);

  it('3.4 高频率更新', async () => {
    const db = createNarrowDb('live_highfreq');
    await db.insert('items').values(Array.from({ length: 1000 }, (_, i) => generateNarrowRow(i + 1))).exec();

    const stream = db.select('*').from('items').limit(10).changes();
    let notifyCount = 0;
    const unsub = stream.subscribe(() => { notifyCount++; });

    // 快速连续插入 100 条 (不等待)
    const start = performance.now();
    const promises = [];
    for (let i = 0; i < 100; i++) {
      promises.push(db.insert('items').values([generateNarrowRow(10000 + i)]).exec());
    }
    await Promise.all(promises);
    await new Promise(r => setTimeout(r, 50)); // 等待所有通知完成
    const duration = performance.now() - start;
    unsub();

    addResult('3. 实时查询', '高频更新 (100次并发)', '总耗时', formatTime(duration));
    addResult('3. 实时查询', '高频更新 (100次并发)', '通知次数', notifyCount);
    addResult('3. 实时查询', '高频更新 (100次并发)', '通知合并率', `${((100 - notifyCount + 1) / 100 * 100).toFixed(0)}%`);
  });

  it('3.5 实时查询取消', async () => {
    const db = createNarrowDb('live_cancel');
    await db.insert('items').values(Array.from({ length: 1000 }, (_, i) => generateNarrowRow(i + 1))).exec();

    const stream = db.select('*').from('items').changes();
    let notifyCount = 0;
    const unsub = stream.subscribe(() => { notifyCount++; });

    // 插入一些数据
    for (let i = 0; i < 10; i++) {
      await db.insert('items').values([generateNarrowRow(10000 + i)]).exec();
    }
    const countBeforeCancel = notifyCount;

    // 取消订阅
    unsub();

    // 继续插入
    for (let i = 0; i < 10; i++) {
      await db.insert('items').values([generateNarrowRow(20000 + i)]).exec();
    }
    const countAfterCancel = notifyCount;

    addResult('3. 实时查询', '取消订阅', '取消前通知次数', countBeforeCancel);
    addResult('3. 实时查询', '取消订阅', '取消后通知次数', countAfterCancel - countBeforeCancel);
    addResult('3. 实时查询', '取消订阅', '资源释放正确', countAfterCancel === countBeforeCancel ? '是' : '否');
  });

});

// ============================================================================
// 4. 混合工作负载测试
// ============================================================================
describe('4. 混合工作负载', () => {

  it('4.1 读写混合 1:1', async () => {
    const db = createWideDb('mixed_rw');
    // 初始数据
    for (let b = 0; b < 5; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateWideRow(b * 10000 + i + 1));
      await db.insert('items').values(rows).exec();
    }

    const writeLatencies: number[] = [];
    const readLatencies: number[] = [];
    const duration = 5000; // 5秒测试
    const startTime = performance.now();
    let writeCount = 0;
    let readCount = 0;

    // 模拟读写混合
    while (performance.now() - startTime < duration) {
      // 写入
      const writeStart = performance.now();
      await db.insert('items').values([generateWideRow(100000 + writeCount)]).exec();
      writeLatencies.push(performance.now() - writeStart);
      writeCount++;

      // 读取
      const readStart = performance.now();
      await db.select('*').from('items').where(col('category').eq('A')).limit(100).exec();
      readLatencies.push(performance.now() - readStart);
      readCount++;
    }

    const avgWriteLatency = writeLatencies.reduce((a, b) => a + b, 0) / writeLatencies.length;
    const avgReadLatency = readLatencies.reduce((a, b) => a + b, 0) / readLatencies.length;

    addResult('4. 混合工作负载', '读写混合 1:1 (5秒)', '写入次数', writeCount);
    addResult('4. 混合工作负载', '读写混合 1:1 (5秒)', '读取次数', readCount);
    addResult('4. 混合工作负载', '读写混合 1:1 (5秒)', '平均写入延迟', formatTime(avgWriteLatency));
    addResult('4. 混合工作负载', '读写混合 1:1 (5秒)', '平均读取延迟', formatTime(avgReadLatency));
  }, 30000);

  it('4.2 峰值冲击', async () => {
    const db = createWideDb('mixed_burst');
    // 初始数据
    for (let b = 0; b < 5; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateWideRow(b * 10000 + i + 1));
      await db.insert('items').values(rows).exec();
    }

    // 创建实时查询
    const streams: { unsub: () => void; count: number }[] = [];
    for (let i = 0; i < 10; i++) {
      const stream = db.select('*').from('items').where(col('category').eq(['A', 'B', 'C', 'D', 'E'][i % 5])).limit(50).changes();
      const state = { unsub: () => {}, count: 0 };
      state.unsub = stream.subscribe(() => { state.count++; });
      streams.push(state);
    }

    // 突发插入 1万行
    const burstStart = performance.now();
    const rows = Array.from({ length: 10000 }, (_, i) => generateWideRow(100000 + i));
    await db.insert('items').values(rows).exec();
    const burstDuration = performance.now() - burstStart;

    const totalNotifications = streams.reduce((sum, s) => sum + s.count, 0);
    streams.forEach(s => s.unsub());

    addResult('4. 混合工作负载', '峰值冲击 (1万行)', '插入耗时', formatTime(burstDuration));
    addResult('4. 混合工作负载', '峰值冲击 (1万行)', '吞吐量', `${(10000 / burstDuration * 1000).toFixed(0)} rows/s`);
    addResult('4. 混合工作负载', '峰值冲击 (1万行)', '10个查询总通知', totalNotifications);
  }, 60000);

  it('4.3 后台同步模拟', async () => {
    const db = createWideDb('mixed_sync');
    // 初始数据
    for (let b = 0; b < 2; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateWideRow(b * 10000 + i + 1));
      await db.insert('items').values(rows).exec();
    }

    // 用户查询
    const userQueryLatencies: number[] = [];

    // 模拟后台同步 (每批 5000 行) + 用户交互查询
    const syncBatches = 3;
    const batchSize = 5000;

    for (let batch = 0; batch < syncBatches; batch++) {
      // 后台同步
      const syncRows = Array.from({ length: batchSize }, (_, i) => generateWideRow(100000 + batch * batchSize + i));
      const syncPromise = db.insert('items').values(syncRows).exec();

      // 同时执行用户查询
      for (let q = 0; q < 5; q++) {
        const queryStart = performance.now();
        await db.select('*').from('items').where(col('category').eq('A')).limit(20).exec();
        userQueryLatencies.push(performance.now() - queryStart);
      }

      await syncPromise;
    }

    const avgUserLatency = userQueryLatencies.reduce((a, b) => a + b, 0) / userQueryLatencies.length;
    const maxUserLatency = Math.max(...userQueryLatencies);

    addResult('4. 混合工作负载', '后台同步模拟', '同步批次', `${syncBatches} x ${batchSize}行`);
    addResult('4. 混合工作负载', '后台同步模拟', '用户查询次数', userQueryLatencies.length);
    addResult('4. 混合工作负载', '后台同步模拟', '平均用户查询延迟', formatTime(avgUserLatency));
    addResult('4. 混合工作负载', '后台同步模拟', '最大用户查询延迟', formatTime(maxUserLatency));
  }, 60000);

});

// ============================================================================
// 5. GIN 索引性能测试
// ============================================================================
describe('5. GIN 索引性能', () => {

  // JSONB 表
  function createJsonbDb(name: string, withIndex: boolean) {
    const db = new Database(name);
    let builder = db.createTable('documents')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('title', JsDataType.String, null)
      .column('metadata', JsDataType.Jsonb, null);

    if (withIndex) {
      builder = builder.index('idx_metadata', 'metadata');
    }

    db.registerTable(builder);
    return db;
  }

  function generateJsonbRow(id: number) {
    const categories = ['tech', 'business', 'science', 'health', 'sports'];
    const statuses = ['published', 'draft', 'archived'];
    const priorities = [1, 2, 3, 4, 5];

    return {
      id,
      title: `Document ${id}`,
      metadata: {
        category: categories[id % categories.length],
        status: statuses[id % statuses.length],
        priority: priorities[id % priorities.length],
        views: id * 10,
        author: `Author ${id % 100}`,
      },
    };
  }

  it('5.1 单谓词 GIN 查询对比', async () => {
    const size = 50000;

    // 无索引
    const dbNoIdx = createJsonbDb('gin_single_no_idx', false);
    for (let b = 0; b < size / 10000; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateJsonbRow(b * 10000 + i + 1));
      await dbNoIdx.insert('documents').values(rows).exec();
    }

    let start = performance.now();
    let result = await dbNoIdx.select('*').from('documents')
      .where(col('metadata').get('$.category').eq('tech'))
      .exec();
    let noIdxDuration = performance.now() - start;

    addResult('5. GIN 索引性能', '单谓词查询 (无索引)', '返回行数', result.length);
    addResult('5. GIN 索引性能', '单谓词查询 (无索引)', '耗时', formatTime(noIdxDuration));

    // 有索引
    const dbWithIdx = createJsonbDb('gin_single_with_idx', true);
    for (let b = 0; b < size / 10000; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateJsonbRow(b * 10000 + i + 1));
      await dbWithIdx.insert('documents').values(rows).exec();
    }

    start = performance.now();
    result = await dbWithIdx.select('*').from('documents')
      .where(col('metadata').get('$.category').eq('tech'))
      .exec();
    let withIdxDuration = performance.now() - start;

    addResult('5. GIN 索引性能', '单谓词查询 (GIN索引)', '返回行数', result.length);
    addResult('5. GIN 索引性能', '单谓词查询 (GIN索引)', '耗时', formatTime(withIdxDuration));
    addResult('5. GIN 索引性能', '单谓词查询', '加速比', `${(noIdxDuration / withIdxDuration).toFixed(1)}x`);
  }, 120000);

  it('5.2 GIN Predicate Combination (多谓词 AND)', async () => {
    const size = 50000;

    // 无索引
    const dbNoIdx = createJsonbDb('gin_multi_no_idx', false);
    for (let b = 0; b < size / 10000; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateJsonbRow(b * 10000 + i + 1));
      await dbNoIdx.insert('documents').values(rows).exec();
    }

    // 2 谓词 AND
    let start = performance.now();
    let result = await dbNoIdx.select('*').from('documents')
      .where(
        col('metadata').get('$.category').eq('tech')
          .and(col('metadata').get('$.status').eq('published'))
      )
      .exec();
    let noIdx2Duration = performance.now() - start;

    addResult('5. GIN 索引性能', '2谓词AND (无索引)', '返回行数', result.length);
    addResult('5. GIN 索引性能', '2谓词AND (无索引)', '耗时', formatTime(noIdx2Duration));

    // 3 谓词 AND
    start = performance.now();
    result = await dbNoIdx.select('*').from('documents')
      .where(
        col('metadata').get('$.category').eq('tech')
          .and(col('metadata').get('$.status').eq('published'))
          .and(col('metadata').get('$.priority').eq(1))
      )
      .exec();
    let noIdx3Duration = performance.now() - start;

    addResult('5. GIN 索引性能', '3谓词AND (无索引)', '返回行数', result.length);
    addResult('5. GIN 索引性能', '3谓词AND (无索引)', '耗时', formatTime(noIdx3Duration));

    // 有索引
    const dbWithIdx = createJsonbDb('gin_multi_with_idx', true);
    for (let b = 0; b < size / 10000; b++) {
      const rows = Array.from({ length: 10000 }, (_, i) => generateJsonbRow(b * 10000 + i + 1));
      await dbWithIdx.insert('documents').values(rows).exec();
    }

    // 2 谓词 AND (GIN Predicate Combination)
    start = performance.now();
    result = await dbWithIdx.select('*').from('documents')
      .where(
        col('metadata').get('$.category').eq('tech')
          .and(col('metadata').get('$.status').eq('published'))
      )
      .exec();
    let withIdx2Duration = performance.now() - start;

    addResult('5. GIN 索引性能', '2谓词AND (GIN索引)', '返回行数', result.length);
    addResult('5. GIN 索引性能', '2谓词AND (GIN索引)', '耗时', formatTime(withIdx2Duration));
    addResult('5. GIN 索引性能', '2谓词AND', '加速比', `${(noIdx2Duration / withIdx2Duration).toFixed(1)}x`);

    // 3 谓词 AND (GIN Predicate Combination)
    start = performance.now();
    result = await dbWithIdx.select('*').from('documents')
      .where(
        col('metadata').get('$.category').eq('tech')
          .and(col('metadata').get('$.status').eq('published'))
          .and(col('metadata').get('$.priority').eq(1))
      )
      .exec();
    let withIdx3Duration = performance.now() - start;

    addResult('5. GIN 索引性能', '3谓词AND (GIN索引)', '返回行数', result.length);
    addResult('5. GIN 索引性能', '3谓词AND (GIN索引)', '耗时', formatTime(withIdx3Duration));
    addResult('5. GIN 索引性能', '3谓词AND', '加速比', `${(noIdx3Duration / withIdx3Duration).toFixed(1)}x`);
  }, 120000);

  it('5.3 GIN 查询计划验证', async () => {
    const db = createJsonbDb('gin_plan_verify', true);
    await db.insert('documents').values(Array.from({ length: 1000 }, (_, i) => generateJsonbRow(i + 1))).exec();

    // 单谓词应使用 GinIndexScan
    const singlePlan = db.select('*').from('documents')
      .where(col('metadata').get('$.category').eq('tech'))
      .explain();

    const usesSingleGin = singlePlan.physical.includes('GinIndexScan');
    addResult('5. GIN 索引性能', '查询计划 (单谓词)', '使用 GinIndexScan', usesSingleGin ? '是' : '否');

    // 多谓词应使用 GinIndexScanMulti
    const multiPlan = db.select('*').from('documents')
      .where(
        col('metadata').get('$.category').eq('tech')
          .and(col('metadata').get('$.status').eq('published'))
      )
      .explain();

    const usesMultiGin = multiPlan.physical.includes('GinIndexScanMulti');
    addResult('5. GIN 索引性能', '查询计划 (多谓词)', '使用 GinIndexScanMulti', usesMultiGin ? '是' : '否');
  });

});

// ============================================================================
// 测试结果汇总
// ============================================================================
describe('测试结果汇总', () => {
  it('打印所有测试结果表格', () => {
    printTable('1. 写入性能');
    printTable('2. 查询性能');
    printTable('3. 实时查询');
    printTable('4. 混合工作负载');
    printTable('5. GIN 索引性能');

    console.log('\n' + '='.repeat(100));
    console.log('测试完成');
    console.log('='.repeat(100));
  });
});