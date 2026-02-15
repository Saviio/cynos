/**
 * 小结果集性能隔离测试
 * 
 * 模拟: SELECT * FROM items WHERE value > threshold
 * 数据量: 100K 行, value 有索引
 * 测试不同选择度: 0.1%, 1%, 3%, 10%
 */

import { describe, it, beforeAll } from 'vitest';
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

const DATA_SIZE = 100000;
const ITERATIONS = 100;

function createDb() {
  const db = new Database('perf_test');
  const builder = db.createTable('items')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('value', JsDataType.Int64, null)
    .column('name', JsDataType.String, null)
    .index('idx_value', 'value');
  db.registerTable(builder);
  return db;
}

function generateData(count: number) {
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    value: i,  // 0 to 99999
    name: `Item ${i + 1}`,
  }));
}

describe('Small ResultSet Performance', () => {
  // 测试不同选择度
  const testCases = [
    { selectivity: '0.1%', threshold: 99900, expectedRows: 100 },
    { selectivity: '1%', threshold: 99000, expectedRows: 1000 },
    { selectivity: '3%', threshold: 97000, expectedRows: 3000 },
    { selectivity: '10%', threshold: 90000, expectedRows: 10000 },
  ];

  for (const { selectivity, threshold, expectedRows } of testCases) {
    it(`${selectivity} selectivity (${expectedRows} rows)`, async () => {
      const db = createDb();
      await db.insert('items').values(generateData(DATA_SIZE)).exec();

      const layout = db.select('*').from('items')
        .where(col('value').gt(threshold))
        .getSchemaLayout();

      // 预热
      for (let i = 0; i < 5; i++) {
        const r = await db.select('*').from('items').where(col('value').gt(threshold)).execBinary();
        const rs = new ResultSet(r, layout);
        rs.toArray();
        rs.free();
      }

      // 测试 1: execBinary + toArray + free
      const t1Start = performance.now();
      for (let i = 0; i < ITERATIONS; i++) {
        const result = await db.select('*').from('items').where(col('value').gt(threshold)).execBinary();
        const rs = new ResultSet(result, layout);
        rs.toArray();
        rs.free();
      }
      const t1 = (performance.now() - t1Start) / ITERATIONS;

      // 测试 2: execBinary + toArray (不 free)
      const t2Start = performance.now();
      for (let i = 0; i < ITERATIONS; i++) {
        const result = await db.select('*').from('items').where(col('value').gt(threshold)).execBinary();
        const rs = new ResultSet(result, layout);
        rs.toArray();
        // 不调用 free
      }
      const t2 = (performance.now() - t2Start) / ITERATIONS;

      // 测试 3: 只 execBinary (不创建 ResultSet)
      const t3Start = performance.now();
      for (let i = 0; i < ITERATIONS; i++) {
        const result = await db.select('*').from('items').where(col('value').gt(threshold)).execBinary();
        result.free();
      }
      const t3 = (performance.now() - t3Start) / ITERATIONS;

      // 测试 4: JSON exec
      const t4Start = performance.now();
      for (let i = 0; i < ITERATIONS; i++) {
        await db.select('*').from('items').where(col('value').gt(threshold)).exec();
      }
      const t4 = (performance.now() - t4Start) / ITERATIONS;

      console.log(`\n[${selectivity}] ${expectedRows} rows:`);
      console.log(`  execBinary + toArray + free: ${t1.toFixed(2)}ms`);
      console.log(`  execBinary + toArray:        ${t2.toFixed(2)}ms`);
      console.log(`  execBinary only:             ${t3.toFixed(2)}ms`);
      console.log(`  JSON exec:                   ${t4.toFixed(2)}ms`);
      console.log(`  free() overhead:             ${(t1 - t2).toFixed(3)}ms`);
      console.log(`  toArray overhead:            ${(t2 - t3).toFixed(2)}ms`);
      console.log(`  Binary vs JSON speedup:      ${(t4 / t1).toFixed(2)}x`);
    });
  }
});
