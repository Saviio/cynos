/**
 * 性能隔离测试 - 确定 Binary Protocol 的瓶颈
 *
 * 测试目标：分离各个阶段的开销，找出真正的瓶颈
 * - Rust 编码时间
 * - WASM→JS 数据传输时间
 * - JS 对象创建时间
 * - TextDecoder 解码时间
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

const ROW_COUNT = 50000;
const ITERATIONS = 10;

// =============================================================================
// 辅助函数
// =============================================================================

function measure(name: string, fn: () => void, iterations: number = ITERATIONS): number {
  // Warmup
  for (let i = 0; i < 3; i++) fn();

  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    fn();
  }
  const duration = (performance.now() - start) / iterations;
  console.log(`[${name}] ${duration.toFixed(3)}ms`);
  return duration;
}

async function measureAsync(name: string, fn: () => Promise<void>, iterations: number = ITERATIONS): Promise<number> {
  // Warmup
  for (let i = 0; i < 3; i++) await fn();

  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    await fn();
  }
  const duration = (performance.now() - start) / iterations;
  console.log(`[${name}] ${duration.toFixed(3)}ms`);
  return duration;
}

// =============================================================================
// SECTION 1: JS 对象创建开销隔离
// =============================================================================
describe('1. JS Object Creation Overhead', () => {
  it('should measure pure object creation cost', () => {
    const colNames = ['id', 'name', 'category', 'price', 'stock', 'rating', 'description', 'is_active'];

    // 测试 1: 空对象创建
    const emptyObjTime = measure('Empty object creation', () => {
      const arr = new Array(ROW_COUNT);
      for (let i = 0; i < ROW_COUNT; i++) {
        arr[i] = {};
      }
    });

    // 测试 2: 带属性的对象创建 (直接赋值)
    const directAssignTime = measure('Direct property assignment', () => {
      const arr = new Array(ROW_COUNT);
      for (let i = 0; i < ROW_COUNT; i++) {
        const obj: Record<string, unknown> = {};
        obj['id'] = i;
        obj['name'] = 'test';
        obj['category'] = 'cat';
        obj['price'] = 1000;
        obj['stock'] = 100;
        obj['rating'] = 4.5;
        obj['description'] = 'desc';
        obj['is_active'] = true;
        arr[i] = obj;
      }
    });

    // 测试 3: 使用变量名赋值
    const varNameAssignTime = measure('Variable name assignment', () => {
      const arr = new Array(ROW_COUNT);
      for (let i = 0; i < ROW_COUNT; i++) {
        const obj: Record<string, unknown> = {};
        for (let j = 0; j < colNames.length; j++) {
          obj[colNames[j]] = j;
        }
        arr[i] = obj;
      }
    });

    // 测试 4: Object literal
    const literalTime = measure('Object literal', () => {
      const arr = new Array(ROW_COUNT);
      for (let i = 0; i < ROW_COUNT; i++) {
        arr[i] = {
          id: i,
          name: 'test',
          category: 'cat',
          price: 1000,
          stock: 100,
          rating: 4.5,
          description: 'desc',
          is_active: true,
        };
      }
    });

    // 测试 5: Object.fromEntries
    const entries: [string, unknown][] = colNames.map(n => [n, null]);
    const fromEntriesTime = measure('Object.fromEntries (reused entries)', () => {
      const arr = new Array(ROW_COUNT);
      for (let i = 0; i < ROW_COUNT; i++) {
        for (let j = 0; j < colNames.length; j++) {
          entries[j][1] = j;
        }
        arr[i] = Object.fromEntries(entries);
      }
    });

    // 测试 6: Object.fromEntries (new entries each time)
    const fromEntriesNewTime = measure('Object.fromEntries (new entries)', () => {
      const arr = new Array(ROW_COUNT);
      for (let i = 0; i < ROW_COUNT; i++) {
        const e: [string, unknown][] = [];
        for (let j = 0; j < colNames.length; j++) {
          e.push([colNames[j], j]);
        }
        arr[i] = Object.fromEntries(e);
      }
    });

    console.log('\n--- Object Creation Summary ---');
    console.log(`Empty object: ${emptyObjTime.toFixed(3)}ms`);
    console.log(`Direct assign: ${directAssignTime.toFixed(3)}ms`);
    console.log(`Variable name: ${varNameAssignTime.toFixed(3)}ms`);
    console.log(`Object literal: ${literalTime.toFixed(3)}ms`);
    console.log(`fromEntries (reused): ${fromEntriesTime.toFixed(3)}ms`);
    console.log(`fromEntries (new): ${fromEntriesNewTime.toFixed(3)}ms`);
  });
});

// =============================================================================
// SECTION 2: TextDecoder 开销隔离
// =============================================================================
describe('2. TextDecoder Overhead', () => {
  it('should measure TextDecoder cost', () => {
    const textDecoder = new TextDecoder();
    const testStrings = Array.from({ length: ROW_COUNT }, (_, i) =>
      new TextEncoder().encode(`Product ${i + 1} with some description text`)
    );

    // 测试 1: 共享 TextDecoder
    const sharedDecoderTime = measure('Shared TextDecoder', () => {
      for (let i = 0; i < ROW_COUNT; i++) {
        textDecoder.decode(testStrings[i]);
      }
    });

    // 测试 2: 每次创建新 TextDecoder
    const newDecoderTime = measure('New TextDecoder each time', () => {
      for (let i = 0; i < ROW_COUNT; i++) {
        new TextDecoder().decode(testStrings[i]);
      }
    });

    console.log('\n--- TextDecoder Summary ---');
    console.log(`Shared decoder: ${sharedDecoderTime.toFixed(3)}ms`);
    console.log(`New decoder: ${newDecoderTime.toFixed(3)}ms`);
    console.log(`Overhead per decode: ${((newDecoderTime - sharedDecoderTime) / ROW_COUNT * 1000).toFixed(3)}μs`);
  });
});

// =============================================================================
// SECTION 3: DataView 读取开销
// =============================================================================
describe('3. DataView Read Overhead', () => {
  it('should measure DataView read cost', () => {
    // 创建一个模拟的 binary buffer
    const buffer = new ArrayBuffer(ROW_COUNT * 32); // 32 bytes per row
    const dataView = new DataView(buffer);
    const uint8Array = new Uint8Array(buffer);

    // 填充测试数据
    for (let i = 0; i < ROW_COUNT; i++) {
      const offset = i * 32;
      dataView.setFloat64(offset, i, true);      // id
      dataView.setFloat64(offset + 8, 1000, true); // price
      dataView.setInt32(offset + 16, 100, true);  // stock
      dataView.setFloat64(offset + 20, 4.5, true); // rating
      dataView.setUint8(offset + 28, 1);          // is_active
    }

    // 测试 1: DataView 读取
    const dataViewTime = measure('DataView reads', () => {
      let sum = 0;
      for (let i = 0; i < ROW_COUNT; i++) {
        const offset = i * 32;
        sum += dataView.getFloat64(offset, true);
        sum += dataView.getFloat64(offset + 8, true);
        sum += dataView.getInt32(offset + 16, true);
        sum += dataView.getFloat64(offset + 20, true);
        sum += dataView.getUint8(offset + 28);
      }
    });

    // 测试 2: DataView 读取 + 对象创建
    const dataViewObjTime = measure('DataView + object creation', () => {
      const arr = new Array(ROW_COUNT);
      for (let i = 0; i < ROW_COUNT; i++) {
        const offset = i * 32;
        arr[i] = {
          id: dataView.getFloat64(offset, true),
          price: dataView.getFloat64(offset + 8, true),
          stock: dataView.getInt32(offset + 16, true),
          rating: dataView.getFloat64(offset + 20, true),
          is_active: dataView.getUint8(offset + 28) !== 0,
        };
      }
    });

    console.log('\n--- DataView Summary ---');
    console.log(`Pure DataView reads: ${dataViewTime.toFixed(3)}ms`);
    console.log(`DataView + objects: ${dataViewObjTime.toFixed(3)}ms`);
    console.log(`Object creation overhead: ${(dataViewObjTime - dataViewTime).toFixed(3)}ms`);
  });
});

// =============================================================================
// SECTION 4: 完整流程分解
// =============================================================================
describe('4. Full Pipeline Breakdown', () => {
  it('should break down JSON vs Binary pipeline', async () => {
    // 创建测试数据库 - 只用数值列
    const db = new Database('perf_isolation_numeric');
    const builder = db.createTable('numbers')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('a', JsDataType.Float64, null)
      .column('b', JsDataType.Float64, null)
      .column('c', JsDataType.Float64, null)
      .column('d', JsDataType.Float64, null);
    db.registerTable(builder);

    // 插入数据
    const data = Array.from({ length: ROW_COUNT }, (_, i) => ({
      id: i + 1,
      a: Math.random() * 1000,
      b: Math.random() * 1000,
      c: Math.random() * 1000,
      d: Math.random() * 1000,
    }));
    await db.insert('numbers').values(data).exec();

    const layout = db.select('*').from('numbers').getSchemaLayout();

    console.log('\n--- Pipeline Breakdown (Numeric Only) ---');

    // JSON 完整流程
    const jsonTotal = await measureAsync('JSON total', async () => {
      const rows = await db.select('*').from('numbers').exec();
    });

    // Binary: 只执行查询 (Rust 编码)
    const binaryExec = await measureAsync('Binary execBinary only', async () => {
      const result = await db.select('*').from('numbers').execBinary();
      result.free();
    });

    // Binary: 执行 + ResultSet 创建
    const binaryResultSet = await measureAsync('Binary + ResultSet creation', async () => {
      const result = await db.select('*').from('numbers').execBinary();
      const rs = new ResultSet(result, layout);
      rs.free();
    });

    // Binary: 完整 toArray
    const binaryToArray = await measureAsync('Binary + toArray', async () => {
      const result = await db.select('*').from('numbers').execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    });

    // Binary: Lazy access (数值聚合)
    const binaryLazy = await measureAsync('Binary lazy access (sum)', async () => {
      const result = await db.select('*').from('numbers').execBinary();
      const rs = new ResultSet(result, layout);
      let sum = 0;
      for (let i = 0; i < rs.length; i++) {
        sum += rs.getNumber(i, 1) ?? 0;
      }
      rs.free();
    });

    console.log('\n--- Breakdown Analysis ---');
    console.log(`JSON total: ${jsonTotal.toFixed(3)}ms`);
    console.log(`Binary execBinary: ${binaryExec.toFixed(3)}ms (Rust encoding + WASM transfer)`);
    console.log(`Binary ResultSet: ${binaryResultSet.toFixed(3)}ms (+ header parsing)`);
    console.log(`Binary toArray: ${binaryToArray.toFixed(3)}ms (+ JS object creation)`);
    console.log(`Binary lazy: ${binaryLazy.toFixed(3)}ms (no object creation)`);
    console.log(`\nJS object creation cost: ${(binaryToArray - binaryResultSet).toFixed(3)}ms`);
    console.log(`Speedup (toArray): ${(jsonTotal / binaryToArray).toFixed(2)}x`);
    console.log(`Speedup (lazy): ${(jsonTotal / binaryLazy).toFixed(2)}x`);
  });

  it('should break down with String columns', async () => {
    // 创建测试数据库 - 包含字符串列
    const db = new Database('perf_isolation_string');
    const builder = db.createTable('products')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('category', JsDataType.String, null)
      .column('price', JsDataType.Float64, null);
    db.registerTable(builder);

    // 插入数据
    const categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'];
    const data = Array.from({ length: ROW_COUNT }, (_, i) => ({
      id: i + 1,
      name: `Product ${i + 1}`,
      category: categories[i % 5],
      price: 1000 + (i % 1000) * 100,
    }));
    await db.insert('products').values(data).exec();

    const layout = db.select('*').from('products').getSchemaLayout();

    console.log('\n--- Pipeline Breakdown (With Strings) ---');

    const jsonTotal = await measureAsync('JSON total', async () => {
      await db.select('*').from('products').exec();
    });

    const binaryExec = await measureAsync('Binary execBinary only', async () => {
      const result = await db.select('*').from('products').execBinary();
      result.free();
    });

    const binaryToArray = await measureAsync('Binary + toArray', async () => {
      const result = await db.select('*').from('products').execBinary();
      const rs = new ResultSet(result, layout);
      rs.toArray();
      rs.free();
    });

    const binaryLazy = await measureAsync('Binary lazy (price sum)', async () => {
      const result = await db.select('*').from('products').execBinary();
      const rs = new ResultSet(result, layout);
      let sum = 0;
      for (let i = 0; i < rs.length; i++) {
        sum += rs.getNumber(i, 3) ?? 0;
      }
      rs.free();
    });

    console.log('\n--- String Column Analysis ---');
    console.log(`JSON total: ${jsonTotal.toFixed(3)}ms`);
    console.log(`Binary execBinary: ${binaryExec.toFixed(3)}ms`);
    console.log(`Binary toArray: ${binaryToArray.toFixed(3)}ms`);
    console.log(`Binary lazy: ${binaryLazy.toFixed(3)}ms`);
    console.log(`\nTextDecoder overhead: ${(binaryToArray - binaryExec).toFixed(3)}ms (includes object creation)`);
    console.log(`Speedup (toArray): ${(jsonTotal / binaryToArray).toFixed(2)}x`);
    console.log(`Speedup (lazy): ${(jsonTotal / binaryLazy).toFixed(2)}x`);
  });
});

// =============================================================================
// SECTION 5: Rust 编码 vs JS 解码
// =============================================================================
describe('5. Rust Encoding vs JS Decoding', () => {
  it('should isolate Rust encoding time', async () => {
    const db = new Database('perf_rust_encoding');
    const builder = db.createTable('data')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('value', JsDataType.Float64, null);
    db.registerTable(builder);

    const data = Array.from({ length: ROW_COUNT }, (_, i) => ({
      id: i + 1,
      value: Math.random() * 1000,
    }));
    await db.insert('data').values(data).exec();

    const layout = db.select('*').from('data').getSchemaLayout();

    console.log('\n--- Rust Encoding Analysis ---');

    // 多次执行 execBinary 来测量 Rust 编码时间
    const execTimes: number[] = [];
    for (let i = 0; i < 20; i++) {
      const start = performance.now();
      const result = await db.select('*').from('data').execBinary();
      execTimes.push(performance.now() - start);
      result.free();
    }

    const avgExec = execTimes.slice(5).reduce((a, b) => a + b, 0) / 15; // 去掉前5次warmup
    console.log(`Average execBinary time: ${avgExec.toFixed(3)}ms`);
    console.log(`Per-row encoding: ${(avgExec / ROW_COUNT * 1000).toFixed(3)}μs`);

    // 测量 JS 解码时间
    const result = await db.select('*').from('data').execBinary();
    const rs = new ResultSet(result, layout);

    const decodeStart = performance.now();
    for (let iter = 0; iter < ITERATIONS; iter++) {
      let sum = 0;
      for (let i = 0; i < rs.length; i++) {
        sum += rs.getNumber(i, 1) ?? 0;
      }
    }
    const decodeTime = (performance.now() - decodeStart) / ITERATIONS;
    console.log(`JS decode time (lazy): ${decodeTime.toFixed(3)}ms`);
    console.log(`Per-row decode: ${(decodeTime / ROW_COUNT * 1000).toFixed(3)}μs`);

    rs.free();
  });
});
