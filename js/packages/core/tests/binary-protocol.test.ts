/**
 * Binary Protocol 正确性测试
 *
 * 验证 Binary Protocol 的数据编码/解码正确性：
 * 1. 所有数据类型的往返测试 (encode → decode → compare)
 * 2. NULL 值处理
 * 3. 边界情况（空结果集、单行、大结果集）
 * 4. ResultSet API 兼容性
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

// ============================================================================
// 1. 基础数据类型正确性测试
// ============================================================================
describe('1. Data Type Correctness', () => {
  /**
   * 1.1 Boolean 类型
   */
  describe('1.1 Boolean', () => {
    it('should encode/decode boolean values correctly', async () => {
      const db = new Database('binary_bool');
      const builder = db.createTable('flags')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('enabled', JsDataType.Boolean, null);
      db.registerTable(builder);

      await db.insert('flags').values([
        { id: 1, enabled: true },
        { id: 2, enabled: false },
        { id: 3, enabled: true },
      ]).exec();

      // JSON 方式
      const jsonResult = await db.select('*').from('flags').exec();

      // Binary 方式
      const layout = db.select('*').from('flags').getSchemaLayout();
      const binaryResult = await db.select('*').from('flags').execBinary();
      const rs = new ResultSet(binaryResult, layout);

      // 比较结果
      expect(rs.length).toBe(jsonResult.length);
      for (let i = 0; i < rs.length; i++) {
        const jsonRow = jsonResult[i] as any;
        const binaryRow = rs.get(i) as any;
        expect(binaryRow.id).toBe(jsonRow.id);
        expect(binaryRow.enabled).toBe(jsonRow.enabled);
      }

      rs.free();
    });
  });

  /**
   * 1.2 Int32 类型
   */
  describe('1.2 Int32', () => {
    it('should encode/decode int32 values correctly', async () => {
      const db = new Database('binary_int32');
      const builder = db.createTable('numbers')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('numbers').values([
        { id: 1, value: 0 },
        { id: 2, value: 100 },
        { id: 3, value: -100 },
        { id: 4, value: 2147483647 },  // Int32 最大值
        { id: 5, value: -2147483648 }, // Int32 最小值
      ]).exec();

      const jsonResult = await db.select('*').from('numbers').exec();
      const layout = db.select('*').from('numbers').getSchemaLayout();
      const binaryResult = await db.select('*').from('numbers').execBinary();
      const rs = new ResultSet(binaryResult, layout);

      expect(rs.length).toBe(jsonResult.length);
      for (let i = 0; i < rs.length; i++) {
        const jsonRow = jsonResult[i] as any;
        const binaryRow = rs.get(i) as any;
        expect(binaryRow.id).toBe(jsonRow.id);
        expect(binaryRow.value).toBe(jsonRow.value);
      }

      rs.free();
    });
  });

  /**
   * 1.3 Int64 类型 (存储为 f64)
   */
  describe('1.3 Int64', () => {
    it('should encode/decode int64 values correctly', async () => {
      const db = new Database('binary_int64');
      const builder = db.createTable('bigints')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int64, null);
      db.registerTable(builder);

      await db.insert('bigints').values([
        { id: 1, value: 0 },
        { id: 2, value: 1000000 },
        { id: 3, value: -1000000 },
        { id: 4, value: Number.MAX_SAFE_INTEGER },
        { id: 5, value: Number.MIN_SAFE_INTEGER },
      ]).exec();

      const jsonResult = await db.select('*').from('bigints').exec();
      const layout = db.select('*').from('bigints').getSchemaLayout();
      const binaryResult = await db.select('*').from('bigints').execBinary();
      const rs = new ResultSet(binaryResult, layout);

      expect(rs.length).toBe(jsonResult.length);
      for (let i = 0; i < rs.length; i++) {
        const jsonRow = jsonResult[i] as any;
        const binaryRow = rs.get(i) as any;
        expect(binaryRow.id).toBe(jsonRow.id);
        expect(binaryRow.value).toBe(jsonRow.value);
      }

      rs.free();
    });
  });

  /**
   * 1.4 Float64 类型
   */
  describe('1.4 Float64', () => {
    it('should encode/decode float64 values correctly', async () => {
      const db = new Database('binary_float64');
      const builder = db.createTable('floats')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      await db.insert('floats').values([
        { id: 1, value: 0.0 },
        { id: 2, value: 3.14159265358979 },
        { id: 3, value: -2.71828 },
        { id: 4, value: 1e100 },
        { id: 5, value: 1e-100 },
      ]).exec();

      const jsonResult = await db.select('*').from('floats').exec();
      const layout = db.select('*').from('floats').getSchemaLayout();
      const binaryResult = await db.select('*').from('floats').execBinary();
      const rs = new ResultSet(binaryResult, layout);

      expect(rs.length).toBe(jsonResult.length);
      for (let i = 0; i < rs.length; i++) {
        const jsonRow = jsonResult[i] as any;
        const binaryRow = rs.get(i) as any;
        expect(binaryRow.id).toBe(jsonRow.id);
        expect(binaryRow.value).toBeCloseTo(jsonRow.value, 10);
      }

      rs.free();
    });
  });

  /**
   * 1.5 String 类型
   */
  describe('1.5 String', () => {
    it('should encode/decode string values correctly', async () => {
      const db = new Database('binary_string');
      const builder = db.createTable('texts')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('content', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('texts').values([
        { id: 1, content: '' },
        { id: 2, content: 'Hello, World!' },
        { id: 3, content: '中文测试' },
        { id: 4, content: '🎉 Emoji Test 🚀' },
        { id: 5, content: 'A'.repeat(1000) }, // 长字符串
      ]).exec();

      const jsonResult = await db.select('*').from('texts').exec();
      const layout = db.select('*').from('texts').getSchemaLayout();
      const binaryResult = await db.select('*').from('texts').execBinary();
      const rs = new ResultSet(binaryResult, layout);

      expect(rs.length).toBe(jsonResult.length);
      for (let i = 0; i < rs.length; i++) {
        const jsonRow = jsonResult[i] as any;
        const binaryRow = rs.get(i) as any;
        expect(binaryRow.id).toBe(jsonRow.id);
        expect(binaryRow.content).toBe(jsonRow.content);
      }

      rs.free();
    });
  });

  /**
   * 1.6 DateTime 类型
   */
  describe('1.6 DateTime', () => {
    it('should encode/decode datetime values correctly', async () => {
      const db = new Database('binary_datetime');
      const builder = db.createTable('events')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('timestamp', JsDataType.DateTime, null);
      db.registerTable(builder);

      const now = new Date();
      const epoch = new Date(0);
      const future = new Date('2100-01-01T00:00:00Z');

      await db.insert('events').values([
        { id: 1, timestamp: now },
        { id: 2, timestamp: epoch },
        { id: 3, timestamp: future },
      ]).exec();

      const jsonResult = await db.select('*').from('events').exec();
      const layout = db.select('*').from('events').getSchemaLayout();
      const binaryResult = await db.select('*').from('events').execBinary();
      const rs = new ResultSet(binaryResult, layout);

      expect(rs.length).toBe(jsonResult.length);
      for (let i = 0; i < rs.length; i++) {
        const jsonRow = jsonResult[i] as any;
        const binaryRow = rs.get(i) as any;
        expect(binaryRow.id).toBe(jsonRow.id);
        // DateTime 比较时间戳
        expect(binaryRow.timestamp).toBeCloseTo(jsonRow.timestamp, -2);
      }

      rs.free();
    });
  });

  /**
   * 1.7 Bytes 类型
   */
  describe('1.7 Bytes', () => {
    it('should encode/decode bytes values correctly', async () => {
      const db = new Database('binary_bytes');
      const builder = db.createTable('blobs')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('data', JsDataType.Bytes, null);
      db.registerTable(builder);

      await db.insert('blobs').values([
        { id: 1, data: new Uint8Array([]) },
        { id: 2, data: new Uint8Array([0, 1, 2, 3, 4]) },
        { id: 3, data: new Uint8Array([255, 254, 253]) },
        { id: 4, data: new Uint8Array(1000).fill(42) }, // 大数据
      ]).exec();

      const jsonResult = await db.select('*').from('blobs').exec();
      const layout = db.select('*').from('blobs').getSchemaLayout();
      const binaryResult = await db.select('*').from('blobs').execBinary();
      const rs = new ResultSet(binaryResult, layout);

      expect(rs.length).toBe(jsonResult.length);
      for (let i = 0; i < rs.length; i++) {
        const jsonRow = jsonResult[i] as any;
        const binaryRow = rs.get(i) as any;
        expect(binaryRow.id).toBe(jsonRow.id);
        // Bytes 比较
        const jsonBytes = new Uint8Array(jsonRow.data);
        const binaryBytes = binaryRow.data as Uint8Array;
        expect(binaryBytes.length).toBe(jsonBytes.length);
        for (let j = 0; j < jsonBytes.length; j++) {
          expect(binaryBytes[j]).toBe(jsonBytes[j]);
        }
      }

      rs.free();
    });
  });

  /**
   * 1.8 Jsonb 类型
   */
  describe('1.8 Jsonb', () => {
    it('should encode/decode jsonb values correctly', async () => {
      const db = new Database('binary_jsonb');
      const builder = db.createTable('documents')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('metadata', JsDataType.Jsonb, null);
      db.registerTable(builder);

      await db.insert('documents').values([
        { id: 1, metadata: { name: 'test', value: 123 } },
        { id: 2, metadata: { nested: { deep: { value: true } } } },
        { id: 3, metadata: [1, 2, 3, 'four', null] },
        { id: 4, metadata: { unicode: '中文', emoji: '🎉' } },
      ]).exec();

      const jsonResult = await db.select('*').from('documents').exec();
      const layout = db.select('*').from('documents').getSchemaLayout();
      const binaryResult = await db.select('*').from('documents').execBinary();
      const rs = new ResultSet(binaryResult, layout);

      expect(rs.length).toBe(jsonResult.length);
      for (let i = 0; i < rs.length; i++) {
        const jsonRow = jsonResult[i] as any;
        const binaryRow = rs.get(i) as any;
        expect(binaryRow.id).toBe(jsonRow.id);
        expect(JSON.stringify(binaryRow.metadata)).toBe(JSON.stringify(jsonRow.metadata));
      }

      rs.free();
    });
  });
});

// ============================================================================
// 2. NULL 值处理测试
// ============================================================================
describe('2. NULL Value Handling', () => {
  it('should handle NULL values correctly for all types', async () => {
    const db = new Database('binary_nulls');
    const builder = db.createTable('nullable')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('bool_col', JsDataType.Boolean, new ColumnOptions().setNullable(true))
      .column('int32_col', JsDataType.Int32, new ColumnOptions().setNullable(true))
      .column('int64_col', JsDataType.Int64, new ColumnOptions().setNullable(true))
      .column('float_col', JsDataType.Float64, new ColumnOptions().setNullable(true))
      .column('string_col', JsDataType.String, new ColumnOptions().setNullable(true))
      .column('datetime_col', JsDataType.DateTime, new ColumnOptions().setNullable(true))
      .column('jsonb_col', JsDataType.Jsonb, new ColumnOptions().setNullable(true));
    db.registerTable(builder);

    await db.insert('nullable').values([
      { id: 1, bool_col: true, int32_col: 42, int64_col: 100, float_col: 3.14, string_col: 'hello', datetime_col: new Date(), jsonb_col: { key: 'value' } },
      { id: 2, bool_col: null, int32_col: null, int64_col: null, float_col: null, string_col: null, datetime_col: null, jsonb_col: null },
      { id: 3, bool_col: false, int32_col: 0, int64_col: 0, float_col: 0.0, string_col: '', datetime_col: new Date(0), jsonb_col: {} },
    ]).exec();

    const jsonResult = await db.select('*').from('nullable').exec();
    const layout = db.select('*').from('nullable').getSchemaLayout();
    const binaryResult = await db.select('*').from('nullable').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    expect(rs.length).toBe(3);

    // Row 1: all non-null
    const row1 = rs.get(0) as any;
    expect(row1.bool_col).toBe(true);
    expect(row1.int32_col).toBe(42);
    expect(row1.string_col).toBe('hello');

    // Row 2: all null
    const row2 = rs.get(1) as any;
    expect(row2.bool_col).toBeNull();
    expect(row2.int32_col).toBeNull();
    expect(row2.int64_col).toBeNull();
    expect(row2.float_col).toBeNull();
    expect(row2.string_col).toBeNull();
    expect(row2.datetime_col).toBeNull();
    expect(row2.jsonb_col).toBeNull();

    // Row 3: zero/empty values (not null)
    const row3 = rs.get(2) as any;
    expect(row3.bool_col).toBe(false);
    expect(row3.int32_col).toBe(0);
    expect(row3.string_col).toBe('');

    rs.free();
  });
});

// ============================================================================
// 3. 边界情况测试
// ============================================================================
describe('3. Edge Cases', () => {
  /**
   * 3.1 空结果集
   */
  it('should handle empty result set', async () => {
    const db = new Database('binary_empty');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    // 不插入任何数据
    const layout = db.select('*').from('items').getSchemaLayout();
    const binaryResult = await db.select('*').from('items').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    expect(rs.length).toBe(0);
    expect(rs.toArray()).toEqual([]);

    rs.free();
  });

  /**
   * 3.2 单行结果集
   */
  it('should handle single row result set', async () => {
    const db = new Database('binary_single');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    await db.insert('items').values([{ id: 1, name: 'only one' }]).exec();

    const layout = db.select('*').from('items').getSchemaLayout();
    const binaryResult = await db.select('*').from('items').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    expect(rs.length).toBe(1);
    expect((rs.get(0) as any).name).toBe('only one');

    rs.free();
  });

  /**
   * 3.3 大结果集
   */
  it('should handle large result set', async () => {
    const db = new Database('binary_large');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('value', JsDataType.Int32, null);
    db.registerTable(builder);

    const count = 10000;
    const data = Array.from({ length: count }, (_, i) => ({
      id: i + 1,
      name: `Item ${i + 1}`,
      value: i * 10,
    }));
    await db.insert('items').values(data).exec();

    const jsonResult = await db.select('*').from('items').exec();
    const layout = db.select('*').from('items').getSchemaLayout();
    const binaryResult = await db.select('*').from('items').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    expect(rs.length).toBe(count);
    expect(jsonResult.length).toBe(count);

    // 抽样验证
    for (const idx of [0, 100, 1000, 5000, 9999]) {
      const jsonRow = jsonResult[idx] as any;
      const binaryRow = rs.get(idx) as any;
      expect(binaryRow.id).toBe(jsonRow.id);
      expect(binaryRow.name).toBe(jsonRow.name);
      expect(binaryRow.value).toBe(jsonRow.value);
    }

    rs.free();
  });
});

// ============================================================================
// 4. ResultSet API 兼容性测试
// ============================================================================
describe('4. ResultSet API Compatibility', () => {
  let db: Database;
  let layout: any;

  beforeAll(async () => {
    db = new Database('binary_api');
    const builder = db.createTable('products')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('price', JsDataType.Int32, null)
      .column('active', JsDataType.Boolean, null);
    db.registerTable(builder);

    await db.insert('products').values([
      { id: 1, name: 'Apple', price: 100, active: true },
      { id: 2, name: 'Banana', price: 50, active: true },
      { id: 3, name: 'Cherry', price: 200, active: false },
      { id: 4, name: 'Date', price: 150, active: true },
      { id: 5, name: 'Elderberry', price: 300, active: false },
    ]).exec();

    layout = db.select('*').from('products').getSchemaLayout();
  });

  /**
   * 4.1 toArray()
   */
  it('should support toArray()', async () => {
    const binaryResult = await db.select('*').from('products').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    const arr = rs.toArray();
    expect(arr).toHaveLength(5);
    expect((arr[0] as any).name).toBe('Apple');

    rs.free();
  });

  /**
   * 4.2 map()
   */
  it('should support map()', async () => {
    const binaryResult = await db.select('*').from('products').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    const names = rs.map((row: any) => row.name);
    expect(names).toEqual(['Apple', 'Banana', 'Cherry', 'Date', 'Elderberry']);

    rs.free();
  });

  /**
   * 4.3 filter()
   */
  it('should support filter()', async () => {
    const binaryResult = await db.select('*').from('products').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    const active = rs.filter((row: any) => row.active);
    expect(active).toHaveLength(3);
    expect(active.map((r: any) => r.name)).toEqual(['Apple', 'Banana', 'Date']);

    rs.free();
  });

  /**
   * 4.4 find()
   */
  it('should support find()', async () => {
    const binaryResult = await db.select('*').from('products').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    const found = rs.find((row: any) => row.price > 150) as any;
    expect(found).toBeDefined();
    expect(found.name).toBe('Cherry');

    const notFound = rs.find((row: any) => row.price > 1000);
    expect(notFound).toBeUndefined();

    rs.free();
  });

  /**
   * 4.5 forEach()
   */
  it('should support forEach()', async () => {
    const binaryResult = await db.select('*').from('products').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    const names: string[] = [];
    rs.forEach((row: any) => names.push(row.name));
    expect(names).toEqual(['Apple', 'Banana', 'Cherry', 'Date', 'Elderberry']);

    rs.free();
  });

  /**
   * 4.6 [Symbol.iterator]
   */
  it('should support iteration with for...of', async () => {
    const binaryResult = await db.select('*').from('products').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    const names: string[] = [];
    for (const row of rs) {
      names.push((row as any).name);
    }
    expect(names).toEqual(['Apple', 'Banana', 'Cherry', 'Date', 'Elderberry']);

    rs.free();
  });

  /**
   * 4.7 columns 属性
   */
  it('should expose columns property', async () => {
    const binaryResult = await db.select('*').from('products').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    expect(rs.columns).toContain('id');
    expect(rs.columns).toContain('name');
    expect(rs.columns).toContain('price');
    expect(rs.columns).toContain('active');

    rs.free();
  });

  /**
   * 4.8 length 属性
   */
  it('should expose length property', async () => {
    const binaryResult = await db.select('*').from('products').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    expect(rs.length).toBe(5);

    rs.free();
  });
});

// ============================================================================
// 5. 类型特定 Getter 测试
// ============================================================================
describe('5. Type-Specific Getters', () => {
  it('should provide type-specific getters', async () => {
    const db = new Database('binary_getters');
    const builder = db.createTable('mixed')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('bool_val', JsDataType.Boolean, null)
      .column('int32_val', JsDataType.Int32, null)
      .column('float_val', JsDataType.Float64, null)
      .column('string_val', JsDataType.String, null);
    db.registerTable(builder);

    await db.insert('mixed').values([
      { id: 1, bool_val: true, int32_val: 42, float_val: 3.14, string_val: 'hello' },
    ]).exec();

    const layout = db.select('*').from('mixed').getSchemaLayout();
    const binaryResult = await db.select('*').from('mixed').execBinary();
    const rs = new ResultSet(binaryResult, layout);

    // 使用类型特定 getter
    expect(rs.getBoolean(0, 1)).toBe(true);
    expect(rs.getInt32(0, 2)).toBe(42);
    expect(rs.getNumber(0, 3)).toBeCloseTo(3.14, 10);
    expect(rs.getString(0, 4)).toBe('hello');

    rs.free();
  });
});
