/**
 * Cynos Database - 数据类型正确性测试套件
 *
 * 验证所有支持的数据类型：
 * 1. Boolean - 布尔类型
 * 2. Int32 - 32位整数
 * 3. Int64 - 64位整数
 * 4. Float64 - 64位浮点数
 * 5. String - 字符串
 * 6. DateTime - 日期时间
 * 7. Bytes - 二进制数据
 * 8. Jsonb - JSON 二进制
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

// ============================================================================
// 1. Boolean 布尔类型测试
// ============================================================================
describe('1. Boolean 布尔类型', () => {
  it('1.1 插入和查询布尔值', async () => {
    const db = new Database('bool_basic');
    const builder = db.createTable('flags')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('enabled', JsDataType.Boolean, null);
    db.registerTable(builder);

    await db.insert('flags').values([
      { id: 1, enabled: true },
      { id: 2, enabled: false },
      { id: 3, enabled: true },
    ]).exec();

    const trueFlags = await db.select('*').from('flags').where(col('enabled').eq(true)).exec();
    expect(trueFlags).toHaveLength(2);

    const falseFlags = await db.select('*').from('flags').where(col('enabled').eq(false)).exec();
    expect(falseFlags).toHaveLength(1);
  });
});

// ============================================================================
// 2. Int32 32位整数测试
// ============================================================================
describe('2. Int32 32位整数', () => {
  it('2.1 插入和查询 Int32', async () => {
    const db = new Database('int32_basic');
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

    const result = await db.select('*').from('numbers').exec();
    expect(result).toHaveLength(5);

    // 验证边界值
    const maxVal = await db.select('*').from('numbers').where(col('value').eq(2147483647)).exec();
    expect(maxVal).toHaveLength(1);

    const minVal = await db.select('*').from('numbers').where(col('value').eq(-2147483648)).exec();
    expect(minVal).toHaveLength(1);
  });

  it('2.2 Int32 范围查询', async () => {
    const db = new Database('int32_range');
    const builder = db.createTable('scores')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('score', JsDataType.Int32, null)
      .index('idx_score', 'score');
    db.registerTable(builder);

    await db.insert('scores').values([
      { id: 1, score: 10 },
      { id: 2, score: 20 },
      { id: 3, score: 30 },
      { id: 4, score: 40 },
      { id: 5, score: 50 },
    ]).exec();

    const result = await db.select('*').from('scores').where(col('score').between(20, 40)).exec();
    expect(result).toHaveLength(3);
  });
});

// ============================================================================
// 3. Int64 64位整数测试
// ============================================================================
describe('3. Int64 64位整数', () => {
  it('3.1 插入和查询 Int64', async () => {
    const db = new Database('int64_basic');
    const builder = db.createTable('bigints')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('value', JsDataType.Int64, null);
    db.registerTable(builder);

    await db.insert('bigints').values([
      { id: 1, value: 0 },
      { id: 2, value: 9007199254740991 },  // JS 安全整数最大值
      { id: 3, value: -9007199254740991 }, // JS 安全整数最小值
    ]).exec();

    const result = await db.select('*').from('bigints').exec();
    expect(result).toHaveLength(3);

    // 验证大数值
    const bigVal = await db.select('*').from('bigints').where(col('value').eq(9007199254740991)).exec();
    expect(bigVal).toHaveLength(1);
  });
});

// ============================================================================
// 4. Float64 64位浮点数测试
// ============================================================================
describe('4. Float64 64位浮点数', () => {
  it('4.1 插入和查询 Float64', async () => {
    const db = new Database('float64_basic');
    const builder = db.createTable('decimals')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('value', JsDataType.Float64, null);
    db.registerTable(builder);

    await db.insert('decimals').values([
      { id: 1, value: 0.0 },
      { id: 2, value: 3.14159265358979 },
      { id: 3, value: -2.71828182845904 },
      { id: 4, value: 1.7976931348623157e+308 }, // Float64 接近最大值
      { id: 5, value: 2.2250738585072014e-308 }, // Float64 接近最小正值
    ]).exec();

    const result = await db.select('*').from('decimals').exec();
    expect(result).toHaveLength(5);
  });

  it('4.2 Float64 比较查询', async () => {
    const db = new Database('float64_compare');
    const builder = db.createTable('prices')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('price', JsDataType.Float64, null);
    db.registerTable(builder);

    await db.insert('prices').values([
      { id: 1, price: 9.99 },
      { id: 2, price: 19.99 },
      { id: 3, price: 29.99 },
      { id: 4, price: 39.99 },
    ]).exec();

    const result = await db.select('*').from('prices').where(col('price').gt(15.0)).exec();
    expect(result).toHaveLength(3);
  });
});

// ============================================================================
// 5. String 字符串测试
// ============================================================================
describe('5. String 字符串', () => {
  it('5.1 插入和查询字符串', async () => {
    const db = new Database('string_basic');
    const builder = db.createTable('texts')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('content', JsDataType.String, null);
    db.registerTable(builder);

    await db.insert('texts').values([
      { id: 1, content: 'Hello' },
      { id: 2, content: 'World' },
      { id: 3, content: '' },  // 空字符串
      { id: 4, content: '你好世界' },  // Unicode
      { id: 5, content: '🎉🎊🎁' },  // Emoji
    ]).exec();

    const result = await db.select('*').from('texts').exec();
    expect(result).toHaveLength(5);

    // 验证 Unicode
    const unicode = await db.select('*').from('texts').where(col('content').eq('你好世界')).exec();
    expect(unicode).toHaveLength(1);

    // 验证 Emoji
    const emoji = await db.select('*').from('texts').where(col('content').eq('🎉🎊🎁')).exec();
    expect(emoji).toHaveLength(1);
  });

  it('5.2 字符串 LIKE 查询', async () => {
    const db = new Database('string_like');
    const builder = db.createTable('names')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    await db.insert('names').values([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
      { id: 4, name: 'David' },
      { id: 5, name: 'Eve' },
    ]).exec();

    // 前缀匹配
    const prefixResult = await db.select('*').from('names').where(col('name').like('A%')).exec();
    expect(prefixResult).toHaveLength(1);

    // 后缀匹配
    const suffixResult = await db.select('*').from('names').where(col('name').like('%e')).exec();
    expect(suffixResult).toHaveLength(3); // Alice, Charlie, Eve

    // 包含匹配
    const containsResult = await db.select('*').from('names').where(col('name').like('%li%')).exec();
    expect(containsResult).toHaveLength(2); // Alice, Charlie
  });
});

// ============================================================================
// 6. DateTime 日期时间测试
// ============================================================================
describe('6. DateTime 日期时间', () => {
  it('6.1 插入和查询日期时间', async () => {
    const db = new Database('datetime_basic');
    const builder = db.createTable('events')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('timestamp', JsDataType.DateTime, null);
    db.registerTable(builder);

    const now = Date.now();
    const yesterday = now - 86400000;
    const tomorrow = now + 86400000;

    await db.insert('events').values([
      { id: 1, timestamp: yesterday },
      { id: 2, timestamp: now },
      { id: 3, timestamp: tomorrow },
    ]).exec();

    const result = await db.select('*').from('events').exec();
    expect(result).toHaveLength(3);

    // 范围查询
    const futureEvents = await db.select('*').from('events').where(col('timestamp').gt(now)).exec();
    expect(futureEvents).toHaveLength(1);
  });
});

// ============================================================================
// 7. Bytes 二进制数据测试
// ============================================================================
describe('7. Bytes 二进制数据', () => {
  it('7.1 插入和查询二进制数据', async () => {
    const db = new Database('bytes_basic');
    const builder = db.createTable('blobs')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('data', JsDataType.Bytes, null);
    db.registerTable(builder);

    // 创建 Uint8Array
    const data1 = new Uint8Array([1, 2, 3, 4, 5]);
    const data2 = new Uint8Array([255, 254, 253]);
    const data3 = new Uint8Array([]); // 空数组

    await db.insert('blobs').values([
      { id: 1, data: data1 },
      { id: 2, data: data2 },
      { id: 3, data: data3 },
    ]).exec();

    const result = await db.select('*').from('blobs').exec();
    expect(result).toHaveLength(3);
  });
});

// ============================================================================
// 8. Jsonb JSON 二进制测试
// ============================================================================
describe('8. Jsonb JSON 二进制', () => {
  it('8.1 插入和查询 JSONB', async () => {
    const db = new Database('jsonb_basic');
    const builder = db.createTable('documents')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('data', JsDataType.Jsonb, null);
    db.registerTable(builder);

    await db.insert('documents').values([
      { id: 1, data: { name: 'Alice', age: 25 } },
      { id: 2, data: { name: 'Bob', tags: ['developer', 'designer'] } },
      { id: 3, data: { nested: { deep: { value: 42 } } } },
      { id: 4, data: [1, 2, 3, 4, 5] },  // 数组
      { id: 5, data: null },  // null
    ]).exec();

    const result = await db.select('*').from('documents').exec();
    expect(result).toHaveLength(5);
  });

  it('8.2 JSONB 路径查询', async () => {
    const db = new Database('jsonb_path');
    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('profile', JsDataType.Jsonb, null);
    db.registerTable(builder);

    await db.insert('users').values([
      { id: 1, profile: { name: 'Alice', age: 25, city: 'Beijing' } },
      { id: 2, profile: { name: 'Bob', age: 30, city: 'Shanghai' } },
      { id: 3, profile: { name: 'Charlie', age: 35, city: 'Beijing' } },
    ]).exec();

    // 使用 JSONB 路径查询
    const beijingUsers = await db.select('*').from('users')
      .where(col('profile').get('$.city').eq('Beijing'))
      .exec();
    expect(beijingUsers).toHaveLength(2);
  });

  it('8.3 复杂 JSONB 结构', async () => {
    const db = new Database('jsonb_complex');
    const builder = db.createTable('configs')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('settings', JsDataType.Jsonb, null);
    db.registerTable(builder);

    const complexData = {
      server: {
        host: 'localhost',
        port: 8080,
        ssl: true,
      },
      database: {
        connections: [
          { name: 'primary', host: 'db1.example.com' },
          { name: 'replica', host: 'db2.example.com' },
        ],
      },
      features: ['auth', 'logging', 'metrics'],
      metadata: {
        version: '1.0.0',
        created: '2024-01-01',
      },
    };

    await db.insert('configs').values([
      { id: 1, settings: complexData },
    ]).exec();

    const result = await db.select('*').from('configs').exec();
    expect(result).toHaveLength(1);
    expect(result[0].settings.server.port).toBe(8080);
    expect(result[0].settings.features).toHaveLength(3);
  });
});

// ============================================================================
// 9. Null 值测试
// ============================================================================
describe('9. Null 值处理', () => {
  it('9.1 各类型的 null 值', async () => {
    const db = new Database('null_values');
    const builder = db.createTable('nullable')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('bool_val', JsDataType.Boolean, new ColumnOptions().setNullable(true))
      .column('int_val', JsDataType.Int32, new ColumnOptions().setNullable(true))
      .column('float_val', JsDataType.Float64, new ColumnOptions().setNullable(true))
      .column('str_val', JsDataType.String, new ColumnOptions().setNullable(true));
    db.registerTable(builder);

    await db.insert('nullable').values([
      { id: 1, bool_val: true, int_val: 100, float_val: 3.14, str_val: 'hello' },
      { id: 2, bool_val: null, int_val: null, float_val: null, str_val: null },
      { id: 3, bool_val: false, int_val: 0, float_val: 0.0, str_val: '' },
    ]).exec();

    // 查询 null 值
    const nullBool = await db.select('*').from('nullable').where(col('bool_val').isNull()).exec();
    expect(nullBool).toHaveLength(1);
    expect(nullBool[0].id).toBe(2);

    // 查询非 null 值
    const notNullStr = await db.select('*').from('nullable').where(col('str_val').isNotNull()).exec();
    expect(notNullStr).toHaveLength(2);
  });
});

// ============================================================================
// 10. 混合类型表测试
// ============================================================================
describe('10. 混合类型表', () => {
  it('10.1 包含所有类型的表', async () => {
    const db = new Database('all_types');
    const builder = db.createTable('everything')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('bool_col', JsDataType.Boolean, null)
      .column('int32_col', JsDataType.Int32, null)
      .column('int64_col', JsDataType.Int64, null)
      .column('float_col', JsDataType.Float64, null)
      .column('str_col', JsDataType.String, null)
      .column('datetime_col', JsDataType.DateTime, null)
      .column('jsonb_col', JsDataType.Jsonb, null);
    db.registerTable(builder);

    const now = Date.now();

    await db.insert('everything').values([
      {
        id: 1,
        bool_col: true,
        int32_col: 42,
        int64_col: 9007199254740991,
        float_col: 3.14159,
        str_col: 'Hello World',
        datetime_col: now,
        jsonb_col: { key: 'value' },
      },
    ]).exec();

    const result = await db.select('*').from('everything').exec();
    expect(result).toHaveLength(1);
    expect(result[0].bool_col).toBe(true);
    expect(result[0].int32_col).toBe(42);
    expect(result[0].float_col).toBeCloseTo(3.14159);
    expect(result[0].str_col).toBe('Hello World');
    expect(result[0].jsonb_col.key).toBe('value');
  });
});

// ============================================================================
// 11. GIN 索引测试 (JSONB 列自动使用 GIN 索引)
// ============================================================================
describe('11. GIN 索引', () => {
  it('11.1 JSONB 列自动使用 GIN 索引', async () => {
    const db = new Database('gin_index');
    const builder = db.createTable('products')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('metadata', JsDataType.Jsonb, null)
      .index('idx_metadata', 'metadata');  // 自动使用 GIN 索引
    db.registerTable(builder);

    await db.insert('products').values([
      { id: 1, name: 'Laptop', metadata: { category: 'Electronics', tags: ['computer', 'portable'], price: 999 } },
      { id: 2, name: 'Phone', metadata: { category: 'Electronics', tags: ['mobile', 'portable'], price: 699 } },
      { id: 3, name: 'Desk', metadata: { category: 'Furniture', tags: ['office', 'wood'], price: 299 } },
    ]).exec();

    // 基本查询验证
    const result = await db.select('*').from('products').exec();
    expect(result).toHaveLength(3);

    // JSONB 路径查询
    const electronics = await db.select('*').from('products')
      .where(col('metadata').get('$.category').eq('Electronics'))
      .exec();
    expect(electronics).toHaveLength(2);
  });

  it('11.2 JSONB 索引与普通索引组合', async () => {
    const db = new Database('gin_combined');
    const builder = db.createTable('articles')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('title', JsDataType.String, null)
      .column('status', JsDataType.String, null)
      .column('tags', JsDataType.Jsonb, null)
      .index('idx_status', 'status')
      .index('idx_tags', 'tags');  // 自动使用 GIN 索引
    db.registerTable(builder);

    await db.insert('articles').values([
      { id: 1, title: 'Article 1', status: 'published', tags: { primary: 'tech', secondary: ['ai', 'ml'] } },
      { id: 2, title: 'Article 2', status: 'draft', tags: { primary: 'tech', secondary: ['web'] } },
      { id: 3, title: 'Article 3', status: 'published', tags: { primary: 'news', secondary: ['world'] } },
    ]).exec();

    // 组合查询
    const publishedTech = await db.select('*').from('articles')
      .where(col('status').eq('published').and(col('tags').get('$.primary').eq('tech')))
      .exec();
    expect(publishedTech).toHaveLength(1);
    expect(publishedTech[0].title).toBe('Article 1');
  });
});
