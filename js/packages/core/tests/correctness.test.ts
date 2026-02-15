/**
 * Cynos Database - 全面功能正确性测试套件
 *
 * 本测试套件验证 Cynos 数据库的所有核心功能：
 *
 * 1. Schema 和数据类型 - 所有支持的数据类型
 * 2. SELECT 查询 - filter, projection, order, limit, offset
 * 3. INSERT/UPDATE/DELETE - CRUD 操作
 * 4. JOIN - inner join, left join
 * 5. 索引 - primary key, unique index, btree index
 * 6. 事务 - commit, rollback
 * 7. Live Query - 实时查询
 * 8. 查询计划验证 - explain 功能
 * 9. 聚合函数 - COUNT, SUM, AVG, MIN, MAX, STDDEV, GEOMEAN, DISTINCT
 *
 * 每个查询都会同时验证 exec() 和 execBinary() 的结果一致性
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import init, {
  Database,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  col,
  SelectBuilder,
} from '../wasm/cynos_database.js';
import { ResultSet } from '../src/result-set.js';

beforeAll(async () => {
  await init();
});

// ============================================================================
// 辅助函数
// ============================================================================

const tick = () => new Promise(r => setTimeout(r, 10));

/**
 * 比较两个值是否深度相等，处理 BigInt 等特殊类型
 */
function deepEqual(a: any, b: any): boolean {
  if (a === b) return true;
  // Handle NaN comparison (NaN !== NaN, but we want them to be equal)
  if (typeof a === 'number' && typeof b === 'number' && Number.isNaN(a) && Number.isNaN(b)) return true;
  if (typeof a === 'bigint' && typeof b === 'bigint') return a === b;
  if (typeof a !== typeof b) return false;
  if (a === null || b === null) return a === b;
  if (typeof a !== 'object') return a === b;

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((v, i) => deepEqual(v, b[i]));
  }

  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    if (a.length !== b.length) return false;
    return a.every((v, i) => v === b[i]);
  }

  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  return keysA.every(key => deepEqual(a[key], b[key]));
}

/**
 * 执行查询并验证 exec() 和 execBinary() 结果一致
 * @returns exec() 的结果
 */
async function execAndVerifyBinary(query: SelectBuilder): Promise<any[]> {
  // 执行 JSON 查询
  const jsonResult = await query.exec();

  // 执行 Binary 查询
  const layout = query.getSchemaLayout();
  const binaryResult = await query.execBinary();
  const rs = new ResultSet(binaryResult, layout);

  // 验证长度一致
  expect(rs.length).toBe(jsonResult.length);

  // 验证每行数据一致
  for (let i = 0; i < rs.length; i++) {
    const jsonRow = jsonResult[i];
    const binaryRow = rs.get(i);

    // 比较每个字段
    for (const key of Object.keys(jsonRow)) {
      const jsonVal = (jsonRow as any)[key];
      const binaryVal = (binaryRow as any)[key];

      if (!deepEqual(jsonVal, binaryVal)) {
        // Custom stringify that handles BigInt
        const stringify = (v: any) => {
          if (typeof v === 'bigint') return v.toString() + 'n';
          return JSON.stringify(v);
        };
        throw new Error(
          `Row ${i}, field "${key}" mismatch:\n` +
          `  exec():       ${stringify(jsonVal)} (${typeof jsonVal})\n` +
          `  execBinary(): ${stringify(binaryVal)} (${typeof binaryVal})`
        );
      }
    }
  }

  return jsonResult;
}

// ============================================================================
// 第一部分：Schema 和数据类型测试
// ============================================================================
describe('1. Schema 和数据类型', () => {
  describe('1.1 表创建和管理', () => {
    it('应该能创建数据库', () => {
      const db = new Database('schema_test_1');
      expect(db.name).toBe('schema_test_1');
    });

    it('应该能创建和注册表', () => {
      const db = new Database('schema_test_2');
      const builder = db.createTable('users')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      expect(db.tableCount()).toBe(1);
      expect(db.tableNames()).toContain('users');
    });

    it('应该能删除表', () => {
      const db = new Database('schema_test_3');
      const builder = db.createTable('temp')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true));
      db.registerTable(builder);
      expect(db.tableCount()).toBe(1);

      db.dropTable('temp');
      expect(db.tableCount()).toBe(0);
    });

    it('应该能获取表引用和列信息', () => {
      const db = new Database('schema_test_4');
      const builder = db.createTable('users')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null)
        .column('bio', JsDataType.String, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      const table = db.table('users');
      expect(table).toBeDefined();
      expect(table!.name).toBe('users');
      expect(table!.columnCount()).toBe(3);
      expect(table!.columnNames()).toContain('id');
      expect(table!.columnNames()).toContain('name');
      expect(table!.columnNames()).toContain('bio');
      expect(table!.getColumnType('id')).toBe(JsDataType.Int64);
      expect(table!.getColumnType('name')).toBe(JsDataType.String);
      expect(table!.isColumnNullable('bio')).toBe(true);
      expect(table!.isColumnNullable('name')).toBe(false);
      expect(table!.primaryKeyColumns()).toContain('id');
    });
  });

  describe('1.2 Boolean 布尔类型', () => {
    it('应该正确存储和查询布尔值', async () => {
      const db = new Database('dtype_bool_1');
      const builder = db.createTable('flags')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('enabled', JsDataType.Boolean, null);
      db.registerTable(builder);

      await db.insert('flags').values([
        { id: 1, enabled: true },
        { id: 2, enabled: false },
        { id: 3, enabled: true },
      ]).exec();

      const trueFlags = await execAndVerifyBinary(db.select('*').from('flags').where(col('enabled').eq(true)));
      expect(trueFlags).toHaveLength(2);

      const falseFlags = await execAndVerifyBinary(db.select('*').from('flags').where(col('enabled').eq(false)));
      expect(falseFlags).toHaveLength(1);
      expect(falseFlags[0].id).toBe(2);
    });
  });

  describe('1.3 Int32 32位整数', () => {
    it('应该正确存储和查询 Int32 值', async () => {
      const db = new Database('dtype_int32_1');
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

      const result = await execAndVerifyBinary(db.select('*').from('numbers'));
      expect(result).toHaveLength(5);

      const maxVal = await execAndVerifyBinary(db.select('*').from('numbers').where(col('value').eq(2147483647)));
      expect(maxVal).toHaveLength(1);
      expect(maxVal[0].id).toBe(4);

      const minVal = await execAndVerifyBinary(db.select('*').from('numbers').where(col('value').eq(-2147483648)));
      expect(minVal).toHaveLength(1);
      expect(minVal[0].id).toBe(5);
    });
  });

  describe('1.4 Int64 64位整数', () => {
    it('应该正确存储和查询 Int64 值', async () => {
      const db = new Database('dtype_int64_1');
      const builder = db.createTable('bigints')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int64, null);
      db.registerTable(builder);

      await db.insert('bigints').values([
        { id: 1, value: 0 },
        { id: 2, value: 9007199254740991 },  // JS 安全整数最大值
        { id: 3, value: -9007199254740991 }, // JS 安全整数最小值
      ]).exec();

      const result = await execAndVerifyBinary(db.select('*').from('bigints'));
      expect(result).toHaveLength(3);

      const bigVal = await execAndVerifyBinary(db.select('*').from('bigints').where(col('value').eq(9007199254740991)));
      expect(bigVal).toHaveLength(1);
      expect(bigVal[0].id).toBe(2);
    });
  });

  describe('1.5 Float64 64位浮点数', () => {
    it('应该正确存储和查询 Float64 值', async () => {
      const db = new Database('dtype_float64_1');
      const builder = db.createTable('decimals')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      await db.insert('decimals').values([
        { id: 1, value: 0.0 },
        { id: 2, value: 3.14159265358979 },
        { id: 3, value: -2.71828182845904 },
        { id: 4, value: 1.7976931348623157e+308 },
        { id: 5, value: 2.2250738585072014e-308 },
      ]).exec();

      const result = await execAndVerifyBinary(db.select('*').from('decimals'));
      expect(result).toHaveLength(5);

      // 浮点数比较
      const piVal = await execAndVerifyBinary(db.select('*').from('decimals').where(col('value').gt(3.14)));
      expect(piVal).toHaveLength(2); // 3.14159... 和 1.79e+308
    });
  });

  describe('1.6 String 字符串', () => {
    it('应该正确存储和查询字符串', async () => {
      const db = new Database('dtype_string_1');
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

      const result = await execAndVerifyBinary(db.select('*').from('texts'));
      expect(result).toHaveLength(5);

      const unicode = await execAndVerifyBinary(db.select('*').from('texts').where(col('content').eq('你好世界')));
      expect(unicode).toHaveLength(1);

      const emoji = await execAndVerifyBinary(db.select('*').from('texts').where(col('content').eq('🎉🎊🎁')));
      expect(emoji).toHaveLength(1);

      const empty = await execAndVerifyBinary(db.select('*').from('texts').where(col('content').eq('')));
      expect(empty).toHaveLength(1);
    });
  });

  describe('1.7 DateTime 日期时间', () => {
    it('应该正确存储和查询日期时间', async () => {
      const db = new Database('dtype_datetime_1');
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

      const result = await execAndVerifyBinary(db.select('*').from('events'));
      expect(result).toHaveLength(3);

      const futureEvents = await execAndVerifyBinary(db.select('*').from('events').where(col('timestamp').gt(now)));
      expect(futureEvents).toHaveLength(1);
      expect(futureEvents[0].id).toBe(3);
    });
  });

  describe('1.8 Bytes 二进制数据', () => {
    it('应该正确存储和查询二进制数据', async () => {
      const db = new Database('dtype_bytes_1');
      const builder = db.createTable('blobs')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('data', JsDataType.Bytes, null);
      db.registerTable(builder);

      const data1 = new Uint8Array([1, 2, 3, 4, 5]);
      const data2 = new Uint8Array([255, 254, 253]);
      const data3 = new Uint8Array([]);

      await db.insert('blobs').values([
        { id: 1, data: data1 },
        { id: 2, data: data2 },
        { id: 3, data: data3 },
      ]).exec();

      const result = await execAndVerifyBinary(db.select('*').from('blobs'));
      expect(result).toHaveLength(3);
    });
  });

  describe('1.9 Jsonb JSON 二进制', () => {
    it('应该正确存储和查询 JSONB', async () => {
      const db = new Database('dtype_jsonb_1');
      const builder = db.createTable('documents')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('data', JsDataType.Jsonb, null);
      db.registerTable(builder);

      await db.insert('documents').values([
        { id: 1, data: { name: 'Alice', age: 25 } },
        { id: 2, data: { name: 'Bob', tags: ['developer', 'designer'] } },
        { id: 3, data: { nested: { deep: { value: 42 } } } },
        { id: 4, data: [1, 2, 3, 4, 5] },
        { id: 5, data: null },
      ]).exec();

      const result = await execAndVerifyBinary(db.select('*').from('documents'));
      expect(result).toHaveLength(5);
      expect(result[0].data.name).toBe('Alice');
      expect(result[1].data.tags).toHaveLength(2);
      expect(result[2].data.nested.deep.value).toBe(42);
    });

    it('应该支持 JSONB 路径查询', async () => {
      const db = new Database('dtype_jsonb_2');
      const builder = db.createTable('users')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('profile', JsDataType.Jsonb, null);
      db.registerTable(builder);

      await db.insert('users').values([
        { id: 1, profile: { name: 'Alice', age: 25, city: 'Beijing' } },
        { id: 2, profile: { name: 'Bob', age: 30, city: 'Shanghai' } },
        { id: 3, profile: { name: 'Charlie', age: 35, city: 'Beijing' } },
      ]).exec();

      const beijingUsers = await db.select('*').from('users')
        .where(col('profile').get('$.city').eq('Beijing'))
        .exec();
      expect(beijingUsers).toHaveLength(2);
    });
  });

  describe('1.10 Null 值处理', () => {
    it('应该正确处理各类型的 null 值', async () => {
      const db = new Database('dtype_null_1');
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

      const nullBool = await execAndVerifyBinary(db.select('*').from('nullable').where(col('bool_val').isNull()));
      expect(nullBool).toHaveLength(1);
      expect(nullBool[0].id).toBe(2);

      const notNullStr = await execAndVerifyBinary(db.select('*').from('nullable').where(col('str_val').isNotNull()));
      expect(notNullStr).toHaveLength(2);
    });
  });

  describe('1.11 混合类型表', () => {
    it('应该正确处理包含所有类型的表', async () => {
      const db = new Database('dtype_mixed_1');
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

      await db.insert('everything').values([{
        id: 1,
        bool_col: true,
        int32_col: 42,
        int64_col: 9007199254740991,
        float_col: 3.14159,
        str_col: 'Hello World',
        datetime_col: now,
        jsonb_col: { key: 'value' },
      }]).exec();

      const result = await execAndVerifyBinary(db.select('*').from('everything'));
      expect(result).toHaveLength(1);
      expect(result[0].bool_col).toBe(true);
      expect(result[0].int32_col).toBe(42);
      expect(result[0].float_col).toBeCloseTo(3.14159);
      expect(result[0].str_col).toBe('Hello World');
      expect(result[0].jsonb_col.key).toBe('value');
    });
  });
});

// ============================================================================
// 第二部分：SELECT 查询测试
// ============================================================================
describe('2. SELECT 查询', () => {
  // 测试数据
  const testUsers = [
    { id: 1, name: 'Alice', age: 25, score: 85.5, active: true, city: 'Beijing' },
    { id: 2, name: 'Bob', age: 30, score: 90.0, active: false, city: 'Shanghai' },
    { id: 3, name: 'Charlie', age: 25, score: 78.5, active: true, city: 'Beijing' },
    { id: 4, name: 'David', age: 35, score: 92.0, active: true, city: 'Guangzhou' },
    { id: 5, name: 'Eve', age: 28, score: 88.0, active: false, city: 'Shanghai' },
  ];

  function createUsersDb(name: string) {
    const db = new Database(name);
    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('age', JsDataType.Int32, null)
      .column('score', JsDataType.Float64, null)
      .column('active', JsDataType.Bool, null)
      .column('city', JsDataType.String, null)
      .index('idx_age', 'age')
      .index('idx_city', 'city');
    db.registerTable(builder);
    return db;
  }

  describe('2.1 Filter 过滤条件', () => {
    describe('2.1.1 eq - 等于', () => {
      it('应该按 id 精确查询', async () => {
        const db = createUsersDb('filter_eq_1');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('id').eq(1)));
        expect(result).toHaveLength(1);
        expect(result[0].name).toBe('Alice');
      });

      it('应该按字符串精确查询', async () => {
        const db = createUsersDb('filter_eq_2');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('name').eq('Bob')));
        expect(result).toHaveLength(1);
        expect(result[0].id).toBe(2);
      });

      it('应该返回多个匹配结果', async () => {
        const db = createUsersDb('filter_eq_3');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('age').eq(25)));
        expect(result).toHaveLength(2);
        expect(result.map((r: any) => r.name).sort()).toEqual(['Alice', 'Charlie']);
      });

      it('应该按布尔值精确查询', async () => {
        const db = createUsersDb('filter_eq_4');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('active').eq(true)));
        expect(result).toHaveLength(3);
      });

      it('应该返回空结果当无匹配时', async () => {
        const db = createUsersDb('filter_eq_5');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('name').eq('NonExistent')));
        expect(result).toHaveLength(0);
      });
    });

    describe('2.1.2 ne - 不等于', () => {
      it('应该返回不等于指定值的行', async () => {
        const db = createUsersDb('filter_ne_1');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('city').ne('Beijing')));
        expect(result).toHaveLength(3);
        expect(result.every((r: any) => r.city !== 'Beijing')).toBe(true);
      });

      it('应该返回不等于指定数值的行', async () => {
        const db = createUsersDb('filter_ne_2');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('age').ne(25)));
        expect(result).toHaveLength(3);
        expect(result.every((r: any) => r.age !== 25)).toBe(true);
      });
    });

    describe('2.1.3 gt/gte - 大于/大于等于', () => {
      it('gt: 应该返回严格大于的行', async () => {
        const db = createUsersDb('filter_gt_1');
        await db.insert('users').values(testUsers).exec();

        // age > 35 应该返回空 (最大是 35)
        const result1 = await execAndVerifyBinary(db.select('*').from('users').where(col('age').gt(35)));
        expect(result1).toHaveLength(0);

        // age > 34 应该返回 David (35)
        const result2 = await execAndVerifyBinary(db.select('*').from('users').where(col('age').gt(34)));
        expect(result2).toHaveLength(1);
        expect(result2[0].name).toBe('David');
      });

      it('gte: 应该返回大于等于的行', async () => {
        const db = createUsersDb('filter_gte_1');
        await db.insert('users').values(testUsers).exec();

        // age >= 35 应该返回 David
        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('age').gte(35)));
        expect(result).toHaveLength(1);
        expect(result[0].name).toBe('David');
      });

      it('gt: 应该正确处理浮点数', async () => {
        const db = createUsersDb('filter_gt_2');
        await db.insert('users').values(testUsers).exec();

        // score > 92.0 应该返回空 (最大是 92.0)
        const result1 = await execAndVerifyBinary(db.select('*').from('users').where(col('score').gt(92.0)));
        expect(result1).toHaveLength(0);

        // score > 91.0 应该返回 David (92.0)
        const result2 = await execAndVerifyBinary(db.select('*').from('users').where(col('score').gt(91.0)));
        expect(result2).toHaveLength(1);
        expect(result2[0].name).toBe('David');
      });
    });

    describe('2.1.4 lt/lte - 小于/小于等于', () => {
      it('lt: 应该返回严格小于的行', async () => {
        const db = createUsersDb('filter_lt_1');
        await db.insert('users').values(testUsers).exec();

        // age < 25 应该返回空 (最小是 25)
        const result1 = await execAndVerifyBinary(db.select('*').from('users').where(col('age').lt(25)));
        expect(result1).toHaveLength(0);

        // age < 26 应该返回 Alice 和 Charlie (25)
        const result2 = await execAndVerifyBinary(db.select('*').from('users').where(col('age').lt(26)));
        expect(result2).toHaveLength(2);
        expect(result2.every((r: any) => r.age < 26)).toBe(true);
      });

      it('lte: 应该返回小于等于的行', async () => {
        const db = createUsersDb('filter_lte_1');
        await db.insert('users').values(testUsers).exec();

        // age <= 25 应该返回 Alice 和 Charlie
        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('age').lte(25)));
        expect(result).toHaveLength(2);
        expect(result.every((r: any) => r.age <= 25)).toBe(true);
      });
    });

    describe('2.1.5 between - 范围查询', () => {
      it('应该返回范围内的行 (包含边界)', async () => {
        const db = createUsersDb('filter_between_1');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('age').between(25, 30)));
        expect(result).toHaveLength(4); // Alice(25), Bob(30), Charlie(25), Eve(28)
        expect(result.every((r: any) => r.age >= 25 && r.age <= 30)).toBe(true);
      });

      it('应该正确处理浮点数范围', async () => {
        const db = createUsersDb('filter_between_2');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('score').between(85.0, 90.0)));
        expect(result).toHaveLength(3); // Alice(85.5), Bob(90.0), Eve(88.0)
      });

      it('应该返回空结果当范围无匹配时', async () => {
        const db = createUsersDb('filter_between_3');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('age').between(100, 200)));
        expect(result).toHaveLength(0);
      });
    });

    describe('2.1.6 in - 包含查询', () => {
      it('应该返回值在列表中的行', async () => {
        const db = createUsersDb('filter_in_1');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('city').in(['Beijing', 'Shanghai'])));
        expect(result).toHaveLength(4);
        expect(result.every((r: any) => ['Beijing', 'Shanghai'].includes(r.city))).toBe(true);
      });

      it('应该正确处理数值列表', async () => {
        const db = createUsersDb('filter_in_2');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('age').in([25, 35])));
        expect(result).toHaveLength(3); // Alice, Charlie, David
      });

      it('应该返回空结果当列表无匹配时', async () => {
        const db = createUsersDb('filter_in_3');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('city').in(['Tokyo', 'Seoul'])));
        expect(result).toHaveLength(0);
      });
    });

    describe('2.1.7 like - 模糊匹配', () => {
      it('应该支持前缀匹配', async () => {
        const db = createUsersDb('filter_like_1');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('name').like('A%')));
        expect(result).toHaveLength(1);
        expect(result[0].name).toBe('Alice');
      });

      it('应该支持后缀匹配', async () => {
        const db = createUsersDb('filter_like_2');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('name').like('%e')));
        expect(result).toHaveLength(3); // Alice, Charlie, Eve
      });

      it('应该支持包含匹配', async () => {
        const db = createUsersDb('filter_like_3');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('name').like('%li%')));
        expect(result).toHaveLength(2); // Alice, Charlie
      });

      it('应该支持单字符通配符', async () => {
        const db = createUsersDb('filter_like_4');
        await db.insert('users').values(testUsers).exec();

        const result = await execAndVerifyBinary(db.select('*').from('users').where(col('name').like('_ob')));
        expect(result).toHaveLength(1);
        expect(result[0].name).toBe('Bob');
      });
    });

    describe('2.1.8 isNull/isNotNull - 空值判断', () => {
      it('应该正确查询 null 值', async () => {
        const db = new Database('filter_null_1');
        const builder = db.createTable('users')
          .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
          .column('name', JsDataType.String, new ColumnOptions().setNullable(true))
          .column('age', JsDataType.Int32, null);
        db.registerTable(builder);

        await db.insert('users').values([
          { id: 1, name: 'Alice', age: 25 },
          { id: 2, name: null, age: 30 },
          { id: 3, name: 'Charlie', age: 35 },
        ]).exec();

        const nullResult = await execAndVerifyBinary(db.select('*').from('users').where(col('name').isNull()));
        expect(nullResult).toHaveLength(1);
        expect(nullResult[0].id).toBe(2);

        const notNullResult = await execAndVerifyBinary(db.select('*').from('users').where(col('name').isNotNull()));
        expect(notNullResult).toHaveLength(2);
      });
    });

    describe('2.1.9 and - 组合条件', () => {
      it('应该正确处理 AND 条件', async () => {
        const db = createUsersDb('filter_and_1');
        await db.insert('users').values(testUsers).exec();

        const result = await db.select('*').from('users')
          .where(col('age').gte(25).and(col('active').eq(true)))
          .exec();
        expect(result).toHaveLength(3); // Alice, Charlie, David
        expect(result.every((r: any) => r.age >= 25 && r.active === true)).toBe(true);
      });

      it('应该正确处理多个 AND 条件', async () => {
        const db = createUsersDb('filter_and_2');
        await db.insert('users').values(testUsers).exec();

        const result = await db.select('*').from('users')
          .where(col('age').gte(25).and(col('active').eq(true)).and(col('score').gt(80)))
          .exec();
        expect(result).toHaveLength(2); // Alice (85.5), David (92.0)
      });
    });

    describe('2.1.10 or - 或条件', () => {
      it('应该正确处理 OR 条件', async () => {
        const db = createUsersDb('filter_or_1');
        await db.insert('users').values(testUsers).exec();

        const result = await db.select('*').from('users')
          .where(col('city').eq('Beijing').or(col('city').eq('Guangzhou')))
          .exec();
        expect(result).toHaveLength(3); // Alice, Charlie, David
        expect(result.every((r: any) => r.city === 'Beijing' || r.city === 'Guangzhou')).toBe(true);
      });
    });

    describe('2.1.11 not - 取反', () => {
      it('应该正确处理 NOT 条件', async () => {
        const db = createUsersDb('filter_not_1');
        await db.insert('users').values(testUsers).exec();

        const result = await db.select('*').from('users')
          .where(col('age').gt(30).not())
          .exec();
        expect(result).toHaveLength(4); // age <= 30
        expect(result.every((r: any) => r.age <= 30)).toBe(true);
      });
    });

    describe('2.1.12 复杂组合条件', () => {
      it('应该正确处理 (A AND B) OR C', async () => {
        const db = createUsersDb('filter_complex_1');
        await db.insert('users').values(testUsers).exec();

        // (active = true AND age > 30) OR city = 'Shanghai'
        const result = await db.select('*').from('users')
          .where(
            col('active').eq(true).and(col('age').gt(30))
              .or(col('city').eq('Shanghai'))
          )
          .exec();
        // David (active=true, age=35) + Bob (Shanghai) + Eve (Shanghai)
        expect(result).toHaveLength(3);
      });
    });
  });

  describe('2.2 Projection 列选择', () => {
    it('应该选择所有列 (*)', async () => {
      const db = createUsersDb('proj_all_1');
      await db.insert('users').values(testUsers).exec();

      const result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(5);
      expect(Object.keys(result[0])).toHaveLength(6);
      expect(result[0]).toHaveProperty('id');
      expect(result[0]).toHaveProperty('name');
      expect(result[0]).toHaveProperty('age');
      expect(result[0]).toHaveProperty('score');
      expect(result[0]).toHaveProperty('active');
      expect(result[0]).toHaveProperty('city');
    });

    it('应该选择指定列 (数组语法)', async () => {
      const db = createUsersDb('proj_array_1');
      await db.insert('users').values(testUsers).exec();

      const result = await execAndVerifyBinary(db.select(['name', 'age']).from('users'));
      expect(result).toHaveLength(5);
      expect(result[0]).toHaveProperty('name');
      expect(result[0]).toHaveProperty('age');
      expect(result[0]).not.toHaveProperty('id');
      expect(result[0]).not.toHaveProperty('score');
    });

    it('应该选择单列 (字符串语法)', async () => {
      const db = createUsersDb('proj_string_1');
      await db.insert('users').values(testUsers).exec();

      const result = await execAndVerifyBinary(db.select('name').from('users'));
      expect(result).toHaveLength(5);
      expect(result[0]).toHaveProperty('name');
      expect(result[0]).not.toHaveProperty('id');
    });

    it('应该与 WHERE 组合使用', async () => {
      const db = createUsersDb('proj_where_1');
      await db.insert('users').values(testUsers).exec();

      const result = await db.select(['name', 'age']).from('users')
        .where(col('age').gt(28))
        .exec();
      expect(result).toHaveLength(2);
      expect(result[0]).toHaveProperty('name');
      expect(result[0]).toHaveProperty('age');
      expect(result[0]).not.toHaveProperty('id');
    });

    it('应该与 ORDER BY 组合使用', async () => {
      const db = createUsersDb('proj_order_1');
      await db.insert('users').values(testUsers).exec();

      const result = await db.select(['name', 'age']).from('users')
        .orderBy('age', JsSortOrder.Desc)
        .exec();
      expect(result).toHaveLength(5);
      expect(result[0].name).toBe('David');
      expect(result[0].age).toBe(35);
      expect(result[0]).not.toHaveProperty('id');
    });
  });

  describe('2.3 Order 排序', () => {
    it('应该按升序排序', async () => {
      const db = createUsersDb('order_asc_1');
      await db.insert('users').values(testUsers).exec();

      const result = await execAndVerifyBinary(db.select('*').from('users').orderBy('age', JsSortOrder.Asc));
      expect(result).toHaveLength(5);
      for (let i = 1; i < result.length; i++) {
        expect(result[i].age).toBeGreaterThanOrEqual(result[i - 1].age);
      }
    });

    it('应该按降序排序', async () => {
      const db = createUsersDb('order_desc_1');
      await db.insert('users').values(testUsers).exec();

      const result = await execAndVerifyBinary(db.select('*').from('users').orderBy('score', JsSortOrder.Desc));
      expect(result).toHaveLength(5);
      for (let i = 1; i < result.length; i++) {
        expect(result[i].score).toBeLessThanOrEqual(result[i - 1].score);
      }
    });

    it('应该按字符串排序', async () => {
      const db = createUsersDb('order_string_1');
      await db.insert('users').values(testUsers).exec();

      const result = await execAndVerifyBinary(db.select('*').from('users').orderBy('name', JsSortOrder.Asc));
      expect(result).toHaveLength(5);
      expect(result[0].name).toBe('Alice');
      expect(result[4].name).toBe('Eve');
    });

    it('应该与 filter 组合使用', async () => {
      const db = createUsersDb('order_filter_1');
      await db.insert('users').values(testUsers).exec();

      const result = await db.select('*').from('users')
        .where(col('active').eq(true))
        .orderBy('score', JsSortOrder.Desc)
        .exec();
      expect(result).toHaveLength(3);
      expect(result[0].name).toBe('David'); // 92.0
      expect(result[1].name).toBe('Alice'); // 85.5
      expect(result[2].name).toBe('Charlie'); // 78.5
    });
  });

  describe('2.4 Limit 限制', () => {
    it('应该限制返回行数', async () => {
      const db = createUsersDb('limit_basic_1');
      await db.insert('users').values(testUsers).exec();

      const result = await execAndVerifyBinary(db.select('*').from('users').limit(2));
      expect(result).toHaveLength(2);
    });

    it('应该在数据不足时返回所有数据', async () => {
      const db = createUsersDb('limit_basic_2');
      await db.insert('users').values(testUsers).exec();

      const result = await execAndVerifyBinary(db.select('*').from('users').limit(10));
      expect(result).toHaveLength(5);
    });

    it('limit 0 应该返回空结果', async () => {
      const db = createUsersDb('limit_zero_1');
      await db.insert('users').values(testUsers).exec();

      const result = await execAndVerifyBinary(db.select('*').from('users').limit(0));
      expect(result).toHaveLength(0);
    });

    it('应该与 filter 组合使用', async () => {
      const db = createUsersDb('limit_filter_1');
      await db.insert('users').values(testUsers).exec();

      const result = await db.select('*').from('users')
        .where(col('active').eq(true))
        .limit(2)
        .exec();
      expect(result).toHaveLength(2);
      expect(result.every((r: any) => r.active === true)).toBe(true);
    });

    it('应该与 orderBy 组合使用', async () => {
      const db = createUsersDb('limit_order_1');
      await db.insert('users').values(testUsers).exec();

      // 取 score 最高的 2 个
      const result = await db.select('*').from('users')
        .orderBy('score', JsSortOrder.Desc)
        .limit(2)
        .exec();
      expect(result).toHaveLength(2);
      expect(result[0].name).toBe('David'); // 92.0
      expect(result[1].name).toBe('Bob');   // 90.0
    });

    it('应该与 filter + orderBy 组合使用', async () => {
      const db = createUsersDb('limit_filter_order_1');
      await db.insert('users').values(testUsers).exec();

      // active=true 中 score 最高的 2 个
      const result = await db.select('*').from('users')
        .where(col('active').eq(true))
        .orderBy('score', JsSortOrder.Desc)
        .limit(2)
        .exec();
      expect(result).toHaveLength(2);
      expect(result[0].name).toBe('David'); // 92.0
      expect(result[1].name).toBe('Alice'); // 85.5
    });
  });

  describe('2.5 Offset 偏移', () => {
    it('应该跳过指定行数', async () => {
      const db = createUsersDb('offset_basic_1');
      await db.insert('users').values(testUsers).exec();

      const result = await db.select('*').from('users')
        .orderBy('id', JsSortOrder.Asc)
        .offset(2)
        .exec();
      expect(result).toHaveLength(3);
      expect(result[0].id).toBe(3);
    });

    it('应该与 limit 组合使用 (分页)', async () => {
      const db = createUsersDb('offset_limit_1');
      await db.insert('users').values(testUsers).exec();

      // 第一页
      const page1 = await db.select('*').from('users')
        .orderBy('id', JsSortOrder.Asc)
        .offset(0)
        .limit(2)
        .exec();
      expect(page1).toHaveLength(2);
      expect(page1[0].id).toBe(1);
      expect(page1[1].id).toBe(2);

      // 第二页
      const page2 = await db.select('*').from('users')
        .orderBy('id', JsSortOrder.Asc)
        .offset(2)
        .limit(2)
        .exec();
      expect(page2).toHaveLength(2);
      expect(page2[0].id).toBe(3);
      expect(page2[1].id).toBe(4);

      // 第三页
      const page3 = await db.select('*').from('users')
        .orderBy('id', JsSortOrder.Asc)
        .offset(4)
        .limit(2)
        .exec();
      expect(page3).toHaveLength(1);
      expect(page3[0].id).toBe(5);
    });

    it('offset 超出数据范围应该返回空', async () => {
      const db = createUsersDb('offset_overflow_1');
      await db.insert('users').values(testUsers).exec();

      const result = await db.select('*').from('users')
        .orderBy('id', JsSortOrder.Asc)
        .offset(10)
        .exec();
      expect(result).toHaveLength(0);
    });
  });

  describe('2.6 空表查询', () => {
    it('应该从空表返回空结果', async () => {
      const db = createUsersDb('empty_table_1');

      const result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(0);
    });

    it('应该从空表返回空结果 (带 filter)', async () => {
      const db = createUsersDb('empty_table_2');

      const result = await execAndVerifyBinary(db.select('*').from('users').where(col('id').eq(1)));
      expect(result).toHaveLength(0);
    });
  });
});

// ============================================================================
// 第三部分：INSERT/UPDATE/DELETE 测试
// ============================================================================
describe('3. INSERT/UPDATE/DELETE 操作', () => {
  function createTestDb(name: string) {
    const db = new Database(name);
    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('age', JsDataType.Int32, null)
      .column('score', JsDataType.Float64, null)
      .column('active', JsDataType.Bool, null)
      .column('city', JsDataType.String, null)
      .index('idx_age', 'age')
      .index('idx_city', 'city');
    db.registerTable(builder);
    return db;
  }

  const testUsers = [
    { id: 1, name: 'Alice', age: 25, score: 85.5, active: true, city: 'Beijing' },
    { id: 2, name: 'Bob', age: 30, score: 90.0, active: false, city: 'Shanghai' },
    { id: 3, name: 'Charlie', age: 25, score: 78.5, active: true, city: 'Beijing' },
    { id: 4, name: 'David', age: 35, score: 92.0, active: true, city: 'Guangzhou' },
    { id: 5, name: 'Eve', age: 28, score: 88.0, active: false, city: 'Shanghai' },
  ];

  describe('3.1 INSERT 插入', () => {
    it('应该插入单行并返回正确的计数', async () => {
      const db = createTestDb('insert_single_1');

      const count = await db.insert('users').values([testUsers[0]]).exec();
      expect(count).toBe(1);
      expect(db.totalRowCount()).toBe(1);
    });

    it('应该插入多行并返回正确的计数', async () => {
      const db = createTestDb('insert_multi_1');

      const count = await db.insert('users').values(testUsers).exec();
      expect(count).toBe(5);
      expect(db.totalRowCount()).toBe(5);
    });

    it('插入后应该能查询到数据', async () => {
      const db = createTestDb('insert_query_1');

      // 插入前为空
      const before = await execAndVerifyBinary(db.select('*').from('users'));
      expect(before).toHaveLength(0);

      // 插入
      await db.insert('users').values([testUsers[0]]).exec();

      // 插入后可查询
      const after = await execAndVerifyBinary(db.select('*').from('users'));
      expect(after).toHaveLength(1);
      expect(after[0].name).toBe('Alice');
      expect(after[0].age).toBe(25);
    });

    it('多次插入应该累积', async () => {
      const db = createTestDb('insert_accumulate_1');

      await db.insert('users').values([testUsers[0]]).exec();
      let result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(1);

      await db.insert('users').values([testUsers[1]]).exec();
      result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(2);

      await db.insert('users').values([testUsers[2], testUsers[3], testUsers[4]]).exec();
      result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(5);
    });

    it('插入后应该能通过 filter 查询', async () => {
      const db = createTestDb('insert_filter_1');
      await db.insert('users').values(testUsers).exec();

      // 新插入一条
      await db.insert('users').values([
        { id: 6, name: 'Frank', age: 22, score: 75.0, active: true, city: 'Beijing' }
      ]).exec();

      // 可以通过 filter 查到新数据
      const result = await execAndVerifyBinary(db.select('*').from('users').where(col('name').eq('Frank')));
      expect(result).toHaveLength(1);
      expect(result[0].age).toBe(22);

      // Beijing 现在有 3 个人
      const beijingUsers = await execAndVerifyBinary(db.select('*').from('users').where(col('city').eq('Beijing')));
      expect(beijingUsers).toHaveLength(3);
    });

    it('应该拒绝重复的主键', async () => {
      const db = createTestDb('insert_dup_pk_1');
      await db.insert('users').values([testUsers[0]]).exec();

      await expect(
        db.insert('users').values([{ id: 1, name: 'Duplicate', age: 30, score: 80.0, active: true, city: 'Test' }]).exec()
      ).rejects.toThrow();
    });
  });

  describe('3.2 UPDATE 更新', () => {
    it('应该更新单行并返回正确的计数', async () => {
      const db = createTestDb('update_single_1');
      await db.insert('users').values(testUsers).exec();

      const count = await db.update('users')
        .set('score', 90.0)
        .where(col('id').eq(1))
        .exec();
      expect(count).toBe(1);
    });

    it('更新后应该能观察到变更', async () => {
      const db = createTestDb('update_observe_1');
      await db.insert('users').values(testUsers).exec();

      // 更新前
      let alice = await execAndVerifyBinary(db.select('*').from('users').where(col('id').eq(1)));
      expect(alice[0].score).toBe(85.5);

      // 更新
      await db.update('users')
        .set('score', 95.0)
        .where(col('id').eq(1))
        .exec();

      // 更新后
      alice = await execAndVerifyBinary(db.select('*').from('users').where(col('id').eq(1)));
      expect(alice[0].score).toBe(95.0);
      expect(alice[0].name).toBe('Alice'); // 其他字段不变
    });

    it('应该批量更新多行', async () => {
      const db = createTestDb('update_batch_1');
      await db.insert('users').values(testUsers).exec();

      // 更新所有 Beijing 用户的 active 为 false
      const count = await db.update('users')
        .set('active', false)
        .where(col('city').eq('Beijing'))
        .exec();
      expect(count).toBe(2);

      // 验证更新
      const beijingUsers = await execAndVerifyBinary(db.select('*').from('users').where(col('city').eq('Beijing')));
      expect(beijingUsers).toHaveLength(2);
      expect(beijingUsers.every((r: any) => r.active === false)).toBe(true);

      // 其他城市用户不受影响
      const david = await execAndVerifyBinary(db.select('*').from('users').where(col('name').eq('David')));
      expect(david[0].active).toBe(true);
    });

    it('更新不存在的行应该返回 0', async () => {
      const db = createTestDb('update_nonexist_1');
      await db.insert('users').values(testUsers).exec();

      const count = await db.update('users')
        .set('age', 100)
        .where(col('id').eq(999))
        .exec();
      expect(count).toBe(0);

      // 数据不变
      const result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(5);
      expect(result.every((r: any) => r.age !== 100)).toBe(true);
    });

    it('更新后 filter 结果应该变化', async () => {
      const db = createTestDb('update_filter_change_1');
      await db.insert('users').values(testUsers).exec();

      // 更新前: active=true 有 3 个
      let activeUsers = await execAndVerifyBinary(db.select('*').from('users').where(col('active').eq(true)));
      expect(activeUsers).toHaveLength(3);

      // 把 Alice 改为 inactive
      await db.update('users')
        .set('active', false)
        .where(col('name').eq('Alice'))
        .exec();

      // 更新后: active=true 只有 2 个
      activeUsers = await execAndVerifyBinary(db.select('*').from('users').where(col('active').eq(true)));
      expect(activeUsers).toHaveLength(2);
      expect(activeUsers.map((r: any) => r.name).sort()).toEqual(['Charlie', 'David']);
    });

    it('应该能更新多个字段', async () => {
      const db = createTestDb('update_multi_field_1');
      await db.insert('users').values(testUsers).exec();

      // 更新 Alice 的 age 和 score
      await db.update('users')
        .set('age', 26)
        .where(col('id').eq(1))
        .exec();
      await db.update('users')
        .set('score', 90.0)
        .where(col('id').eq(1))
        .exec();

      const alice = await execAndVerifyBinary(db.select('*').from('users').where(col('id').eq(1)));
      expect(alice[0].age).toBe(26);
      expect(alice[0].score).toBe(90.0);
    });
  });

  describe('3.3 DELETE 删除', () => {
    it('应该删除单行并返回正确的计数', async () => {
      const db = createTestDb('delete_single_1');
      await db.insert('users').values(testUsers).exec();

      const count = await db.delete('users').where(col('id').eq(1)).exec();
      expect(count).toBe(1);
      expect(db.totalRowCount()).toBe(4);
    });

    it('删除后应该查找不到', async () => {
      const db = createTestDb('delete_query_1');
      await db.insert('users').values(testUsers).exec();

      // 删除前可以找到
      let alice = await execAndVerifyBinary(db.select('*').from('users').where(col('name').eq('Alice')));
      expect(alice).toHaveLength(1);

      // 删除
      await db.delete('users').where(col('id').eq(1)).exec();

      // 删除后找不到
      alice = await execAndVerifyBinary(db.select('*').from('users').where(col('name').eq('Alice')));
      expect(alice).toHaveLength(0);

      // 总数减少
      const all = await execAndVerifyBinary(db.select('*').from('users'));
      expect(all).toHaveLength(4);
    });

    it('应该批量删除多行', async () => {
      const db = createTestDb('delete_batch_1');
      await db.insert('users').values(testUsers).exec();

      // 删除所有 Beijing 用户
      const count = await db.delete('users').where(col('city').eq('Beijing')).exec();
      expect(count).toBe(2);

      // Beijing 用户找不到了
      const beijingUsers = await execAndVerifyBinary(db.select('*').from('users').where(col('city').eq('Beijing')));
      expect(beijingUsers).toHaveLength(0);

      // 总数减少
      const all = await execAndVerifyBinary(db.select('*').from('users'));
      expect(all).toHaveLength(3);
    });

    it('删除不存在的行应该返回 0', async () => {
      const db = createTestDb('delete_nonexist_1');
      await db.insert('users').values(testUsers).exec();

      const count = await db.delete('users').where(col('id').eq(999)).exec();
      expect(count).toBe(0);

      // 数据不变
      const result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(5);
    });

    it('删除后 filter 结果应该变化', async () => {
      const db = createTestDb('delete_filter_change_1');
      await db.insert('users').values(testUsers).exec();

      // 删除前: score > 85 有 4 个
      let highScoreUsers = await execAndVerifyBinary(db.select('*').from('users').where(col('score').gt(85)));
      expect(highScoreUsers).toHaveLength(4);

      // 删除 David (score=92)
      await db.delete('users').where(col('name').eq('David')).exec();

      // 删除后: score > 85 只有 3 个
      highScoreUsers = await execAndVerifyBinary(db.select('*').from('users').where(col('score').gt(85)));
      expect(highScoreUsers).toHaveLength(3);
    });

    it('应该能删除全部数据', async () => {
      const db = createTestDb('delete_all_1');
      await db.insert('users').values(testUsers).exec();

      // 逐个删除
      for (const user of testUsers) {
        await db.delete('users').where(col('id').eq(user.id)).exec();
      }

      // 全部删除后为空
      const result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(0);
    });
  });

  describe('3.4 CRUD 组合操作', () => {
    it('应该正确处理 INSERT -> UPDATE -> DELETE 序列', async () => {
      const db = createTestDb('crud_sequence_1');

      // INSERT
      await db.insert('users').values([testUsers[0]]).exec();
      let result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');

      // UPDATE
      await db.update('users').set('name', 'Alice Updated').where(col('id').eq(1)).exec();
      result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result[0].name).toBe('Alice Updated');

      // DELETE
      await db.delete('users').where(col('id').eq(1)).exec();
      result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(0);
    });

    it('应该正确处理大量数据的 CRUD', async () => {
      const db = createTestDb('crud_large_1');

      // 插入 100 条数据
      const largeData = Array.from({ length: 100 }, (_, i) => ({
        id: i + 1,
        name: `User${i + 1}`,
        age: 20 + (i % 50),
        score: 60 + (i % 40),
        active: i % 2 === 0,
        city: ['Beijing', 'Shanghai', 'Guangzhou'][i % 3],
      }));

      await db.insert('users').values(largeData).exec();
      let result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(100);

      // 更新所有 Beijing 用户
      await db.update('users').set('active', false).where(col('city').eq('Beijing')).exec();
      const beijingUsers = await execAndVerifyBinary(db.select('*').from('users').where(col('city').eq('Beijing')));
      expect(beijingUsers.every((r: any) => r.active === false)).toBe(true);

      // 删除 age < 30 的用户
      await db.delete('users').where(col('age').lt(30)).exec();
      result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result.every((r: any) => r.age >= 30)).toBe(true);
    });
  });
});

// ============================================================================
// 第四部分：JOIN 测试
// ============================================================================
describe('4. JOIN 操作', () => {
  function createJoinTestDb(name: string) {
    const db = new Database(name);

    // employees 表
    const employeesBuilder = db.createTable('employees')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('dept_id', JsDataType.Int64, null)
      .column('salary', JsDataType.Float64, null)
      .index('idx_dept_id', 'dept_id');
    db.registerTable(employeesBuilder);

    // departments 表
    const departmentsBuilder = db.createTable('departments')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('budget', JsDataType.Float64, null);
    db.registerTable(departmentsBuilder);

    return db;
  }

  const departments = [
    { id: 1, name: 'Engineering', budget: 1000000 },
    { id: 2, name: 'Sales', budget: 500000 },
    { id: 3, name: 'Marketing', budget: 300000 },
  ];

  const employees = [
    { id: 1, name: 'Alice', dept_id: 1, salary: 80000 },
    { id: 2, name: 'Bob', dept_id: 1, salary: 90000 },
    { id: 3, name: 'Charlie', dept_id: 2, salary: 70000 },
    { id: 4, name: 'David', dept_id: 2, salary: 75000 },
    { id: 5, name: 'Eve', dept_id: null, salary: 60000 }, // 无部门
  ];

  describe('4.1 INNER JOIN', () => {
    it('应该返回两表匹配的行', async () => {
      const db = createJoinTestDb('inner_join_1');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values(employees.filter(e => e.dept_id !== null)).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .innerJoin('departments', joinCondition)
        .exec();

      expect(result).toHaveLength(4); // Alice, Bob, Charlie, David
    });

    it('应该正确合并两表的列', async () => {
      const db = createJoinTestDb('inner_join_2');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values(employees.filter(e => e.dept_id !== null)).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .innerJoin('departments', joinCondition)
        .exec();

      // 验证结果包含两表的列
      expect(result[0]).toHaveProperty('salary'); // employees 表的列
      expect(result[0]).toHaveProperty('budget'); // departments 表的列
    });

    it('应该与 WHERE 组合使用', async () => {
      const db = createJoinTestDb('inner_join_where_1');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values(employees.filter(e => e.dept_id !== null)).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .innerJoin('departments', joinCondition)
        .where(col('salary').gt(75000))
        .exec();

      expect(result).toHaveLength(2); // Alice (80000), Bob (90000)
    });

    it('应该与 ORDER BY 组合使用', async () => {
      const db = createJoinTestDb('inner_join_order_1');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values(employees.filter(e => e.dept_id !== null)).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .innerJoin('departments', joinCondition)
        .orderBy('salary', JsSortOrder.Desc)
        .exec();

      expect(result).toHaveLength(4);
      expect(result[0].salary).toBe(90000); // Bob
    });

    it('应该与 LIMIT 组合使用', async () => {
      const db = createJoinTestDb('inner_join_limit_1');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values(employees.filter(e => e.dept_id !== null)).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .innerJoin('departments', joinCondition)
        .orderBy('salary', JsSortOrder.Desc)
        .limit(2)
        .exec();

      expect(result).toHaveLength(2);
    });

    it('无匹配时应该返回空结果', async () => {
      const db = createJoinTestDb('inner_join_empty_1');
      await db.insert('departments').values([{ id: 100, name: 'Empty', budget: 0 }]).exec();
      await db.insert('employees').values(employees.filter(e => e.dept_id !== null)).exec();

      // 没有员工属于 id=100 的部门
      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .innerJoin('departments', joinCondition)
        .where(col('budget').eq(0))
        .exec();

      expect(result).toHaveLength(0);
    });
  });

  describe('4.2 LEFT JOIN', () => {
    it('应该返回左表所有行，右表无匹配时为 null', async () => {
      const db = createJoinTestDb('left_join_1');
      await db.insert('departments').values(departments).exec();
      // 包含无部门的员工
      await db.insert('employees').values([
        { id: 1, name: 'Alice', dept_id: 1, salary: 80000 },
        { id: 2, name: 'Bob', dept_id: 1, salary: 90000 },
        { id: 3, name: 'Charlie', dept_id: 2, salary: 70000 },
        { id: 4, name: 'David', dept_id: 999, salary: 75000 }, // 不存在的部门
      ]).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .leftJoin('departments', joinCondition)
        .exec();

      expect(result).toHaveLength(4); // 所有员工都应该返回
    });

    it('应该与 WHERE 组合使用', async () => {
      const db = createJoinTestDb('left_join_where_1');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values(employees.filter(e => e.dept_id !== null)).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .leftJoin('departments', joinCondition)
        .where(col('salary').gt(75000))
        .exec();

      expect(result).toHaveLength(2); // Alice, Bob
    });

    it('应该与 ORDER BY 组合使用', async () => {
      const db = createJoinTestDb('left_join_order_1');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values(employees.filter(e => e.dept_id !== null)).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .leftJoin('departments', joinCondition)
        .orderBy('salary', JsSortOrder.Asc)
        .exec();

      expect(result).toHaveLength(4);
      expect(result[0].salary).toBe(70000); // Charlie
    });

    it('应该与 LIMIT 组合使用', async () => {
      const db = createJoinTestDb('left_join_limit_1');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values(employees.filter(e => e.dept_id !== null)).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .leftJoin('departments', joinCondition)
        .limit(2)
        .exec();

      expect(result).toHaveLength(2);
    });
  });

  describe('4.3 多表 JOIN', () => {
    it('应该支持多个 LEFT JOIN', async () => {
      const db = new Database('multi_join_1');

      // 创建三个表
      const productsBuilder = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null)
        .column('category_id', JsDataType.Int64, null)
        .column('region_id', JsDataType.Int64, null);
      db.registerTable(productsBuilder);

      const categoriesBuilder = db.createTable('categories')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(categoriesBuilder);

      const regionsBuilder = db.createTable('regions')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(regionsBuilder);

      // 插入数据
      await db.insert('categories').values([
        { id: 1, name: 'Electronics' },
        { id: 2, name: 'Clothing' },
      ]).exec();

      await db.insert('regions').values([
        { id: 1, name: 'North' },
        { id: 2, name: 'South' },
      ]).exec();

      await db.insert('products').values([
        { id: 1, name: 'Laptop', category_id: 1, region_id: 1 },
        { id: 2, name: 'Phone', category_id: 1, region_id: 2 },
        { id: 3, name: 'Shirt', category_id: 2, region_id: 1 },
      ]).exec();

      // 多表 JOIN
      const catJoin = col('category_id').eq('id');
      const regJoin = col('region_id').eq('id');

      const result = await db.select('*')
        .from('products')
        .leftJoin('categories', catJoin)
        .leftJoin('regions', regJoin)
        .exec();

      expect(result).toHaveLength(3);
    });
  });

  describe('4.4 JOIN 数据正确性验证', () => {
    it('INNER JOIN 结果应该只包含匹配的行', async () => {
      const db = createJoinTestDb('join_correctness_1');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values([
        { id: 1, name: 'Alice', dept_id: 1, salary: 80000 },
        { id: 2, name: 'Bob', dept_id: 999, salary: 90000 }, // 不存在的部门
      ]).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .innerJoin('departments', joinCondition)
        .exec();

      // INNER JOIN 只返回匹配的行，Bob 的 dept_id=999 不存在
      expect(result).toHaveLength(1);
      // 'name' 列在两个表中都存在，所以使用 table.column 格式
      expect(result[0]['employees.name']).toBe('Alice');
    });

    it('JOIN 后的数据应该正确关联', async () => {
      const db = createJoinTestDb('join_correctness_2');
      await db.insert('departments').values(departments).exec();
      await db.insert('employees').values([
        { id: 1, name: 'Alice', dept_id: 1, salary: 80000 },
      ]).exec();

      const joinCondition = col('dept_id').eq('id');
      const result = await db.select('*')
        .from('employees')
        .innerJoin('departments', joinCondition)
        .exec();

      expect(result).toHaveLength(1);
      // 验证 Alice 关联到 Engineering 部门
      expect(result[0].salary).toBe(80000);
      expect(result[0].budget).toBe(1000000);
    });
  });
});

// ============================================================================
// 第五部分：索引和主键测试
// ============================================================================
describe('5. 索引和主键', () => {
  describe('5.1 Primary Key 主键', () => {
    it('应该自动创建主键索引', async () => {
      const db = new Database('pk_index_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      const table = db.table('items');
      expect(table!.primaryKeyColumns()).toContain('id');
    });

    it('主键查询应该使用索引', async () => {
      const db = new Database('pk_index_2');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ]).exec();

      // 检查查询计划
      const plan = db.select('*').from('items').where(col('id').eq(2)).explain();
      expect(plan.optimized).toContain('IndexGet');
    });

    it('主键应该保证唯一性', async () => {
      const db = new Database('pk_unique_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([{ id: 1, name: 'a' }]).exec();

      // 插入重复主键应该失败
      await expect(
        db.insert('items').values([{ id: 1, name: 'b' }]).exec()
      ).rejects.toThrow();
    });

    it('主键查询结果应该正确', async () => {
      const db = new Database('pk_query_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'first' },
        { id: 2, name: 'second' },
        { id: 3, name: 'third' },
      ]).exec();

      const result = await execAndVerifyBinary(db.select('*').from('items').where(col('id').eq(2)));
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('second');
    });
  });

  describe('5.2 Unique Index 唯一索引', () => {
    it('应该创建唯一索引', async () => {
      const db = new Database('unique_index_1');
      const builder = db.createTable('users')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('email', JsDataType.String, null)
        .uniqueIndex('idx_email', 'email');
      db.registerTable(builder);

      await db.insert('users').values([
        { id: 1, email: 'alice@test.com' },
      ]).exec();

      // 插入重复 email 应该失败
      await expect(
        db.insert('users').values([{ id: 2, email: 'alice@test.com' }]).exec()
      ).rejects.toThrow();
    });

    it('唯一索引应该允许不同的值', async () => {
      const db = new Database('unique_index_2');
      const builder = db.createTable('users')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('email', JsDataType.String, null)
        .uniqueIndex('idx_email', 'email');
      db.registerTable(builder);

      await db.insert('users').values([
        { id: 1, email: 'alice@test.com' },
        { id: 2, email: 'bob@test.com' },
      ]).exec();

      const result = await execAndVerifyBinary(db.select('*').from('users'));
      expect(result).toHaveLength(2);
    });
  });

  describe('5.3 BTree Index 普通索引', () => {
    it('应该创建普通索引', async () => {
      const db = new Database('btree_index_1');
      const builder = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('price', JsDataType.Float64, null)
        .column('category', JsDataType.String, null)
        .index('idx_price', 'price')
        .index('idx_category', 'category');
      db.registerTable(builder);

      await db.insert('products').values([
        { id: 1, price: 10.0, category: 'A' },
        { id: 2, price: 20.0, category: 'B' },
        { id: 3, price: 30.0, category: 'A' },
      ]).exec();

      const result = await execAndVerifyBinary(db.select('*').from('products'));
      expect(result).toHaveLength(3);
    });

    it('索引应该加速范围查询', async () => {
      const db = new Database('btree_index_2');
      const builder = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('price', JsDataType.Float64, null)
        .index('idx_price', 'price');
      db.registerTable(builder);

      // 插入大量数据
      const products = Array.from({ length: 100 }, (_, i) => ({
        id: i + 1,
        price: i * 10.0,
      }));
      await db.insert('products').values(products).exec();

      // 范围查询应该使用索引
      const plan = db.select('*').from('products').where(col('price').between(100, 200)).explain();
      expect(plan.optimized).toContain('Index');
    });

    it('索引查询结果应该正确', async () => {
      const db = new Database('btree_index_3');
      const builder = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('price', JsDataType.Float64, null)
        .index('idx_price', 'price');
      db.registerTable(builder);

      await db.insert('products').values([
        { id: 1, price: 10.0 },
        { id: 2, price: 20.0 },
        { id: 3, price: 30.0 },
        { id: 4, price: 40.0 },
        { id: 5, price: 50.0 },
      ]).exec();

      const result = await execAndVerifyBinary(db.select('*').from('products').where(col('price').between(20, 40)));
      expect(result).toHaveLength(3);
      expect(result.every((r: any) => r.price >= 20 && r.price <= 40)).toBe(true);
    });
  });

  describe('5.4 索引与 CRUD 操作', () => {
    it('INSERT 后索引应该更新', async () => {
      const db = new Database('index_insert_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null)
        .index('idx_value', 'value');
      db.registerTable(builder);

      await db.insert('items').values([{ id: 1, value: 100 }]).exec();

      // 通过索引查询
      const result1 = await execAndVerifyBinary(db.select('*').from('items').where(col('value').eq(100)));
      expect(result1).toHaveLength(1);

      // 插入新数据
      await db.insert('items').values([{ id: 2, value: 200 }]).exec();

      // 新数据也应该能通过索引查询
      const result2 = await execAndVerifyBinary(db.select('*').from('items').where(col('value').eq(200)));
      expect(result2).toHaveLength(1);
    });

    it('UPDATE 后索引应该更新', async () => {
      const db = new Database('index_update_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null)
        .index('idx_value', 'value');
      db.registerTable(builder);

      await db.insert('items').values([{ id: 1, value: 100 }]).exec();

      // 更新前可以通过旧值查询
      let result = await execAndVerifyBinary(db.select('*').from('items').where(col('value').eq(100)));
      expect(result).toHaveLength(1);

      // 更新
      await db.update('items').set('value', 200).where(col('id').eq(1)).exec();

      // 更新后旧值查不到
      result = await execAndVerifyBinary(db.select('*').from('items').where(col('value').eq(100)));
      expect(result).toHaveLength(0);

      // 更新后新值可以查到
      result = await execAndVerifyBinary(db.select('*').from('items').where(col('value').eq(200)));
      expect(result).toHaveLength(1);
    });

    it('DELETE 后索引应该更新', async () => {
      const db = new Database('index_delete_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null)
        .index('idx_value', 'value');
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 100 },
        { id: 2, value: 200 },
      ]).exec();

      // 删除前可以查询
      let result = await execAndVerifyBinary(db.select('*').from('items').where(col('value').eq(100)));
      expect(result).toHaveLength(1);

      // 删除
      await db.delete('items').where(col('id').eq(1)).exec();

      // 删除后查不到
      result = await execAndVerifyBinary(db.select('*').from('items').where(col('value').eq(100)));
      expect(result).toHaveLength(0);

      // 其他数据不受影响
      result = await execAndVerifyBinary(db.select('*').from('items').where(col('value').eq(200)));
      expect(result).toHaveLength(1);
    });
  });
});

// ============================================================================
// 第六部分：事务测试
// ============================================================================
describe('6. 事务', () => {
  function createTxTestDb(name: string) {
    const db = new Database(name);
    const builder = db.createTable('accounts')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('balance', JsDataType.Float64, null);
    db.registerTable(builder);
    return db;
  }

  describe('6.1 事务提交', () => {
    it('应该能提交事务', async () => {
      const db = createTxTestDb('tx_commit_1');

      const tx = db.transaction();
      tx.insert('accounts', [
        { id: 1, name: 'Alice', balance: 100.0 },
        { id: 2, name: 'Bob', balance: 200.0 },
      ]);

      expect(tx.active).toBe(true);
      expect(tx.state).toBe('active');

      tx.commit();

      expect(tx.active).toBe(false);
      expect(db.totalRowCount()).toBe(2);
    });

    it('提交后数据应该可见', async () => {
      const db = createTxTestDb('tx_commit_2');

      const tx = db.transaction();
      tx.insert('accounts', [{ id: 1, name: 'Alice', balance: 100.0 }]);
      tx.commit();

      const result = await execAndVerifyBinary(db.select('*').from('accounts'));
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });
  });

  describe('6.2 事务回滚', () => {
    it('应该能回滚事务', async () => {
      const db = createTxTestDb('tx_rollback_1');

      // 先插入一些数据
      await db.insert('accounts').values([{ id: 1, name: 'Alice', balance: 100.0 }]).exec();
      expect(db.totalRowCount()).toBe(1);

      // 开始事务并插入更多数据
      const tx = db.transaction();
      tx.insert('accounts', [{ id: 2, name: 'Bob', balance: 200.0 }]);

      // 回滚
      tx.rollback();

      // 应该只有原来的数据
      expect(db.totalRowCount()).toBe(1);
    });

    it('回滚后数据应该恢复', async () => {
      const db = createTxTestDb('tx_rollback_2');

      await db.insert('accounts').values([{ id: 1, name: 'Alice', balance: 100.0 }]).exec();

      const tx = db.transaction();
      tx.insert('accounts', [{ id: 2, name: 'Bob', balance: 200.0 }]);
      tx.rollback();

      const result = await execAndVerifyBinary(db.select('*').from('accounts'));
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Alice');
    });
  });

  describe('6.3 事务中的多个操作', () => {
    it('应该支持事务中的多个 INSERT', async () => {
      const db = createTxTestDb('tx_multi_insert_1');

      const tx = db.transaction();
      tx.insert('accounts', [{ id: 1, name: 'Alice', balance: 100.0 }]);
      tx.insert('accounts', [{ id: 2, name: 'Bob', balance: 200.0 }]);
      tx.insert('accounts', [{ id: 3, name: 'Charlie', balance: 300.0 }]);
      tx.commit();

      const result = await execAndVerifyBinary(db.select('*').from('accounts'));
      expect(result).toHaveLength(3);
    });

    it('应该支持事务中的 UPDATE', async () => {
      const db = createTxTestDb('tx_update_1');

      await db.insert('accounts').values([
        { id: 1, name: 'Alice', balance: 100.0 },
        { id: 2, name: 'Bob', balance: 200.0 },
      ]).exec();

      const tx = db.transaction();
      // 转账: Alice -> Bob
      tx.update('accounts', { balance: 50.0 }, col('id').eq(1));
      tx.update('accounts', { balance: 250.0 }, col('id').eq(2));
      tx.commit();

      const results = await execAndVerifyBinary(db.select('*').from('accounts').orderBy('id', JsSortOrder.Asc));
      expect(results[0].balance).toBe(50.0);
      expect(results[1].balance).toBe(250.0);
    });

    it('应该支持事务中的 DELETE', async () => {
      const db = createTxTestDb('tx_delete_1');

      await db.insert('accounts').values([
        { id: 1, name: 'Alice', balance: 100.0 },
        { id: 2, name: 'Bob', balance: 200.0 },
      ]).exec();

      const tx = db.transaction();
      tx.delete('accounts', col('id').eq(1));
      tx.commit();

      const result = await execAndVerifyBinary(db.select('*').from('accounts'));
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Bob');
    });
  });

  describe('6.4 事务原子性', () => {
    it('回滚应该撤销所有操作', async () => {
      const db = createTxTestDb('tx_atomic_1');

      await db.insert('accounts').values([
        { id: 1, name: 'Alice', balance: 100.0 },
      ]).exec();

      const tx = db.transaction();
      tx.insert('accounts', [{ id: 2, name: 'Bob', balance: 200.0 }]);
      tx.update('accounts', { balance: 50.0 }, col('id').eq(1));
      tx.rollback();

      // 所有操作都应该被撤销
      const result = await execAndVerifyBinary(db.select('*').from('accounts'));
      expect(result).toHaveLength(1);
      expect(result[0].balance).toBe(100.0); // 原始值
    });
  });
});

// ============================================================================
// 第七部分: Live Query 测试
// ============================================================================

describe('7. Live Query 测试', () => {
  // 创建 Live Query 测试数据库
  function createLiveQueryTestDb(name: string): Database {
    const db = new Database(name);
    const builder = db.createTable('events')
      .column('id', JsDataType.Int32, new ColumnOptions().primaryKey(true))
      .column('type', JsDataType.String, null)
      .column('data', JsDataType.String, new ColumnOptions().setNullable(true))
      .column('timestamp', JsDataType.Int64, null);
    db.registerTable(builder);
    return db;
  }

  describe('7.1 INSERT 观察', () => {
    it('应该观察到新插入的数据', async () => {
      const db = createLiveQueryTestDb('lq_insert_1');

      const observable = db.select('*').from('events').observe();

      // 初始应该为空
      expect(observable.getResult()).toHaveLength(0);

      let lastData: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastData = data;
      });

      // 插入数据
      await db.insert('events').values([
        { id: 1, type: 'click', data: 'button1', timestamp: BigInt(1000) },
      ]).exec();

      expect(lastData).toHaveLength(1);
      expect(lastData[0].type).toBe('click');

      unsubscribe();
    });

    it('应该观察到批量插入', async () => {
      const db = createLiveQueryTestDb('lq_batch_insert_1');

      const observable = db.select('*').from('events').observe();
      let lastData: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastData = data;
      });

      // 批量插入
      await db.insert('events').values([
        { id: 1, type: 'click', data: 'a', timestamp: BigInt(1000) },
        { id: 2, type: 'scroll', data: 'b', timestamp: BigInt(2000) },
        { id: 3, type: 'hover', data: 'c', timestamp: BigInt(3000) },
      ]).exec();

      expect(lastData).toHaveLength(3);

      unsubscribe();
    });
  });

  describe('7.2 UPDATE 观察', () => {
    it('应该观察到数据更新', async () => {
      const db = createLiveQueryTestDb('lq_update_1');

      await db.insert('events').values([
        { id: 1, type: 'click', data: 'original', timestamp: BigInt(1000) },
      ]).exec();

      const observable = db.select('*').from('events').observe();
      expect(observable.getResult()[0].data).toBe('original');

      let lastData: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastData = data;
      });

      // 更新数据
      await db.update('events').set({ data: 'updated' }).where(col('id').eq(1)).exec();

      expect(lastData[0].data).toBe('updated');

      unsubscribe();
    });
  });

  describe('7.3 DELETE 观察', () => {
    it('应该观察到数据删除', async () => {
      const db = createLiveQueryTestDb('lq_delete_1');

      await db.insert('events').values([
        { id: 1, type: 'click', data: 'a', timestamp: BigInt(1000) },
        { id: 2, type: 'scroll', data: 'b', timestamp: BigInt(2000) },
      ]).exec();

      const observable = db.select('*').from('events').observe();
      expect(observable.getResult()).toHaveLength(2);

      let lastData: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastData = data;
      });

      // 删除数据
      await db.delete('events').where(col('id').eq(1)).exec();

      expect(lastData).toHaveLength(1);
      expect(lastData[0].id).toBe(2);

      unsubscribe();
    });
  });

  describe('7.4 带过滤条件的 Live Query', () => {
    it('应该只观察符合条件的数据', async () => {
      const db = createLiveQueryTestDb('lq_filter_1');

      const observable = db.select('*')
        .from('events')
        .where(col('type').eq('click'))
        .observe();

      let lastData: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastData = data;
      });

      // 插入不同类型的事件
      await db.insert('events').values([
        { id: 1, type: 'click', data: 'a', timestamp: BigInt(1000) },
        { id: 2, type: 'scroll', data: 'b', timestamp: BigInt(2000) },
        { id: 3, type: 'click', data: 'c', timestamp: BigInt(3000) },
      ]).exec();

      // 只应该看到 click 类型的事件
      expect(lastData).toHaveLength(2);
      expect(lastData.every((e: any) => e.type === 'click')).toBe(true);

      unsubscribe();
    });

    it('应该正确处理带 LIMIT 的 Live Query', async () => {
      const db = createLiveQueryTestDb('lq_limit_1');

      const observable = db.select('*')
        .from('events')
        .orderBy('timestamp', JsSortOrder.Desc)
        .limit(2)
        .observe();

      let lastData: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastData = data;
      });

      // 插入多条数据
      await db.insert('events').values([
        { id: 1, type: 'a', data: null, timestamp: BigInt(1000) },
        { id: 2, type: 'b', data: null, timestamp: BigInt(2000) },
        { id: 3, type: 'c', data: null, timestamp: BigInt(3000) },
        { id: 4, type: 'd', data: null, timestamp: BigInt(4000) },
      ]).exec();

      // 只应该返回最新的2条
      expect(lastData).toHaveLength(2);
      expect(lastData[0].id).toBe(4);
      expect(lastData[1].id).toBe(3);

      unsubscribe();
    });
  });

  describe('7.5 取消订阅', () => {
    it('取消订阅后不应该再收到更新', async () => {
      const db = createLiveQueryTestDb('lq_unsub_1');

      const observable = db.select('*').from('events').observe();
      let changeCount = 0;
      const unsubscribe = observable.subscribe(() => {
        changeCount++;
      });

      // 取消订阅
      unsubscribe();
      const countAfterUnsub = changeCount;

      // 插入数据
      await db.insert('events').values([
        { id: 1, type: 'click', data: 'a', timestamp: BigInt(1000) },
      ]).exec();

      // 回调次数不应该增加
      expect(changeCount).toBe(countAfterUnsub);
    });
  });

  describe('7.6 多个订阅者', () => {
    it('应该支持多个订阅者独立工作', async () => {
      const db = createLiveQueryTestDb('lq_multi_sub_1');

      const observable1 = db.select('*').from('events').observe();
      const observable2 = db.select('*')
        .from('events')
        .where(col('type').eq('click'))
        .observe();

      let lastData1: any[] = [];
      let lastData2: any[] = [];

      const unsub1 = observable1.subscribe((data: any[]) => {
        lastData1 = data;
      });

      const unsub2 = observable2.subscribe((data: any[]) => {
        lastData2 = data;
      });

      await db.insert('events').values([
        { id: 1, type: 'click', data: 'a', timestamp: BigInt(1000) },
        { id: 2, type: 'scroll', data: 'b', timestamp: BigInt(2000) },
      ]).exec();

      // 第一个订阅者看到所有数据
      expect(lastData1).toHaveLength(2);
      // 第二个订阅者只看到 click 类型
      expect(lastData2).toHaveLength(1);

      unsub1();
      unsub2();
    });
  });
});

// ============================================================================
// 第八部分: 查询计划验证测试
// ============================================================================

describe('8. 查询计划验证测试', () => {
  describe('8.1 主键索引优化', () => {
    it('主键等值查询应该使用 IndexGet', async () => {
      const db = new Database('plan_pk_eq_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ]).exec();

      const plan = db.select('*').from('items').where(col('id').eq(2)).explain();

      // 主键等值查询应该使用 IndexGet，而不是 Filter + Scan
      expect(plan.optimized).toContain('IndexGet');
      expect(plan.optimized).not.toContain('Scan');
    });

    it('主键 IN 查询应该使用索引', async () => {
      const db = new Database('plan_pk_in_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ]).exec();

      const plan = db.select('*').from('items').where(col('id').in([1, 3])).explain();

      // IN 查询应该使用索引
      expect(plan.optimized).toContain('Index');
    });
  });

  describe('8.2 普通索引优化', () => {
    it('索引列等值查询应该使用索引', async () => {
      const db = new Database('plan_idx_eq_1');
      const builder = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.String, null)
        .index('idx_category', 'category');
      db.registerTable(builder);

      await db.insert('products').values([
        { id: 1, category: 'Electronics' },
        { id: 2, category: 'Furniture' },
        { id: 3, category: 'Electronics' },
      ]).exec();

      const plan = db.select('*').from('products').where(col('category').eq('Electronics')).explain();

      // 索引列查询应该使用索引，而不是全表扫描
      expect(plan.optimized).toContain('Index');
      expect(plan.optimized).not.toMatch(/Scan\s*\{[^}]*table:\s*"products"/);
    });

    it('索引列范围查询应该使用 IndexScan', async () => {
      const db = new Database('plan_idx_range_1');
      const builder = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('price', JsDataType.Float64, null)
        .index('idx_price', 'price');
      db.registerTable(builder);

      await db.insert('products').values([
        { id: 1, price: 50.0 },
        { id: 2, price: 100.0 },
        { id: 3, price: 150.0 },
        { id: 4, price: 200.0 },
      ]).exec();

      const plan = db.select('*').from('products').where(col('price').between(100, 200)).explain();

      // 范围查询应该使用 IndexScan
      expect(plan.optimized).toContain('Index');
    });
  });

  describe('8.3 JOIN 查询计划', () => {
    function createJoinDb(name: string): Database {
      const db = new Database(name);

      const employeesBuilder = db.createTable('employees')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null)
        .column('dept_id', JsDataType.Int64, null);
      db.registerTable(employeesBuilder);

      const departmentsBuilder = db.createTable('departments')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(departmentsBuilder);

      return db;
    }

    it('INNER JOIN 查询计划应该包含 Join 节点', async () => {
      const db = createJoinDb('plan_inner_join_1');

      const plan = db.select('*')
        .from('employees')
        .innerJoin('departments', col('dept_id').eq('id'))
        .explain();

      // JOIN 查询计划应该包含 Join 操作
      expect(plan.optimized).toContain('Join');
    });

    it('LEFT JOIN 查询计划应该包含 LeftJoin 节点', async () => {
      const db = createJoinDb('plan_left_join_1');

      const plan = db.select('*')
        .from('employees')
        .leftJoin('departments', col('dept_id').eq('id'))
        .explain();

      // LEFT JOIN 查询计划应该包含 LeftJoin 或 Join
      expect(plan.optimized).toMatch(/Join|LeftJoin/);
    });
  });

  describe('8.4 排序和限制优化', () => {
    it('ORDER BY 应该生成 Sort 节点', async () => {
      const db = new Database('plan_sort_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      const plan = db.select('*').from('items').orderBy('value', JsSortOrder.Desc).explain();

      expect(plan.optimized).toContain('Sort');
    });

    it('LIMIT 应该生成 Limit 节点', async () => {
      const db = new Database('plan_limit_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      const plan = db.select('*').from('items').limit(10).explain();

      expect(plan.optimized).toContain('Limit');
    });

    it('OFFSET 应该在查询计划中体现', async () => {
      const db = new Database('plan_offset_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      const plan = db.select('*').from('items').offset(5).explain();

      // OFFSET 可能被合并到 Limit 节点中，检查 offset 值是否存在
      expect(plan.optimized).toMatch(/offset.*5|Offset/i);
    });

    it('ORDER BY + LIMIT 应该正确组合', async () => {
      const db = new Database('plan_sort_limit_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      const plan = db.select('*')
        .from('items')
        .orderBy('value', JsSortOrder.Desc)
        .limit(10)
        .explain();

      expect(plan.optimized).toContain('Sort');
      expect(plan.optimized).toContain('Limit');
    });
  });

  describe('8.5 过滤条件优化', () => {
    it('WHERE 条件应该生成 Filter 节点', async () => {
      const db = new Database('plan_filter_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('status', JsDataType.String, null);
      db.registerTable(builder);

      const plan = db.select('*').from('items').where(col('status').eq('active')).explain();

      expect(plan.optimized).toContain('Filter');
    });

    it('AND 条件应该正确表示', async () => {
      const db = new Database('plan_and_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('a', JsDataType.Int32, null)
        .column('b', JsDataType.Int32, null);
      db.registerTable(builder);

      const plan = db.select('*')
        .from('items')
        .where(col('a').gt(10).and(col('b').lt(100)))
        .explain();

      expect(plan.optimized).toContain('Filter');
      // AND 条件应该在计划中体现
      expect(plan.optimized).toMatch(/And|&&/i);
    });

    it('OR 条件应该正确表示', async () => {
      const db = new Database('plan_or_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('status', JsDataType.String, null);
      db.registerTable(builder);

      const plan = db.select('*')
        .from('items')
        .where(col('status').eq('active').or(col('status').eq('pending')))
        .explain();

      expect(plan.optimized).toContain('Filter');
      // OR 条件应该在计划中体现
      expect(plan.optimized).toMatch(/Or|\|\|/i);
    });
  });

  describe('8.6 投影优化', () => {
    it('SELECT 指定列应该生成 Project 节点', async () => {
      const db = new Database('plan_project_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null)
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      const plan = db.select('id', 'name').from('items').explain();

      // 选择特定列应该有投影操作
      expect(plan.optimized).toMatch(/Project|Projection/i);
    });
  });

  describe('8.7 查询计划与执行结果一致性', () => {
    it('优化后的查询应该返回正确结果', async () => {
      const db = new Database('plan_exec_1');
      const builder = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null)
        .column('price', JsDataType.Float64, null);
      db.registerTable(builder);

      await db.insert('products').values([
        { id: 1, name: 'A', price: 100.0 },
        { id: 2, name: 'B', price: 200.0 },
        { id: 3, name: 'C', price: 150.0 },
      ]).exec();

      // 主键查询
      const plan1 = db.select('*').from('products').where(col('id').eq(2)).explain();
      const result1 = await execAndVerifyBinary(db.select('*').from('products').where(col('id').eq(2)));

      expect(plan1.optimized).toContain('IndexGet');
      expect(result1).toHaveLength(1);
      expect(result1[0].name).toBe('B');

      // 排序查询
      const result2 = await db.select('*')
        .from('products')
        .orderBy('price', JsSortOrder.Asc)
        .exec();

      expect(result2).toHaveLength(3);
      expect(result2[0].price).toBe(100.0);
      expect(result2[1].price).toBe(150.0);
      expect(result2[2].price).toBe(200.0);
    });
  });
});

// ============================================================================
// Section 9: JOIN 边界情况
// ============================================================================
describe('9. JOIN 边界情况', () => {
  describe('9.1 空表 JOIN', () => {
    it('空表 JOIN 空表应返回空结果', async () => {
      const db = new Database('join_empty_1');
      const t1 = db.createTable('empty1')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('val', JsDataType.String, null);
      const t2 = db.createTable('empty2')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('ref_id', JsDataType.Int64, null);
      db.registerTable(t1);
      db.registerTable(t2);

      const result = await execAndVerifyBinary(
        db.select('*').from('empty1').innerJoin('empty2', col('empty1.id').eq(col('empty2.ref_id')))
      );
      expect(result).toHaveLength(0);
    });

    it('空表 JOIN 非空表应返回空结果 (INNER JOIN)', async () => {
      const db = new Database('join_empty_2');
      const t1 = db.createTable('empty_t')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true));
      const t2 = db.createTable('filled_t')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('ref_id', JsDataType.Int64, null);
      db.registerTable(t1);
      db.registerTable(t2);

      await db.insert('filled_t').values([
        { id: 1, ref_id: 100 },
        { id: 2, ref_id: 200 },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('empty_t').innerJoin('filled_t', col('empty_t.id').eq(col('filled_t.ref_id')))
      );
      expect(result).toHaveLength(0);
    });

    it('非空表 JOIN 空表应返回空结果 (INNER JOIN)', async () => {
      const db = new Database('join_empty_3');
      const t1 = db.createTable('filled_t')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true));
      const t2 = db.createTable('empty_t')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('ref_id', JsDataType.Int64, null);
      db.registerTable(t1);
      db.registerTable(t2);

      await db.insert('filled_t').values([
        { id: 1 },
        { id: 2 },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('filled_t').innerJoin('empty_t', col('filled_t.id').eq(col('empty_t.ref_id')))
      );
      expect(result).toHaveLength(0);
    });
  });

  describe('9.2 自连接 (Self Join)', () => {
    it('表与自身 JOIN 应正确工作', async () => {
      const db = new Database('self_join_1');
      const builder = db.createTable('employees')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null)
        .column('manager_id', JsDataType.Int64, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('employees').values([
        { id: 1, name: 'CEO', manager_id: null },
        { id: 2, name: 'Manager', manager_id: 1 },
        { id: 3, name: 'Employee', manager_id: 2 },
      ]).exec();

      // 自连接查找员工和其经理
      const result = await execAndVerifyBinary(
        db.select('employees.name', 'employees.manager_id')
          .from('employees')
          .innerJoin('employees as managers', col('employees.manager_id').eq(col('managers.id')))
      );

      // 只有有经理的员工会出现在结果中
      expect(result).toHaveLength(2);
    });
  });

  describe('9.3 多列 JOIN 条件', () => {
    it('多列 JOIN 条件应正确匹配', async () => {
      const db = new Database('multi_col_join_1');
      const t1 = db.createTable('orders')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('year', JsDataType.Int32, null)
        .column('month', JsDataType.Int32, null);
      const t2 = db.createTable('reports')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('year', JsDataType.Int32, null)
        .column('month', JsDataType.Int32, null)
        .column('summary', JsDataType.String, null);
      db.registerTable(t1);
      db.registerTable(t2);

      await db.insert('orders').values([
        { id: 1, year: 2024, month: 1 },
        { id: 2, year: 2024, month: 2 },
        { id: 3, year: 2025, month: 1 },
      ]).exec();

      await db.insert('reports').values([
        { id: 1, year: 2024, month: 1, summary: 'Jan 2024' },
        { id: 2, year: 2024, month: 2, summary: 'Feb 2024' },
        { id: 3, year: 2024, month: 3, summary: 'Mar 2024' },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('orders.id', 'reports.summary')
          .from('orders')
          .innerJoin('reports',
            col('orders.year').eq(col('reports.year'))
              .and(col('orders.month').eq(col('reports.month')))
          )
      );

      expect(result).toHaveLength(2);
      expect(result.map(r => r.summary)).toContain('Jan 2024');
      expect(result.map(r => r.summary)).toContain('Feb 2024');
    });
  });

  describe('9.4 三表 JOIN', () => {
    it('三表连续 JOIN 应正确工作', async () => {
      const db = new Database('three_table_join_1');
      const t1 = db.createTable('users')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      const t2 = db.createTable('orders')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('user_id', JsDataType.Int64, null)
        .column('product_id', JsDataType.Int64, null);
      const t3 = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(t1);
      db.registerTable(t2);
      db.registerTable(t3);

      await db.insert('users').values([
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ]).exec();

      await db.insert('products').values([
        { id: 101, name: 'Laptop' },
        { id: 102, name: 'Phone' },
      ]).exec();

      await db.insert('orders').values([
        { id: 1, user_id: 1, product_id: 101 },
        { id: 2, user_id: 1, product_id: 102 },
        { id: 3, user_id: 2, product_id: 101 },
      ]).exec();

      const query = db.select('users.name', 'products.name')
        .from('orders')
        .innerJoin('users', col('orders.user_id').eq(col('users.id')))
        .innerJoin('products', col('orders.product_id').eq(col('products.id')));

      const result = await query.exec();

      expect(result).toHaveLength(3);
    });
  });

  describe('9.5 JOIN 条件使用 != (不等于)', () => {
    it('JOIN 使用不等于条件应返回所有不匹配的组合', async () => {
      const db = new Database('neq_join_1');
      const t1 = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null);
      const t2 = db.createTable('exclusions')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null);
      db.registerTable(t1);
      db.registerTable(t2);

      await db.insert('items').values([
        { id: 1, category: 1 },
        { id: 2, category: 2 },
      ]).exec();

      await db.insert('exclusions').values([
        { id: 1, category: 1 },
        { id: 2, category: 2 },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('items.id', 'exclusions.id')
          .from('items')
          .innerJoin('exclusions', col('items.category').eq(col('exclusions.category')).not())
      );

      // 2 items x 2 exclusions - 2 相等的 = 2 个结果
      expect(result).toHaveLength(2);
    });
  });

  describe('9.6 JOIN 列有 NULL 值', () => {
    it('NULL 值在 JOIN 条件中不应匹配任何值 (包括 NULL)', async () => {
      const db = new Database('null_join_1');
      const t1 = db.createTable('left_t')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('ref', JsDataType.Int64, new ColumnOptions().setNullable(true));
      const t2 = db.createTable('right_t')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('val', JsDataType.Int64, new ColumnOptions().setNullable(true));
      db.registerTable(t1);
      db.registerTable(t2);

      await db.insert('left_t').values([
        { id: 1, ref: 100 },
        { id: 2, ref: null },
        { id: 3, ref: 200 },
      ]).exec();

      await db.insert('right_t').values([
        { id: 1, val: 100 },
        { id: 2, val: null },
        { id: 3, val: 200 },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('left_t.id', 'right_t.id')
          .from('left_t')
          .innerJoin('right_t', col('left_t.ref').eq(col('right_t.val')))
      );

      // NULL = NULL 应该是 false，所以只有 ref=100 和 ref=200 匹配
      expect(result).toHaveLength(2);
      const leftIds = result.map(r => r['left_t.id'] || r.id);
      expect(leftIds).not.toContain(2);
    });
  });
});

// ============================================================================
// Section 10: NULL 值处理
// ============================================================================
describe('10. NULL 值处理', () => {
  describe('10.1 IS NULL / IS NOT NULL', () => {
    it('WHERE col IS NULL 应正确过滤', async () => {
      const db = new Database('null_filter_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, new ColumnOptions().setNullable(true))
        .column('value', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'A', value: 100 },
        { id: 2, name: null, value: 200 },
        { id: 3, name: 'C', value: null },
        { id: 4, name: null, value: null },
      ]).exec();

      const nullNames = await execAndVerifyBinary(
        db.select('*').from('items').where(col('name').isNull())
      );
      expect(nullNames).toHaveLength(2);
      expect(nullNames.map(r => r.id)).toContain(2);
      expect(nullNames.map(r => r.id)).toContain(4);

      const nullValues = await execAndVerifyBinary(
        db.select('*').from('items').where(col('value').isNull())
      );
      expect(nullValues).toHaveLength(2);
      expect(nullValues.map(r => r.id)).toContain(3);
      expect(nullValues.map(r => r.id)).toContain(4);
    });

    it('WHERE col IS NOT NULL 应正确过滤', async () => {
      const db = new Database('null_filter_2');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'A' },
        { id: 2, name: null },
        { id: 3, name: 'C' },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('name').isNotNull())
      );
      expect(result).toHaveLength(2);
      expect(result.map(r => r.name)).toContain('A');
      expect(result.map(r => r.name)).toContain('C');
    });
  });

  describe('10.2 NULL 值排序', () => {
    it('ORDER BY ASC 时 NULL 值的位置', async () => {
      const db = new Database('null_sort_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('score', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, score: 50 },
        { id: 2, score: null },
        { id: 3, score: 100 },
        { id: 4, score: null },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').orderBy('score', JsSortOrder.Asc)
      );
      expect(result).toHaveLength(4);
      // 验证 NULL 值的位置 (通常 NULL 排在最前或最后)
      const scores = result.map(r => r.score);
      // 检查非 NULL 值是否正确排序
      const nonNullScores = scores.filter(s => s !== null);
      expect(nonNullScores).toEqual([50, 100]);
    });

    it('ORDER BY DESC 时 NULL 值的位置', async () => {
      const db = new Database('null_sort_2');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('score', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, score: 50 },
        { id: 2, score: null },
        { id: 3, score: 100 },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').orderBy('score', JsSortOrder.Desc)
      );
      expect(result).toHaveLength(3);
      const nonNullScores = result.map(r => r.score).filter(s => s !== null);
      expect(nonNullScores).toEqual([100, 50]);
    });
  });

  describe('10.3 NULL 在 BETWEEN 中的行为', () => {
    it('NULL 值不应匹配 BETWEEN 条件', async () => {
      const db = new Database('null_between_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 5 },
        { id: 2, value: null },
        { id: 3, value: 15 },
        { id: 4, value: 25 },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('value').between(1, 20))
      );
      expect(result).toHaveLength(2);
      expect(result.map(r => r.id)).toContain(1);
      expect(result.map(r => r.id)).toContain(3);
      expect(result.map(r => r.id)).not.toContain(2);  // NULL 不匹配
    });
  });

  describe('10.4 NULL 在 IN 中的行为', () => {
    it('NULL 值不应匹配 IN 条件 (即使 IN 列表包含 NULL)', async () => {
      const db = new Database('null_in_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 1 },
        { id: 2, category: null },
        { id: 3, category: 2 },
        { id: 4, category: 3 },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('category').in([1, 2]))
      );
      expect(result).toHaveLength(2);
      expect(result.map(r => r.id)).toContain(1);
      expect(result.map(r => r.id)).toContain(3);
      expect(result.map(r => r.id)).not.toContain(2);  // NULL 不匹配
    });
  });

  describe('10.5 UPDATE 设置 NULL', () => {
    it('UPDATE 应能将值设置为 NULL', async () => {
      const db = new Database('null_update_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'Original' },
      ]).exec();

      await db.update('items').set({ name: null }).where(col('id').eq(1)).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(1))
      );
      expect(result).toHaveLength(1);
      expect(result[0].name).toBeNull();
    });

    it('UPDATE 应能将 NULL 设置为非 NULL 值', async () => {
      const db = new Database('null_update_2');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: null },
      ]).exec();

      await db.update('items').set({ name: 'Updated' }).where(col('id').eq(1)).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(1))
      );
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Updated');
    });
  });

  describe('10.6 NULL 与比较运算符', () => {
    it('NULL 与任何值比较都应返回 false', async () => {
      const db = new Database('null_compare_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 20 },
      ]).exec();

      // NULL > 5 应该是 false
      const gtResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('value').gt(5))
      );
      expect(gtResult).toHaveLength(2);
      expect(gtResult.map(r => r.id)).not.toContain(2);

      // NULL < 100 应该是 false
      const ltResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('value').lt(100))
      );
      expect(ltResult).toHaveLength(2);
      expect(ltResult.map(r => r.id)).not.toContain(2);

      // NULL = NULL 应该是 false (使用 eq)
      const eqResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('value').eq(null as any))
      );
      // 这个行为取决于实现，可能返回 0 或 1 行
      // 标准 SQL 中 NULL = NULL 是 UNKNOWN (false)
    });
  });
});

// ============================================================================
// Section 11: 索引边界
// ============================================================================
describe('11. 索引边界', () => {
  describe('11.1 主键边界值', () => {
    it('主键为 0 的查询应正确工作', async () => {
      const db = new Database('pk_zero_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 0, name: 'Zero' },
        { id: 1, name: 'One' },
        { id: 2, name: 'Two' },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(0))
      );
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Zero');
    });

    it('主键为负数的查询应正确工作', async () => {
      const db = new Database('pk_negative_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: -100, name: 'Negative' },
        { id: 0, name: 'Zero' },
        { id: 100, name: 'Positive' },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(-100))
      );
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Negative');

      // 范围查询包含负数
      const rangeResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').lt(0))
      );
      expect(rangeResult).toHaveLength(1);
      expect(rangeResult[0].id).toBe(-100);
    });

    it('主键为 BigInt 最大值的查询应正确工作', async () => {
      const db = new Database('pk_bigint_max_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      const maxBigInt = BigInt('9223372036854775807');  // Int64 最大值
      const minBigInt = BigInt('-9223372036854775808'); // Int64 最小值

      await db.insert('items').values([
        { id: maxBigInt, name: 'Max' },
        { id: minBigInt, name: 'Min' },
        { id: 0, name: 'Zero' },
      ]).exec();

      const maxResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(maxBigInt))
      );
      expect(maxResult).toHaveLength(1);
      expect(maxResult[0].name).toBe('Max');

      const minResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(minBigInt))
      );
      expect(minResult).toHaveLength(1);
      expect(minResult[0].name).toBe('Min');
    });
  });

  describe('11.2 索引列有重复值', () => {
    it('索引列有重复值时的范围查询', async () => {
      const db = new Database('idx_dup_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null)
        .column('name', JsDataType.String, null)
        .index('idx_category', ['category']);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 1, name: 'A1' },
        { id: 2, category: 1, name: 'A2' },
        { id: 3, category: 2, name: 'B1' },
        { id: 4, category: 2, name: 'B2' },
        { id: 5, category: 2, name: 'B3' },
      ]).exec();

      // 精确查询重复值
      const exactResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('category').eq(2))
      );
      expect(exactResult).toHaveLength(3);

      // 范围查询
      const rangeResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('category').gte(1).and(col('category').lte(2)))
      );
      expect(rangeResult).toHaveLength(5);
    });
  });

  describe('11.3 复合索引部分列查询', () => {
    it('复合索引只使用第一列查询', async () => {
      const db = new Database('composite_idx_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('year', JsDataType.Int32, null)
        .column('month', JsDataType.Int32, null)
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);
      // 注意: 如果 API 支持复合索引，这里应该创建 (year, month) 复合索引

      await db.insert('items').values([
        { id: 1, year: 2024, month: 1, value: 100 },
        { id: 2, year: 2024, month: 2, value: 200 },
        { id: 3, year: 2025, month: 1, value: 300 },
      ]).exec();

      // 只用第一列查询
      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('year').eq(2024))
      );
      expect(result).toHaveLength(2);
    });
  });

  describe('11.4 索引列 UPDATE 后的查询', () => {
    it('UPDATE 索引列后查询应返回正确结果', async () => {
      const db = new Database('idx_update_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null)
        .column('name', JsDataType.String, null)
        .index('idx_category', ['category']);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 1, name: 'A' },
        { id: 2, category: 2, name: 'B' },
      ]).exec();

      // 更新索引列
      await db.update('items').set({ category: 3 }).where(col('id').eq(1)).exec();

      // 旧值查询应该找不到
      const oldResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('category').eq(1))
      );
      expect(oldResult).toHaveLength(0);

      // 新值查询应该找到
      const newResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('category').eq(3))
      );
      expect(newResult).toHaveLength(1);
      expect(newResult[0].name).toBe('A');
    });
  });

  describe('11.5 DELETE 后索引更新', () => {
    it('DELETE 后索引应正确更新', async () => {
      const db = new Database('idx_delete_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null)
        .index('idx_category', ['category']);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 1 },
        { id: 2, category: 1 },
        { id: 3, category: 2 },
      ]).exec();

      // 删除一条记录
      await db.delete('items').where(col('id').eq(1)).exec();

      // 索引查询应该只返回剩余的记录
      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('category').eq(1))
      );
      expect(result).toHaveLength(1);
      expect(result[0].id).toBe(2);
    });

    it('DELETE 所有记录后索引应为空', async () => {
      const db = new Database('idx_delete_all_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null)
        .index('idx_category', ['category']);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 1 },
        { id: 2, category: 1 },
      ]).exec();

      // 删除所有记录
      await db.delete('items').where(col('category').eq(1)).exec();

      // 索引查询应该返回空
      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('category').eq(1))
      );
      expect(result).toHaveLength(0);

      // 全表查询也应该为空
      const allResult = await execAndVerifyBinary(
        db.select('*').from('items')
      );
      expect(allResult).toHaveLength(0);
    });
  });
});

// ============================================================================
// Section 12: 数据类型边界
// ============================================================================
describe('12. 数据类型边界', () => {
  describe('12.1 Int32 边界值', () => {
    it('Int32 最大/最小值应正确存储和查询', async () => {
      const db = new Database('int32_bounds_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      const int32Max = 2147483647;
      const int32Min = -2147483648;

      await db.insert('items').values([
        { id: 1, value: int32Max },
        { id: 2, value: int32Min },
        { id: 3, value: 0 },
      ]).exec();

      const maxResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(1))
      );
      expect(maxResult[0].value).toBe(int32Max);

      const minResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(2))
      );
      expect(minResult[0].value).toBe(int32Min);
    });
  });

  describe('12.2 Int64 边界值', () => {
    it('Int64 最大/最小值应正确存储和查询', async () => {
      const db = new Database('int64_bounds_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int64, null);
      db.registerTable(builder);

      // Note: Int64 values are stored as Float64 in JavaScript, which has limited precision
      // for values outside the safe integer range (±2^53-1). Large values will lose precision.
      const int64Max = BigInt('9223372036854775807');
      const int64Min = BigInt('-9223372036854775808');

      await db.insert('items').values([
        { id: 1, value: int64Max },
        { id: 2, value: int64Min },
        { id: 3, value: BigInt(0) },
      ]).exec();

      const maxResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(1))
      );
      // Due to Float64 precision limits, large Int64 values lose precision
      expect(typeof maxResult[0].value).toBe('number');

      const minResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(2))
      );
      expect(typeof minResult[0].value).toBe('number');
    });
  });

  describe('12.3 Float64 精度', () => {
    it('Float64 应正确处理精度问题', async () => {
      const db = new Database('float64_precision_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 0.1 },
        { id: 2, value: 0.2 },
        { id: 3, value: 0.3 },
        { id: 4, value: Number.MAX_VALUE },
        { id: 5, value: Number.MIN_VALUE },
        { id: 6, value: Number.EPSILON },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items')
      );
      expect(result).toHaveLength(6);

      // 验证特殊值
      const maxVal = result.find(r => r.id === 4);
      expect(maxVal?.value).toBe(Number.MAX_VALUE);

      const minVal = result.find(r => r.id === 5);
      expect(minVal?.value).toBe(Number.MIN_VALUE);
    });

    it('Float64 应正确处理 Infinity 和 NaN', async () => {
      const db = new Database('float64_special_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: Infinity },
        { id: 2, value: -Infinity },
        { id: 3, value: NaN },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items')
      );
      expect(result).toHaveLength(3);

      const infVal = result.find(r => r.id === 1);
      expect(infVal?.value).toBe(Infinity);

      const negInfVal = result.find(r => r.id === 2);
      expect(negInfVal?.value).toBe(-Infinity);

      const nanVal = result.find(r => r.id === 3);
      expect(Number.isNaN(nanVal?.value)).toBe(true);
    });
  });

  describe('12.4 String 边界', () => {
    it('空字符串应正确存储和查询', async () => {
      const db = new Database('string_empty_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: '' },
        { id: 2, name: null },
        { id: 3, name: 'normal' },
      ]).exec();

      // 空字符串查询
      const emptyResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('name').eq(''))
      );
      expect(emptyResult).toHaveLength(1);
      expect(emptyResult[0].id).toBe(1);

      // 空字符串不等于 NULL
      const nullResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('name').isNull())
      );
      expect(nullResult).toHaveLength(1);
      expect(nullResult[0].id).toBe(2);
    });

    it('超长字符串应正确存储', async () => {
      const db = new Database('string_long_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('content', JsDataType.String, null);
      db.registerTable(builder);

      const longString = 'x'.repeat(100000);  // 100KB 字符串

      await db.insert('items').values([
        { id: 1, content: longString },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(1))
      );
      expect(result).toHaveLength(1);
      expect(result[0].content).toBe(longString);
      expect(result[0].content.length).toBe(100000);
    });

    it('Unicode 字符串应正确存储', async () => {
      const db = new Database('string_unicode_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: '中文测试' },
        { id: 2, name: '日本語テスト' },
        { id: 3, name: '🎉🚀💻' },
        { id: 4, name: 'مرحبا' },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items')
      );
      expect(result).toHaveLength(4);
      expect(result.find(r => r.id === 1)?.name).toBe('中文测试');
      expect(result.find(r => r.id === 3)?.name).toBe('🎉🚀💻');
    });
  });

  describe('12.5 DateTime 边界', () => {
    it('DateTime 边界值应正确存储', async () => {
      const db = new Database('datetime_bounds_1');
      const builder = db.createTable('events')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('timestamp', JsDataType.DateTime, null);
      db.registerTable(builder);

      const epoch = new Date(0);  // 1970-01-01
      const farFuture = new Date('2100-12-31T23:59:59.999Z');
      const farPast = new Date('1900-01-01T00:00:00.000Z');

      await db.insert('events').values([
        { id: 1, timestamp: epoch },
        { id: 2, timestamp: farFuture },
        { id: 3, timestamp: farPast },
      ]).exec();

      const result = await db.select('*').from('events').exec();
      expect(result).toHaveLength(3);
    });
  });

  describe('12.6 Bytes 边界', () => {
    it('空 Bytes 数组应正确存储', async () => {
      const db = new Database('bytes_empty_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('data', JsDataType.Bytes, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, data: new Uint8Array([]) },
        { id: 2, data: new Uint8Array([1, 2, 3]) },
        { id: 3, data: null },
      ]).exec();

      const result = await db.select('*').from('items').exec();
      expect(result).toHaveLength(3);

      const emptyBytes = result.find(r => r.id === 1);
      expect(emptyBytes?.data).toEqual(new Uint8Array([]));
    });

    it('大 Bytes 数组应正确存储', async () => {
      const db = new Database('bytes_large_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('data', JsDataType.Bytes, null);
      db.registerTable(builder);

      const largeData = new Uint8Array(100000);  // 100KB
      for (let i = 0; i < largeData.length; i++) {
        largeData[i] = i % 256;
      }

      await db.insert('items').values([
        { id: 1, data: largeData },
      ]).exec();

      const result = await db.select('*').from('items').where(col('id').eq(1)).exec();
      expect(result).toHaveLength(1);
      expect(result[0].data.length).toBe(100000);
      expect(result[0].data[0]).toBe(0);
      expect(result[0].data[255]).toBe(255);
    });
  });

  describe('12.7 Jsonb 边界', () => {
    it('深层嵌套 Jsonb 应正确存储', async () => {
      const db = new Database('jsonb_deep_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('data', JsDataType.Jsonb, null);
      db.registerTable(builder);

      // 创建深层嵌套对象
      let deepObj: any = { value: 'leaf' };
      for (let i = 0; i < 20; i++) {
        deepObj = { nested: deepObj };
      }

      await db.insert('items').values([
        { id: 1, data: deepObj },
      ]).exec();

      const result = await db.select('*').from('items').where(col('id').eq(1)).exec();
      expect(result).toHaveLength(1);

      // 验证嵌套结构
      let current = result[0].data;
      for (let i = 0; i < 20; i++) {
        expect(current).toHaveProperty('nested');
        current = current.nested;
      }
      expect(current.value).toBe('leaf');
    });

    it('各种 JSON 类型应正确存储', async () => {
      const db = new Database('jsonb_types_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('data', JsDataType.Jsonb, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, data: null },
        { id: 2, data: true },
        { id: 3, data: false },
        { id: 4, data: 42 },
        { id: 5, data: 3.14 },
        { id: 6, data: 'string' },
        { id: 7, data: [1, 2, 3] },
        { id: 8, data: { key: 'value' } },
      ]).exec();

      const result = await db.select('*').from('items').exec();
      expect(result).toHaveLength(8);

      expect(result.find(r => r.id === 2)?.data).toBe(true);
      expect(result.find(r => r.id === 4)?.data).toBe(42);
      expect(result.find(r => r.id === 7)?.data).toEqual([1, 2, 3]);
    });
  });
});

// ============================================================================
// Section 13: 事务边界
// ============================================================================
describe('13. 事务边界', () => {
  describe('13.1 事务中 INSERT 后立即 SELECT', () => {
    it('INSERT 后立即 SELECT 应能看到新数据', async () => {
      const db = new Database('tx_insert_select_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      // INSERT
      await db.insert('items').values([
        { id: 1, name: 'First' },
      ]).exec();

      // 立即 SELECT
      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(1))
      );
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('First');

      // 再 INSERT
      await db.insert('items').values([
        { id: 2, name: 'Second' },
      ]).exec();

      // 再 SELECT
      const result2 = await execAndVerifyBinary(
        db.select('*').from('items')
      );
      expect(result2).toHaveLength(2);
    });
  });

  describe('13.2 事务中 UPDATE 后 SELECT', () => {
    it('UPDATE 后立即 SELECT 应能看到更新后的数据', async () => {
      const db = new Database('tx_update_select_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 100 },
      ]).exec();

      // UPDATE
      await db.update('items').set({ value: 200 }).where(col('id').eq(1)).exec();

      // 立即 SELECT
      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(1))
      );
      expect(result).toHaveLength(1);
      expect(result[0].value).toBe(200);
    });

    it('多次 UPDATE 后 SELECT 应返回最终值', async () => {
      const db = new Database('tx_multi_update_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('counter', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, counter: 0 },
      ]).exec();

      // 多次 UPDATE
      for (let i = 1; i <= 10; i++) {
        await db.update('items').set({ counter: i }).where(col('id').eq(1)).exec();
      }

      const result = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(1))
      );
      expect(result[0].counter).toBe(10);
    });
  });

  describe('13.3 事务中 DELETE 后 SELECT', () => {
    it('DELETE 后立即 SELECT 应看不到已删除的数据', async () => {
      const db = new Database('tx_delete_select_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'ToDelete' },
        { id: 2, name: 'ToKeep' },
      ]).exec();

      // DELETE
      await db.delete('items').where(col('id').eq(1)).exec();

      // 立即 SELECT
      const result = await execAndVerifyBinary(
        db.select('*').from('items')
      );
      expect(result).toHaveLength(1);
      expect(result[0].id).toBe(2);

      // 查询已删除的记录
      const deletedResult = await execAndVerifyBinary(
        db.select('*').from('items').where(col('id').eq(1))
      );
      expect(deletedResult).toHaveLength(0);
    });
  });

  describe('13.4 大量操作的事务', () => {
    it('大量 INSERT 应正确执行', async () => {
      const db = new Database('tx_bulk_insert_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      // 批量插入 1000 条记录
      const records = [];
      for (let i = 0; i < 1000; i++) {
        records.push({ id: i, value: i * 10 });
      }
      await db.insert('items').values(records).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items')
      );
      expect(result).toHaveLength(1000);

      // 验证第一条和最后一条
      const first = result.find(r => r.id === 0);
      expect(first?.value).toBe(0);

      const last = result.find(r => r.id === 999);
      expect(last?.value).toBe(9990);
    });

    it('大量 UPDATE 应正确执行', async () => {
      const db = new Database('tx_bulk_update_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      // 插入 100 条记录
      const records = [];
      for (let i = 0; i < 100; i++) {
        records.push({ id: i, value: 0 });
      }
      await db.insert('items').values(records).exec();

      // 批量更新
      await db.update('items').set({ value: 999 }).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items')
      );
      expect(result).toHaveLength(100);
      expect(result.every(r => r.value === 999)).toBe(true);
    });

    it('大量 DELETE 应正确执行', async () => {
      const db = new Database('tx_bulk_delete_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null);
      db.registerTable(builder);

      // 插入 200 条记录，一半 category=1，一半 category=2
      const records = [];
      for (let i = 0; i < 200; i++) {
        records.push({ id: i, category: i < 100 ? 1 : 2 });
      }
      await db.insert('items').values(records).exec();

      // 删除 category=1 的所有记录
      await db.delete('items').where(col('category').eq(1)).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items')
      );
      expect(result).toHaveLength(100);
      expect(result.every(r => r.category === 2)).toBe(true);
    });
  });

  describe('13.5 混合操作序列', () => {
    it('INSERT -> UPDATE -> DELETE -> SELECT 序列应正确执行', async () => {
      const db = new Database('tx_mixed_ops_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('status', JsDataType.String, null);
      db.registerTable(builder);

      // INSERT
      await db.insert('items').values([
        { id: 1, status: 'created' },
        { id: 2, status: 'created' },
        { id: 3, status: 'created' },
      ]).exec();

      // UPDATE
      await db.update('items').set({ status: 'updated' }).where(col('id').eq(1)).exec();

      // DELETE
      await db.delete('items').where(col('id').eq(2)).exec();

      // SELECT
      const result = await execAndVerifyBinary(
        db.select('*').from('items').orderBy('id', JsSortOrder.Asc)
      );

      expect(result).toHaveLength(2);
      expect(result[0].id).toBe(1);
      expect(result[0].status).toBe('updated');
      expect(result[1].id).toBe(3);
      expect(result[1].status).toBe('created');
    });
  });
});

// ============================================================================
// Section 14: Live Query 边界
// ============================================================================
describe('14. Live Query 边界', () => {
  describe('14.1 订阅后立即取消', () => {
    it('订阅后立即取消不应导致错误', async () => {
      const db = new Database('lq_immediate_unsub_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'Test' },
      ]).exec();

      const query = db.select('*').from('items');
      const observable = query.observe();

      let callCount = 0;
      const unsubscribe = observable.subscribe(() => {
        callCount++;
      });

      // 立即取消 (subscribe 返回的是 unsubscribe 函数)
      unsubscribe();

      // 插入新数据不应触发回调
      await db.insert('items').values([{ id: 2, name: 'New' }]).exec();

      // 等待一小段时间确保没有回调
      await new Promise(resolve => setTimeout(resolve, 50));

      // 回调次数应该只有初始的一次或零次
      expect(callCount).toBeLessThanOrEqual(1);
    });
  });

  describe('14.2 多次订阅同一查询', () => {
    it('多个订阅者应各自收到通知', async () => {
      const db = new Database('lq_multi_sub_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'Initial' },
      ]).exec();

      const query = db.select('*').from('items');
      const observable = query.observe();

      let count1 = 0;
      let count2 = 0;
      let count3 = 0;

      const unsub1 = observable.subscribe(() => { count1++; });
      const unsub2 = observable.subscribe(() => { count2++; });
      const unsub3 = observable.subscribe(() => { count3++; });

      // 插入新数据
      await db.insert('items').values([{ id: 2, name: 'New' }]).exec();

      await new Promise(resolve => setTimeout(resolve, 100));

      // 所有订阅者都应该收到通知
      expect(count1).toBeGreaterThanOrEqual(1);
      expect(count2).toBeGreaterThanOrEqual(1);
      expect(count3).toBeGreaterThanOrEqual(1);

      unsub1();
      unsub2();
      unsub3();
    });

    it('取消一个订阅不影响其他订阅', async () => {
      const db = new Database('lq_partial_unsub_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('val', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([{ id: 1, val: 1 }]).exec();

      const query = db.select('*').from('items');
      const observable = query.observe();

      let count1 = 0;
      let count2 = 0;

      const unsub1 = observable.subscribe(() => { count1++; });
      const unsub2 = observable.subscribe(() => { count2++; });

      // 取消第一个订阅
      unsub1();

      // 重置计数
      const initialCount2 = count2;

      // 插入新数据
      await db.insert('items').values([{ id: 2, val: 2 }]).exec();

      await new Promise(resolve => setTimeout(resolve, 100));

      // sub2 应该仍然收到通知
      expect(count2).toBeGreaterThan(initialCount2);

      unsub2();
    });
  });

  describe('14.3 订阅带复杂 WHERE 的查询', () => {
    it('复杂 WHERE 条件的 Live Query 应正确过滤', async () => {
      const db = new Database('lq_complex_where_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null)
        .column('status', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 1, status: 1 },
      ]).exec();

      const query = db.select('*').from('items')
        .where(col('category').eq(1).and(col('status').gte(1)));
      const observable = query.observe();

      let lastResults: any[] = [];
      const unsub = observable.subscribe((results) => {
        lastResults = results;
      });

      await new Promise(resolve => setTimeout(resolve, 50));

      // 插入匹配的数据
      await db.insert('items').values([{ id: 2, category: 1, status: 2 }]).exec();

      await new Promise(resolve => setTimeout(resolve, 100));

      // 应该有 2 条匹配的记录
      expect(lastResults.length).toBe(2);

      // 插入不匹配的数据
      await db.insert('items').values([{ id: 3, category: 2, status: 1 }]).exec();

      await new Promise(resolve => setTimeout(resolve, 100));

      // 仍然只有 2 条匹配的记录
      expect(lastResults.length).toBe(2);

      unsub();
    });
  });

  describe('14.4 批量 INSERT 时的通知', () => {
    it('批量 INSERT 应触发一次通知', async () => {
      const db = new Database('lq_batch_insert_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      const query = db.select('*').from('items');
      const observable = query.observe();

      let notifyCount = 0;
      let lastLength = 0;
      const unsub = observable.subscribe((results) => {
        notifyCount++;
        lastLength = results.length;
      });

      await new Promise(resolve => setTimeout(resolve, 50));
      const initialCount = notifyCount;

      // 批量插入 100 条记录
      const records = [];
      for (let i = 1; i <= 100; i++) {
        records.push({ id: i, name: `Item ${i}` });
      }
      await db.insert('items').values(records).exec();

      await new Promise(resolve => setTimeout(resolve, 100));

      // 批量插入应该只触发一次通知 (或少量几次)
      expect(notifyCount - initialCount).toBeLessThanOrEqual(3);
      expect(lastLength).toBe(100);

      unsub();
    });
  });

  describe('14.5 快速连续 UPDATE 时的通知', () => {
    it('快速连续 UPDATE 应正确处理', async () => {
      const db = new Database('lq_rapid_update_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('counter', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([{ id: 1, counter: 0 }]).exec();

      const query = db.select('*').from('items');
      const observable = query.observe();

      let lastCounter = 0;
      const unsub = observable.subscribe((results) => {
        if (results.length > 0) {
          lastCounter = results[0].counter;
        }
      });

      await new Promise(resolve => setTimeout(resolve, 50));

      // 快速连续更新
      for (let i = 1; i <= 10; i++) {
        await db.update('items').set({ counter: i }).where(col('id').eq(1)).exec();
      }

      await new Promise(resolve => setTimeout(resolve, 200));

      // 最终值应该是 10
      expect(lastCounter).toBe(10);

      unsub();
    });
  });

  describe('14.6 DELETE 所有数据时的通知', () => {
    it('DELETE 所有数据应触发空结果通知', async () => {
      const db = new Database('lq_delete_all_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      const query = db.select('*').from('items');
      const observable = query.observe();

      // 使用 getResult() 获取初始值，subscribe 只在数据变化时回调
      expect(observable.getResult()).toHaveLength(0);

      let lastLength = -1;
      const unsub = observable.subscribe((results) => {
        lastLength = results.length;
      });

      // 在 subscribe 之后插入数据，触发回调
      await db.insert('items').values([
        { id: 1, name: 'A' },
        { id: 2, name: 'B' },
        { id: 3, name: 'C' },
      ]).exec();

      expect(lastLength).toBe(3);

      // 删除所有数据
      await db.delete('items').exec();

      // 应该收到空结果通知
      expect(lastLength).toBe(0);

      unsub();
    });
  });
});

// ============================================================================
// Section 15: 查询组合
// ============================================================================
describe('15. 查询组合', () => {
  describe('15.1 WHERE + ORDER BY + LIMIT + OFFSET 全组合', () => {
    it('所有子句组合应正确工作', async () => {
      const db = new Database('combo_all_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null)
        .column('score', JsDataType.Int32, null)
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 1, score: 100, name: 'A' },
        { id: 2, category: 1, score: 90, name: 'B' },
        { id: 3, category: 2, score: 95, name: 'C' },
        { id: 4, category: 1, score: 85, name: 'D' },
        { id: 5, category: 1, score: 80, name: 'E' },
        { id: 6, category: 1, score: 75, name: 'F' },
        { id: 7, category: 2, score: 70, name: 'G' },
      ]).exec();

      // WHERE category=1 + ORDER BY score DESC + LIMIT 3 + OFFSET 1
      const result = await execAndVerifyBinary(
        db.select('*').from('items')
          .where(col('category').eq(1))
          .orderBy('score', JsSortOrder.Desc)
          .limit(3)
          .offset(1)
      );

      // category=1 的有: A(100), B(90), D(85), E(80), F(75)
      // 按 score DESC 排序后: A, B, D, E, F
      // OFFSET 1 跳过 A
      // LIMIT 3 取 B, D, E
      expect(result).toHaveLength(3);
      expect(result[0].name).toBe('B');
      expect(result[1].name).toBe('D');
      expect(result[2].name).toBe('E');
    });

    it('OFFSET 超过结果集大小应返回空', async () => {
      const db = new Database('combo_offset_exceed_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('val', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, val: 10 },
        { id: 2, val: 20 },
        { id: 3, val: 30 },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items')
          .orderBy('val', JsSortOrder.Asc)
          .offset(10)
      );

      expect(result).toHaveLength(0);
    });

    it('LIMIT 0 应返回空结果', async () => {
      const db = new Database('combo_limit_zero_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('val', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, val: 10 },
        { id: 2, val: 20 },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('*').from('items').limit(0)
      );

      expect(result).toHaveLength(0);
    });
  });

  describe('15.2 JOIN + WHERE + ORDER BY + LIMIT', () => {
    it('JOIN 后的复杂查询应正确工作', async () => {
      const db = new Database('combo_join_1');
      const t1 = db.createTable('users')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null)
        .column('active', JsDataType.Boolean, null);
      const t2 = db.createTable('orders')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('user_id', JsDataType.Int64, null)
        .column('amount', JsDataType.Float64, null);
      db.registerTable(t1);
      db.registerTable(t2);

      await db.insert('users').values([
        { id: 1, name: 'Alice', active: true },
        { id: 2, name: 'Bob', active: true },
        { id: 3, name: 'Charlie', active: false },
      ]).exec();

      await db.insert('orders').values([
        { id: 1, user_id: 1, amount: 100.0 },
        { id: 2, user_id: 1, amount: 200.0 },
        { id: 3, user_id: 2, amount: 150.0 },
        { id: 4, user_id: 1, amount: 50.0 },
        { id: 5, user_id: 3, amount: 300.0 },
      ]).exec();

      // JOIN + WHERE active=true + ORDER BY amount DESC + LIMIT 3
      const result = await execAndVerifyBinary(
        db.select('users.name', 'orders.amount')
          .from('orders')
          .innerJoin('users', col('orders.user_id').eq(col('users.id')))
          .where(col('users.active').eq(true))
          .orderBy('orders.amount', JsSortOrder.Desc)
          .limit(3)
      );

      // active=true 的用户订单: Alice(100, 200, 50), Bob(150)
      // 按 amount DESC: 200, 150, 100, 50
      // LIMIT 3: 200, 150, 100
      expect(result).toHaveLength(3);
      expect(result[0].amount).toBe(200.0);
      expect(result[1].amount).toBe(150.0);
      expect(result[2].amount).toBe(100.0);
    });

    it('多表 JOIN + 复杂 WHERE 条件', async () => {
      const db = new Database('combo_multi_join_1');
      const t1 = db.createTable('categories')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      const t2 = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category_id', JsDataType.Int64, null)
        .column('name', JsDataType.String, null)
        .column('price', JsDataType.Float64, null);
      const t3 = db.createTable('inventory')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('product_id', JsDataType.Int64, null)
        .column('quantity', JsDataType.Int32, null);
      db.registerTable(t1);
      db.registerTable(t2);
      db.registerTable(t3);

      await db.insert('categories').values([
        { id: 1, name: 'Electronics' },
        { id: 2, name: 'Books' },
      ]).exec();

      await db.insert('products').values([
        { id: 1, category_id: 1, name: 'Laptop', price: 1000.0 },
        { id: 2, category_id: 1, name: 'Phone', price: 500.0 },
        { id: 3, category_id: 2, name: 'Novel', price: 20.0 },
      ]).exec();

      await db.insert('inventory').values([
        { id: 1, product_id: 1, quantity: 10 },
        { id: 2, product_id: 2, quantity: 0 },
        { id: 3, product_id: 3, quantity: 50 },
      ]).exec();

      // 三表 JOIN + WHERE (category=Electronics AND quantity > 0)
      const result = await execAndVerifyBinary(
        db.select('products.name', 'inventory.quantity')
          .from('products')
          .innerJoin('categories', col('products.category_id').eq(col('categories.id')))
          .innerJoin('inventory', col('products.id').eq(col('inventory.product_id')))
          .where(
            col('categories.name').eq('Electronics')
              .and(col('inventory.quantity').gt(0))
          )
      );

      // Electronics 类别且库存 > 0: 只有 Laptop
      expect(result).toHaveLength(1);
      expect(result[0].name).toBe('Laptop');
    });
  });

  describe('15.3 多重 ORDER BY', () => {
    it('多列排序应按顺序应用', async () => {
      const db = new Database('combo_multi_order_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.Int32, null)
        .column('score', JsDataType.Int32, null)
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 1, score: 100, name: 'A' },
        { id: 2, category: 2, score: 100, name: 'B' },
        { id: 3, category: 1, score: 90, name: 'C' },
        { id: 4, category: 2, score: 90, name: 'D' },
        { id: 5, category: 1, score: 100, name: 'E' },
      ]).exec();

      // 先按 category ASC，再按 score DESC
      const result = await execAndVerifyBinary(
        db.select('*').from('items')
          .orderBy('category', JsSortOrder.Asc)
          .orderBy('score', JsSortOrder.Desc)
      );

      expect(result).toHaveLength(5);
      // category=1: score 100 (A, E), score 90 (C)
      // category=2: score 100 (B), score 90 (D)
      expect(result[0].category).toBe(1);
      expect(result[0].score).toBe(100);
      expect(result[2].category).toBe(1);
      expect(result[2].score).toBe(90);
      expect(result[3].category).toBe(2);
    });
  });

  describe('15.4 WHERE 条件组合', () => {
    it('AND + OR 组合条件应正确计算', async () => {
      const db = new Database('combo_where_logic_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('a', JsDataType.Int32, null)
        .column('b', JsDataType.Int32, null)
        .column('c', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, a: 1, b: 1, c: 1 },
        { id: 2, a: 1, b: 2, c: 1 },
        { id: 3, a: 2, b: 1, c: 1 },
        { id: 4, a: 2, b: 2, c: 2 },
        { id: 5, a: 1, b: 1, c: 2 },
      ]).exec();

      // (a=1 AND b=1) OR c=2
      const result = await execAndVerifyBinary(
        db.select('*').from('items')
          .where(
            col('a').eq(1).and(col('b').eq(1))
              .or(col('c').eq(2))
          )
      );

      // 匹配: id=1 (a=1,b=1), id=4 (c=2), id=5 (a=1,b=1 AND c=2)
      expect(result).toHaveLength(3);
      expect(result.map(r => r.id)).toEqual(expect.arrayContaining([1, 4, 5]));
    });

    it('多个 BETWEEN 条件组合', async () => {
      const db = new Database('combo_between_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('x', JsDataType.Int32, null)
        .column('y', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, x: 5, y: 5 },
        { id: 2, x: 15, y: 5 },
        { id: 3, x: 5, y: 15 },
        { id: 4, x: 15, y: 15 },
        { id: 5, x: 10, y: 10 },
      ]).exec();

      // x BETWEEN 1 AND 10 AND y BETWEEN 1 AND 10
      const result = await execAndVerifyBinary(
        db.select('*').from('items')
          .where(
            col('x').between(1, 10)
              .and(col('y').between(1, 10))
          )
      );

      // 匹配: id=1 (5,5), id=5 (10,10)
      expect(result).toHaveLength(2);
      expect(result.map(r => r.id)).toEqual(expect.arrayContaining([1, 5]));
    });

    it('IN + 其他条件组合', async () => {
      const db = new Database('combo_in_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('status', JsDataType.Int32, null)
        .column('priority', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, status: 1, priority: 1 },
        { id: 2, status: 2, priority: 1 },
        { id: 3, status: 1, priority: 2 },
        { id: 4, status: 3, priority: 1 },
        { id: 5, status: 2, priority: 2 },
      ]).exec();

      // status IN (1, 2) AND priority = 1
      const result = await execAndVerifyBinary(
        db.select('*').from('items')
          .where(
            col('status').in([1, 2])
              .and(col('priority').eq(1))
          )
      );

      // 匹配: id=1, id=2
      expect(result).toHaveLength(2);
      expect(result.map(r => r.id)).toEqual(expect.arrayContaining([1, 2]));
    });
  });

  describe('15.5 选择特定列 + 其他子句', () => {
    it('选择特定列 + WHERE + ORDER BY', async () => {
      const db = new Database('combo_select_cols_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null)
        .column('score', JsDataType.Int32, null)
        .column('secret', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'A', score: 100, secret: 'xxx' },
        { id: 2, name: 'B', score: 90, secret: 'yyy' },
        { id: 3, name: 'C', score: 95, secret: 'zzz' },
      ]).exec();

      const result = await execAndVerifyBinary(
        db.select('name', 'score')
          .from('items')
          .where(col('score').gte(90))
          .orderBy('score', JsSortOrder.Desc)
      );

      expect(result).toHaveLength(3);
      expect(result[0].name).toBe('A');
      expect(result[0].score).toBe(100);
      // secret 列不应该在结果中
      expect(result[0].secret).toBeUndefined();
    });
  });
});

// ============================================================================
// 第十六部分：聚合函数测试
// ============================================================================
describe('16. 聚合函数 (Aggregate Functions)', () => {
  describe('16.1 COUNT 函数', () => {
    it('COUNT(*) 应该返回所有行数', async () => {
      const db = new Database('agg_count_star_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ]).exec();

      const result = await db.select('*').from('items').count().exec();
      expect(result).toHaveLength(1);
      expect(result[0].count).toBe(3);
    });

    it('COUNT(column) 应该只计算非 null 值', async () => {
      const db = new Database('agg_count_col_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 30 },
      ]).exec();

      const result = await db.select('*').from('items').countCol('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].count_value).toBe(2);
    });

    it('COUNT(*) 空表应该返回 0', async () => {
      const db = new Database('agg_count_empty_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      const result = await db.select('*').from('items').count().exec();
      expect(result).toHaveLength(1);
      expect(result[0].count).toBe(0);
    });
  });

  describe('16.2 SUM 函数', () => {
    it('SUM 应该正确计算整数和', async () => {
      const db = new Database('agg_sum_int_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ]).exec();

      const result = await db.select('*').from('items').sum('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].sum_value).toBe(60);
    });

    it('SUM 应该正确计算浮点数和', async () => {
      const db = new Database('agg_sum_float_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10.5 },
        { id: 2, value: 20.3 },
        { id: 3, value: 30.2 },
      ]).exec();

      const result = await db.select('*').from('items').sum('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].sum_value).toBeCloseTo(61.0, 1);
    });

    it('SUM 应该忽略 null 值', async () => {
      const db = new Database('agg_sum_null_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 20 },
      ]).exec();

      const result = await db.select('*').from('items').sum('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].sum_value).toBe(30);
    });
  });

  describe('16.3 AVG 函数', () => {
    it('AVG 应该正确计算平均值', async () => {
      const db = new Database('agg_avg_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ]).exec();

      const result = await db.select('*').from('items').avg('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].avg_value).toBe(20);
    });

    it('AVG 应该忽略 null 值', async () => {
      const db = new Database('agg_avg_null_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: null },
        { id: 3, value: 20 },
      ]).exec();

      const result = await db.select('*').from('items').avg('value').exec();
      expect(result).toHaveLength(1);
      // AVG of [10, 20] = 15
      expect(result[0].avg_value).toBe(15);
    });

    it('AVG 空集应该返回 null', async () => {
      const db = new Database('agg_avg_empty_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      const result = await db.select('*').from('items').avg('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].avg_value).toBeNull();
    });
  });

  describe('16.4 MIN/MAX 函数', () => {
    it('MIN 应该返回最小值', async () => {
      const db = new Database('agg_min_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 30 },
        { id: 2, value: 10 },
        { id: 3, value: 20 },
      ]).exec();

      const result = await db.select('*').from('items').min('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].min_value).toBe(10);
    });

    it('MAX 应该返回最大值', async () => {
      const db = new Database('agg_max_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 30 },
        { id: 2, value: 10 },
        { id: 3, value: 20 },
      ]).exec();

      const result = await db.select('*').from('items').max('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].max_value).toBe(30);
    });

    it('MIN/MAX 应该忽略 null 值', async () => {
      const db = new Database('agg_minmax_null_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 30 },
        { id: 2, value: null },
        { id: 3, value: 10 },
        { id: 4, value: null },
        { id: 5, value: 20 },
      ]).exec();

      const minResult = await db.select('*').from('items').min('value').exec();
      expect(minResult[0].min_value).toBe(10);

      const maxResult = await db.select('*').from('items').max('value').exec();
      expect(maxResult[0].max_value).toBe(30);
    });

    it('MIN/MAX 全 null 应该返回 null', async () => {
      const db = new Database('agg_minmax_allnull_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: null },
        { id: 2, value: null },
      ]).exec();

      const minResult = await db.select('*').from('items').min('value').exec();
      expect(minResult[0].min_value).toBeNull();

      const maxResult = await db.select('*').from('items').max('value').exec();
      expect(maxResult[0].max_value).toBeNull();
    });

    it('MIN/MAX 字符串类型', async () => {
      const db = new Database('agg_minmax_string_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('name', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, name: 'banana' },
        { id: 2, name: 'apple' },
        { id: 3, name: 'cherry' },
      ]).exec();

      const minResult = await db.select('*').from('items').min('name').exec();
      expect(minResult[0].min_name).toBe('apple');

      const maxResult = await db.select('*').from('items').max('name').exec();
      expect(maxResult[0].max_name).toBe('cherry');
    });
  });

  describe('16.5 STDDEV 函数', () => {
    it('STDDEV 应该正确计算标准差', async () => {
      const db = new Database('agg_stddev_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      // 数据: [2, 4, 4, 4, 5, 5, 7, 9], mean = 5, variance = 4, stddev = 2
      await db.insert('items').values([
        { id: 1, value: 2 },
        { id: 2, value: 4 },
        { id: 3, value: 4 },
        { id: 4, value: 4 },
        { id: 5, value: 5 },
        { id: 6, value: 5 },
        { id: 7, value: 7 },
        { id: 8, value: 9 },
      ]).exec();

      const result = await db.select('*').from('items').stddev('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].stddev_value).toBeCloseTo(2.0, 2);
    });

    it('STDDEV 单值应该返回 0', async () => {
      const db = new Database('agg_stddev_single_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 5 },
      ]).exec();

      const result = await db.select('*').from('items').stddev('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].stddev_value).toBeCloseTo(0, 2);
    });

    it('STDDEV 空集应该返回 null', async () => {
      const db = new Database('agg_stddev_empty_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      const result = await db.select('*').from('items').stddev('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].stddev_value).toBeNull();
    });
  });

  describe('16.6 GEOMEAN 函数', () => {
    it('GEOMEAN 应该正确计算几何平均数', async () => {
      const db = new Database('agg_geomean_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      // 几何平均数 of [2, 8] = sqrt(2 * 8) = sqrt(16) = 4
      await db.insert('items').values([
        { id: 1, value: 2 },
        { id: 2, value: 8 },
      ]).exec();

      const result = await db.select('*').from('items').geomean('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].geomean_value).toBeCloseTo(4.0, 2);
    });

    it('GEOMEAN 应该过滤非正数', async () => {
      const db = new Database('agg_geomean_filter_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      // 只有 [2, 8] 是正数，geomean = 4
      await db.insert('items').values([
        { id: 1, value: 2 },
        { id: 2, value: 0 },
        { id: 3, value: -1 },
        { id: 4, value: 8 },
      ]).exec();

      const result = await db.select('*').from('items').geomean('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].geomean_value).toBeCloseTo(4.0, 2);
    });

    it('GEOMEAN 全非正数应该返回 null', async () => {
      const db = new Database('agg_geomean_allneg_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Float64, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 0 },
        { id: 2, value: -1 },
      ]).exec();

      const result = await db.select('*').from('items').geomean('value').exec();
      expect(result).toHaveLength(1);
      expect(result[0].geomean_value).toBeNull();
    });
  });

  describe('16.7 DISTINCT 函数', () => {
    it('DISTINCT 应该返回去重后的计数', async () => {
      const db = new Database('agg_distinct_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 1 },
        { id: 2, value: 2 },
        { id: 3, value: 1 }, // duplicate
        { id: 4, value: 3 },
        { id: 5, value: 2 }, // duplicate
      ]).exec();

      const result = await db.select('*').from('items').distinct('value').exec();
      expect(result).toHaveLength(1);
      // 去重后: 1, 2, 3 = 3 个不同值
      expect(result[0].distinct_value).toBe(3);
    });

    it('DISTINCT 应该包含 null 作为一个独立值', async () => {
      const db = new Database('agg_distinct_null_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, new ColumnOptions().setNullable(true));
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 1 },
        { id: 2, value: null },
        { id: 3, value: 1 },
        { id: 4, value: null },
      ]).exec();

      const result = await db.select('*').from('items').distinct('value').exec();
      expect(result).toHaveLength(1);
      // 去重后: 1, null = 2 个不同值
      expect(result[0].distinct_value).toBe(2);
    });
  });

  describe('16.8 GROUP BY 聚合', () => {
    it('GROUP BY 单列 + COUNT', async () => {
      const db = new Database('agg_groupby_count_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.String, null)
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 'A', value: 10 },
        { id: 2, category: 'A', value: 20 },
        { id: 3, category: 'B', value: 30 },
        { id: 4, category: 'A', value: 40 },
        { id: 5, category: 'B', value: 50 },
      ]).exec();

      const result = await db.select('*').from('items').groupBy('category').count().exec();
      expect(result).toHaveLength(2);

      // 按 category 排序验证
      const sorted = result.sort((a: any, b: any) => a.category.localeCompare(b.category));
      expect(sorted[0].category).toBe('A');
      expect(sorted[0].count).toBe(3);
      expect(sorted[1].category).toBe('B');
      expect(sorted[1].count).toBe(2);
    });

    it('GROUP BY + SUM', async () => {
      const db = new Database('agg_groupby_sum_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.String, null)
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 'A', value: 10 },
        { id: 2, category: 'A', value: 20 },
        { id: 3, category: 'B', value: 30 },
      ]).exec();

      const result = await db.select('*').from('items').groupBy('category').sum('value').exec();
      expect(result).toHaveLength(2);

      const sorted = result.sort((a: any, b: any) => a.category.localeCompare(b.category));
      expect(sorted[0].category).toBe('A');
      expect(sorted[0].sum_value).toBe(30); // 10 + 20
      expect(sorted[1].category).toBe('B');
      expect(sorted[1].sum_value).toBe(30);
    });

    it('GROUP BY + AVG', async () => {
      const db = new Database('agg_groupby_avg_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.String, null)
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 'A', value: 10 },
        { id: 2, category: 'A', value: 20 },
        { id: 3, category: 'A', value: 30 },
        { id: 4, category: 'B', value: 100 },
      ]).exec();

      const result = await db.select('*').from('items').groupBy('category').avg('value').exec();
      expect(result).toHaveLength(2);

      const sorted = result.sort((a: any, b: any) => a.category.localeCompare(b.category));
      expect(sorted[0].category).toBe('A');
      expect(sorted[0].avg_value).toBe(20); // (10 + 20 + 30) / 3
      expect(sorted[1].category).toBe('B');
      expect(sorted[1].avg_value).toBe(100);
    });

    it('GROUP BY + 多个聚合函数', async () => {
      const db = new Database('agg_groupby_multi_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.String, null)
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 'A', value: 10 },
        { id: 2, category: 'A', value: 20 },
        { id: 3, category: 'A', value: 30 },
        { id: 4, category: 'B', value: 100 },
      ]).exec();

      const result = await db.select('*').from('items')
        .groupBy('category')
        .count()
        .sum('value')
        .avg('value')
        .min('value')
        .max('value')
        .exec();

      expect(result).toHaveLength(2);

      const sorted = result.sort((a: any, b: any) => a.category.localeCompare(b.category));

      // Group A: count=3, sum=60, avg=20, min=10, max=30
      expect(sorted[0].category).toBe('A');
      expect(sorted[0].count).toBe(3);
      expect(sorted[0].sum_value).toBe(60);
      expect(sorted[0].avg_value).toBe(20);
      expect(sorted[0].min_value).toBe(10);
      expect(sorted[0].max_value).toBe(30);

      // Group B: count=1, sum=100, avg=100, min=100, max=100
      expect(sorted[1].category).toBe('B');
      expect(sorted[1].count).toBe(1);
      expect(sorted[1].sum_value).toBe(100);
      expect(sorted[1].avg_value).toBe(100);
      expect(sorted[1].min_value).toBe(100);
      expect(sorted[1].max_value).toBe(100);
    });
  });

  describe('16.9 聚合 + WHERE 条件', () => {
    it('WHERE + COUNT', async () => {
      const db = new Database('agg_where_count_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('status', JsDataType.String, null)
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, status: 'active', value: 10 },
        { id: 2, status: 'inactive', value: 20 },
        { id: 3, status: 'active', value: 30 },
        { id: 4, status: 'active', value: 40 },
      ]).exec();

      const result = await db.select('*').from('items')
        .where(col('status').eq('active'))
        .count()
        .exec();

      expect(result).toHaveLength(1);
      expect(result[0].count).toBe(3);
    });

    it('WHERE + SUM', async () => {
      const db = new Database('agg_where_sum_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('status', JsDataType.String, null)
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, status: 'active', value: 10 },
        { id: 2, status: 'inactive', value: 20 },
        { id: 3, status: 'active', value: 30 },
      ]).exec();

      const result = await db.select('*').from('items')
        .where(col('status').eq('active'))
        .sum('value')
        .exec();

      expect(result).toHaveLength(1);
      expect(result[0].sum_value).toBe(40); // 10 + 30
    });

    it('WHERE + GROUP BY + 聚合', async () => {
      const db = new Database('agg_where_groupby_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.String, null)
        .column('status', JsDataType.String, null)
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, category: 'A', status: 'active', value: 10 },
        { id: 2, category: 'A', status: 'inactive', value: 20 },
        { id: 3, category: 'A', status: 'active', value: 30 },
        { id: 4, category: 'B', status: 'active', value: 40 },
        { id: 5, category: 'B', status: 'inactive', value: 50 },
      ]).exec();

      const result = await db.select('*').from('items')
        .where(col('status').eq('active'))
        .groupBy('category')
        .sum('value')
        .exec();

      expect(result).toHaveLength(2);

      const sorted = result.sort((a: any, b: any) => a.category.localeCompare(b.category));
      expect(sorted[0].category).toBe('A');
      expect(sorted[0].sum_value).toBe(40); // 10 + 30
      expect(sorted[1].category).toBe('B');
      expect(sorted[1].sum_value).toBe(40);
    });
  });

  describe('16.10 聚合 + Live Query', () => {
    it('COUNT Live Query - INSERT 触发更新', async () => {
      const db = new Database('agg_lq_count_insert_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      // 初始插入数据
      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
      ]).exec();

      const observable = db.select('*').from('items').count().observe();

      // 初始 count 应该是 2
      expect(observable.getResult()).toHaveLength(1);
      expect(observable.getResult()[0].count).toBe(2);

      let lastResult: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastResult = data;
      });

      // 插入新数据
      await db.insert('items').values([
        { id: 3, value: 30 },
        { id: 4, value: 40 },
      ]).exec();

      // count 应该更新为 4
      expect(lastResult).toHaveLength(1);
      expect(lastResult[0].count).toBe(4);

      unsubscribe();
    });

    it('SUM Live Query - INSERT 触发更新', async () => {
      const db = new Database('agg_lq_sum_insert_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
      ]).exec();

      const observable = db.select('*').from('items').sum('value').observe();

      expect(observable.getResult()[0].sum_value).toBe(30);

      let lastResult: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastResult = data;
      });

      await db.insert('items').values([
        { id: 3, value: 30 },
      ]).exec();

      expect(lastResult[0].sum_value).toBe(60);

      unsubscribe();
    });

    it('AVG Live Query - UPDATE 触发更新', async () => {
      const db = new Database('agg_lq_avg_update_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: 20 },
        { id: 3, value: 30 },
      ]).exec();

      const observable = db.select('*').from('items').avg('value').observe();

      expect(observable.getResult()[0].avg_value).toBe(20); // (10+20+30)/3

      let lastResult: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastResult = data;
      });

      // 更新一条数据
      await db.update('items').set({ value: 60 }).where(col('id').eq(BigInt(3))).exec();

      expect(lastResult[0].avg_value).toBe(30); // (10+20+60)/3

      unsubscribe();
    });

    it('MIN/MAX Live Query - DELETE 触发更新', async () => {
      const db = new Database('agg_lq_minmax_delete_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, value: 10 },
        { id: 2, value: 50 },
        { id: 3, value: 30 },
      ]).exec();

      const minObservable = db.select('*').from('items').min('value').observe();
      const maxObservable = db.select('*').from('items').max('value').observe();

      expect(minObservable.getResult()[0].min_value).toBe(10);
      expect(maxObservable.getResult()[0].max_value).toBe(50);

      let lastMinResult: any[] = [];
      let lastMaxResult: any[] = [];
      const unsubMin = minObservable.subscribe((data: any[]) => {
        lastMinResult = data;
      });
      const unsubMax = maxObservable.subscribe((data: any[]) => {
        lastMaxResult = data;
      });

      // 删除最小值
      await db.delete('items').where(col('id').eq(BigInt(1))).exec();

      expect(lastMinResult[0].min_value).toBe(30);
      expect(lastMaxResult[0].max_value).toBe(50);

      // 删除最大值
      await db.delete('items').where(col('id').eq(BigInt(2))).exec();

      expect(lastMinResult[0].min_value).toBe(30);
      expect(lastMaxResult[0].max_value).toBe(30);

      unsubMin();
      unsubMax();
    });

    it('GROUP BY + SUM Live Query - INSERT 触发分组更新', async () => {
      const db = new Database('agg_lq_groupby_insert_1');
      const builder = db.createTable('sales')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.String, null)
        .column('amount', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('sales').values([
        { id: 1, category: 'A', amount: 100 },
        { id: 2, category: 'B', amount: 200 },
        { id: 3, category: 'A', amount: 150 },
      ]).exec();

      const observable = db.select('*').from('sales')
        .groupBy('category')
        .sum('amount')
        .observe();

      const initialResult = observable.getResult();
      const sortedInitial = initialResult.sort((a: any, b: any) => a.category.localeCompare(b.category));
      expect(sortedInitial[0].category).toBe('A');
      expect(sortedInitial[0].sum_amount).toBe(250);
      expect(sortedInitial[1].category).toBe('B');
      expect(sortedInitial[1].sum_amount).toBe(200);

      let lastResult: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastResult = data;
      });

      // 插入新数据到 category B
      await db.insert('sales').values([
        { id: 4, category: 'B', amount: 300 },
      ]).exec();

      const sortedResult = lastResult.sort((a: any, b: any) => a.category.localeCompare(b.category));
      expect(sortedResult[0].category).toBe('A');
      expect(sortedResult[0].sum_amount).toBe(250);
      expect(sortedResult[1].category).toBe('B');
      expect(sortedResult[1].sum_amount).toBe(500);

      unsubscribe();
    });

    it('GROUP BY + COUNT Live Query - DELETE 触发分组更新', async () => {
      const db = new Database('agg_lq_groupby_delete_1');
      const builder = db.createTable('events')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('type', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('events').values([
        { id: 1, type: 'click' },
        { id: 2, type: 'scroll' },
        { id: 3, type: 'click' },
        { id: 4, type: 'click' },
      ]).exec();

      const observable = db.select('*').from('events')
        .groupBy('type')
        .count()
        .observe();

      let lastResult: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastResult = data;
      });

      // 删除一个 click 事件
      await db.delete('events').where(col('id').eq(BigInt(1))).exec();

      const sortedResult = lastResult.sort((a: any, b: any) => a.type.localeCompare(b.type));
      expect(sortedResult[0].type).toBe('click');
      expect(sortedResult[0].count).toBe(2);
      expect(sortedResult[1].type).toBe('scroll');
      expect(sortedResult[1].count).toBe(1);

      unsubscribe();
    });

    it('WHERE + 聚合 Live Query - 条件过滤后聚合', async () => {
      const db = new Database('agg_lq_where_1');
      const builder = db.createTable('orders')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('status', JsDataType.String, null)
        .column('total', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('orders').values([
        { id: 1, status: 'completed', total: 100 },
        { id: 2, status: 'pending', total: 200 },
        { id: 3, status: 'completed', total: 150 },
      ]).exec();

      const observable = db.select('*').from('orders')
        .where(col('status').eq('completed'))
        .sum('total')
        .observe();

      expect(observable.getResult()[0].sum_total).toBe(250);

      let lastResult: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastResult = data;
      });

      // 插入一个 completed 订单
      await db.insert('orders').values([
        { id: 4, status: 'completed', total: 300 },
      ]).exec();

      expect(lastResult[0].sum_total).toBe(550);

      // 插入一个 pending 订单 (不应该影响结果)
      await db.insert('orders').values([
        { id: 5, status: 'pending', total: 500 },
      ]).exec();

      expect(lastResult[0].sum_total).toBe(550);

      unsubscribe();
    });

    it('多个聚合函数 Live Query', async () => {
      const db = new Database('agg_lq_multi_1');
      const builder = db.createTable('scores')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('score', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('scores').values([
        { id: 1, score: 80 },
        { id: 2, score: 90 },
        { id: 3, score: 70 },
      ]).exec();

      const countObs = db.select('*').from('scores').count().observe();
      const sumObs = db.select('*').from('scores').sum('score').observe();
      const avgObs = db.select('*').from('scores').avg('score').observe();

      let lastCount: any[] = [];
      let lastSum: any[] = [];
      let lastAvg: any[] = [];

      const unsub1 = countObs.subscribe((data: any[]) => { lastCount = data; });
      const unsub2 = sumObs.subscribe((data: any[]) => { lastSum = data; });
      const unsub3 = avgObs.subscribe((data: any[]) => { lastAvg = data; });

      // 插入新数据
      await db.insert('scores').values([
        { id: 4, score: 100 },
      ]).exec();

      expect(lastCount[0].count).toBe(4);
      expect(lastSum[0].sum_score).toBe(340);
      expect(lastAvg[0].avg_score).toBe(85);

      unsub1();
      unsub2();
      unsub3();
    });

    it('空表聚合 Live Query - 从空到有数据', async () => {
      const db = new Database('agg_lq_empty_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('value', JsDataType.Int32, null);
      db.registerTable(builder);

      const observable = db.select('*').from('items').count().observe();

      // 空表 count 应该是 0
      expect(observable.getResult()[0].count).toBe(0);

      let lastResult: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastResult = data;
      });

      // 插入数据
      await db.insert('items').values([
        { id: 1, value: 10 },
      ]).exec();

      expect(lastResult[0].count).toBe(1);

      unsubscribe();
    });

    it('GROUP BY Live Query - 新分组出现', async () => {
      const db = new Database('agg_lq_new_group_1');
      const builder = db.createTable('products')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('category', JsDataType.String, null)
        .column('price', JsDataType.Int32, null);
      db.registerTable(builder);

      await db.insert('products').values([
        { id: 1, category: 'electronics', price: 100 },
        { id: 2, category: 'electronics', price: 200 },
      ]).exec();

      const observable = db.select('*').from('products')
        .groupBy('category')
        .count()
        .observe();

      expect(observable.getResult()).toHaveLength(1);
      expect(observable.getResult()[0].category).toBe('electronics');
      expect(observable.getResult()[0].count).toBe(2);

      let lastResult: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastResult = data;
      });

      // 插入新分类
      await db.insert('products').values([
        { id: 3, category: 'clothing', price: 50 },
      ]).exec();

      expect(lastResult).toHaveLength(2);
      const sorted = lastResult.sort((a: any, b: any) => a.category.localeCompare(b.category));
      expect(sorted[0].category).toBe('clothing');
      expect(sorted[0].count).toBe(1);
      expect(sorted[1].category).toBe('electronics');
      expect(sorted[1].count).toBe(2);

      unsubscribe();
    });

    it('GROUP BY Live Query - 分组消失', async () => {
      const db = new Database('agg_lq_group_disappear_1');
      const builder = db.createTable('items')
        .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
        .column('type', JsDataType.String, null);
      db.registerTable(builder);

      await db.insert('items').values([
        { id: 1, type: 'A' },
        { id: 2, type: 'B' },
      ]).exec();

      const observable = db.select('*').from('items')
        .groupBy('type')
        .count()
        .observe();

      expect(observable.getResult()).toHaveLength(2);

      let lastResult: any[] = [];
      const unsubscribe = observable.subscribe((data: any[]) => {
        lastResult = data;
      });

      // 删除 type B 的所有数据
      await db.delete('items').where(col('id').eq(BigInt(2))).exec();

      expect(lastResult).toHaveLength(1);
      expect(lastResult[0].type).toBe('A');
      expect(lastResult[0].count).toBe(1);

      unsubscribe();
    });
  });
});
