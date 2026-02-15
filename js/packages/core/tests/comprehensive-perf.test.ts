/**
 * Cynos Database - Comprehensive Performance Test Suite
 * =====================================================
 *
 * 测试目标：
 * 本测试套件旨在全面评估 Cynos 数据库的查询性能，特别关注：
 * 1. 索引优化效果 - 对比有索引 vs 无索引的性能差异
 * 2. 结果集大小影响 - 对比小结果集 vs 大结果集的性能差异
 * 3. 各类查询操作 - 点查询、范围查询、过滤器、CRUD 操作
 * 4. Live Query 特性 - 实时查询的性能和优化效果
 *
 * 数据模型：
 * - products 表：10,000 行测试数据
 * - 5 个分类（Electronics, Clothing, Books, Home, Sports），每个分类 2,000 行
 * - price: 1000-100900 范围，步长 100
 * - stock: 0-499 循环
 * - rating: 1.0-5.9 范围
 *
 * 性能瓶颈说明：
 * - 索引查找本身非常快（微秒级）
 * - 主要瓶颈在于 WASM→JS 的数据序列化
 * - 因此，返回行数越多，序列化开销越大
 * - 要准确测量索引优化效果，需要控制返回行数（使用 LIMIT）
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

// =============================================================================
// 测试配置
// =============================================================================

/** 标准测试数据量 */
const DATA_SIZES = [1000, 10000];

/** 大数据量测试（用于 LIMIT 优化对比） */
const LARGE_SIZE = 50000;

// =============================================================================
// 性能记录工具
// =============================================================================

interface PerfResult {
  category: string;
  operation: string;
  dataSize: number;
  duration: number;
  rowsAffected: number;
}

const perfResults: PerfResult[] = [];

/**
 * 记录性能测试结果
 * @param category - 测试类别（如 "Point Query", "Range Query"）
 * @param operation - 具体操作（如 "Primary Key Lookup"）
 * @param dataSize - 数据集大小
 * @param duration - 执行时间（毫秒）
 * @param rowsAffected - 影响/返回的行数
 */
function record(category: string, operation: string, dataSize: number, duration: number, rowsAffected: number) {
  perfResults.push({ category, operation, dataSize, duration, rowsAffected });
  const throughput = rowsAffected / (duration / 1000);
  console.log(`[${category}] ${operation} (${dataSize} rows): ${duration.toFixed(2)}ms, ${rowsAffected} rows (${Math.round(throughput).toLocaleString()} rows/sec)`);
}

// =============================================================================
// 测试数据生成
// =============================================================================

/**
 * 创建测试数据库
 *
 * 表结构：
 * - id: Int64, 主键（自动索引）
 * - name: String, 无索引（用于全表扫描测试）
 * - category: String, 有索引（用于索引查询测试）
 * - price: Int64, 有索引（用于范围查询测试）
 * - stock: Int32, 有索引（用于比较运算符测试）
 * - rating: Float64, 无索引（用于非索引范围查询对比）
 * - description: String, 可空（用于 isNull/isNotNull 测试）
 * - is_active: Bool（用于布尔过滤测试）
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
    .column('is_active', JsDataType.Bool, null)
    .index('idx_category', 'category')  // 索引：用于 eq/IN 查询
    .index('idx_price', 'price')        // 索引：用于范围查询
    .index('idx_stock', 'stock');       // 索引：用于比较运算符
  db.registerTable(productsBuilder);

  return db;
}

/**
 * 生成测试产品数据
 *
 * 数据分布：
 * - category: 5 个分类均匀分布，每个分类占 20%
 *   - Electronics (id % 5 == 0): 20%
 *   - Clothing (id % 5 == 1): 20%
 *   - Books (id % 5 == 2): 20%
 *   - Home (id % 5 == 3): 20%
 *   - Sports (id % 5 == 4): 20%
 *
 * - price: 1000 + (i % 1000) * 100 = 1000 ~ 100900
 *   - price < 5000: 约 4% (40/1000)
 *   - price BETWEEN 5000 AND 10000: 约 5% (50/1000)
 *   - price > 50000: 约 50%
 *
 * - stock: i % 500 = 0 ~ 499
 *   - stock >= 400: 约 20% (100/500)
 *   - stock <= 50: 约 10% (51/500)
 *
 * - rating: 1.0 + (i % 50) / 10 = 1.0 ~ 5.9
 *   - rating BETWEEN 2.0 AND 4.0: 约 42% (21/50)
 *
 * - description: 每 10 行有 1 行为 null (10%)
 *
 * - is_active: 每 3 行有 1 行为 false (33%)
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
 * 生成偏态分布的测试数据
 *
 * 数据分布：
 * - category: 偏态分布
 *   - Electronics: 80% (用于测试低选择性场景)
 *   - Clothing: 5%
 *   - Books: 5%
 *   - Home: 5%
 *   - Sports: 5%
 *
 * 其他字段与 generateProducts 相同
 *
 * 用途：测试优化器在低选择性（高重复率）场景下的行为
 * - 当某个值占 80% 时，索引可能不如全表扫描高效
 * - 验证优化器是否能正确选择执行计划
 */
function generateSkewedProducts(count: number) {
  const categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'];
  return Array.from({ length: count }, (_, i) => {
    // 80% Electronics, 其他各 5%
    let category: string;
    if (i < count * 0.8) {
      category = 'Electronics';
    } else {
      // 剩余 20% 均匀分布到其他 4 个分类
      const remaining = i - Math.floor(count * 0.8);
      const categoryIndex = (remaining % 4) + 1; // 1-4 对应 Clothing, Books, Home, Sports
      category = categories[categoryIndex];
    }

    return {
      id: i + 1,
      name: `Product ${i + 1}`,
      category,
      price: 1000 + (i % 1000) * 100,
      stock: i % 500,
      rating: 1.0 + (i % 50) / 10,
      description: i % 10 === 0 ? null : `Description for product ${i + 1}`,
      is_active: i % 3 !== 0,
    };
  });
}

// =============================================================================
// SECTION 1: 点查询（Point Queries）
// =============================================================================
/**
 * 点查询测试说明：
 *
 * 点查询是指返回单行或少量行的精确匹配查询。
 * 这类查询最能体现索引优化的效果，因为：
 * 1. 索引查找是 O(log n) 复杂度
 * 2. 全表扫描是 O(n) 复杂度
 * 3. 返回行数少，序列化开销可忽略
 *
 * 测试对比：
 * - 1.1 主键查询（自动索引）
 * - 1.2 索引列查询 + LIMIT（体现索引效果）
 * - 1.3 索引列查询（大结果集，体现序列化开销）
 * - 1.4 非索引列查询（全表扫描基准）
 */
describe('1. Point Queries', () => {
  /**
   * 1.1 主键查询
   *
   * 场景：通过主键 id 查找单行
   * 预期：O(1) 或 O(log n) 复杂度，亚毫秒级响应
   * 返回行数：1 行
   *
   * 这是最快的查询类型，因为：
   * - 主键自动建立索引
   * - 只返回 1 行，序列化开销最小
   */
  describe('1.1 Primary Key Lookup', () => {
    for (const size of DATA_SIZES) {
      it(`should lookup by primary key in ${size} rows`, async () => {
        const db = createTestDb(`pk_lookup_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const iterations = 100;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          const targetId = (i % size) + 1;
          await db.select('*').from('products').where(col('id').eq(targetId)).exec();
        }
        const duration = performance.now() - start;

        record('Point Query', 'Primary Key Lookup', size, duration / iterations, 1);
        expect(duration / iterations).toBeLessThan(50);
      });
    }
  });

  /**
   * 1.2 索引列查询 + LIMIT（小结果集）
   *
   * 场景：通过索引列 category 查找，限制返回 10 行
   * 预期：索引查找 + 少量序列化，毫秒级响应
   * 返回行数：10 行
   *
   * 这个测试展示了索引优化的真实效果：
   * - 使用 IndexGet 计划节点直接定位数据
   * - LIMIT 10 限制序列化开销
   * - 与 1.4 非索引查询对比，可以看到索引带来的性能提升
   */
  describe('1.2 Indexed Column Lookup (Small Result)', () => {
    for (const size of DATA_SIZES) {
      it(`should lookup by indexed column with LIMIT in ${size} rows`, async () => {
        const db = createTestDb(`idx_lookup_limit_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'];
        const iterations = 100;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          const result = await db.select('*').from('products')
            .where(col('category').eq(categories[i % 5]))
            .limit(10)
            .exec();
          // 验证 LIMIT 生效
          expect(result.length).toBeLessThanOrEqual(10);
        }
        const duration = performance.now() - start;

        record('Point Query', 'Indexed Lookup + LIMIT 10', size, duration / iterations, 10);
      });
    }
  });

  /**
   * 1.3 索引列查询（大结果集）
   *
   * 场景：通过索引列 category 查找，不限制返回行数
   * 预期：索引查找快，但序列化开销大
   * 返回行数：约 20% 数据（size / 5 行）
   *
   * 这个测试展示了序列化瓶颈：
   * - 索引查找本身很快
   * - 但返回 2000 行（10000 * 20%）需要大量序列化
   * - 与 1.2 对比，可以看到序列化开销的影响
   */
  describe('1.3 Indexed Column Lookup (Large Result)', () => {
    for (const size of DATA_SIZES) {
      it(`should lookup by indexed column without LIMIT in ${size} rows`, async () => {
        const db = createTestDb(`idx_lookup_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'];
        const iterations = 20; // 减少迭代次数，因为每次返回大量数据
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('products')
            .where(col('category').eq(categories[i % 5]))
            .exec();
        }
        const duration = performance.now() - start;

        const result = await db.select('*').from('products')
          .where(col('category').eq('Electronics'))
          .exec();
        // 验证返回约 20% 数据（每个分类占 20%）
        expect(result.length).toBe(size / 5);
        record('Point Query', 'Indexed Lookup (No LIMIT)', size, duration / iterations, result.length);
      });
    }
  });

  /**
   * 1.4 非索引列查询（全表扫描）
   *
   * 场景：通过非索引列 name 查找
   * 预期：全表扫描，O(n) 复杂度
   * 返回行数：1 行
   *
   * 这是性能基准测试：
   * - 必须扫描所有行才能找到匹配项
   * - 与 1.1/1.2 对比，展示索引的重要性
   */
  describe('1.4 Non-Indexed Column Lookup (Full Scan)', () => {
    for (const size of DATA_SIZES) {
      it(`should lookup by non-indexed column in ${size} rows`, async () => {
        const db = createTestDb(`scan_lookup_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const iterations = 20;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('products')
            .where(col('name').eq(`Product ${(i % size) + 1}`))
            .exec();
        }
        const duration = performance.now() - start;

        record('Point Query', 'Non-Indexed Lookup (Full Scan)', size, duration / iterations, 1);
      });
    }
  });
});

// =============================================================================
// SECTION 2: 范围查询（Range Queries）
// =============================================================================
/**
 * 范围查询测试说明：
 *
 * 范围查询使用比较运算符（>, <, >=, <=, BETWEEN）查找数据。
 * 索引对范围查询同样有效，因为 B-Tree 索引支持范围扫描。
 *
 * 测试对比：
 * - 2.1 BETWEEN 索引列（小结果集）
 * - 2.2 BETWEEN 索引列（大结果集）
 * - 2.3 BETWEEN 非索引列（全表扫描对比）
 * - 2.4 比较运算符（gt, lt, gte, lte）
 */
describe('2. Range Queries', () => {
  /**
   * 2.1 BETWEEN 索引列（小结果集）
   *
   * 场景：price BETWEEN 5000 AND 10000，约 5% 数据
   * 预期：索引范围扫描，返回约 500 行（10000 * 5%）
   * 返回行数：约 5% 数据
   *
   * 注意：price 分布为 1000 ~ 100900，步长 100
   * BETWEEN 5000 AND 10000 匹配 price = 5000, 5100, ..., 10000
   * 共 51 个值，每个值在 10000 行中出现 10 次 = 510 行
   */
  describe('2.1 BETWEEN on Indexed Column (Small Result)', () => {
    for (const size of DATA_SIZES) {
      it(`should query BETWEEN on indexed column in ${size} rows`, async () => {
        const db = createTestDb(`between_idx_small_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const iterations = 50;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('products')
            .where(col('price').between(5000, 10000))
            .limit(10)
            .exec();
        }
        const duration = performance.now() - start;

        record('Range Query', 'BETWEEN (Indexed) + LIMIT 10', size, duration / iterations, 10);
      });
    }
  });

  /**
   * 2.2 BETWEEN 索引列（大结果集）
   *
   * 场景：price BETWEEN 5000 AND 50000，约 45% 数据
   * 预期：索引范围扫描，但返回大量数据
   * 返回行数：约 45% 数据
   *
   * 这个测试展示了即使使用索引，大结果集仍然有序列化瓶颈
   */
  describe('2.2 BETWEEN on Indexed Column (Large Result)', () => {
    for (const size of DATA_SIZES) {
      it(`should query BETWEEN on indexed column without LIMIT in ${size} rows`, async () => {
        const db = createTestDb(`between_idx_large_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const iterations = 20;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('products')
            .where(col('price').between(5000, 50000))
            .exec();
        }
        const duration = performance.now() - start;

        const result = await db.select('*').from('products')
          .where(col('price').between(5000, 50000))
          .exec();
        record('Range Query', 'BETWEEN (Indexed) No LIMIT', size, duration / iterations, result.length);
      });
    }
  });

  /**
   * 2.3 BETWEEN 非索引列（全表扫描对比）
   *
   * 场景：rating BETWEEN 2.0 AND 4.0，约 42% 数据
   * 预期：全表扫描，O(n) 复杂度
   * 返回行数：约 42% 数据
   *
   * 与 2.1/2.2 对比，展示索引对范围查询的优化效果
   */
  describe('2.3 BETWEEN on Non-Indexed Column (Full Scan)', () => {
    for (const size of DATA_SIZES) {
      it(`should query BETWEEN on non-indexed column in ${size} rows`, async () => {
        const db = createTestDb(`between_scan_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const iterations = 20;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('products')
            .where(col('rating').between(2.0, 4.0))
            .exec();
        }
        const duration = performance.now() - start;

        const result = await db.select('*').from('products')
          .where(col('rating').between(2.0, 4.0))
          .exec();
        record('Range Query', 'BETWEEN (Non-Indexed)', size, duration / iterations, result.length);
      });
    }
  });

  /**
   * 2.4 比较运算符（gt, lt, gte, lte）
   *
   * 场景：测试各种比较运算符在索引列上的性能
   * 预期：索引范围扫描
   *
   * 测试用例：
   * - price > 90000: 约 10% 数据（高选择度）
   * - price < 5000: 约 4% 数据（高选择度）
   * - stock >= 400: 约 20% 数据
   * - stock <= 50: 约 10% 数据
   */
  describe('2.4 Comparison Operators (gt, lt, gte, lte)', () => {
    for (const size of DATA_SIZES) {
      it(`should query with comparison operators in ${size} rows`, async () => {
        const db = createTestDb(`compare_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        // Test gt: price > 90000 (约 10% 数据)
        let start = performance.now();
        const gtResult = await db.select('*').from('products')
          .where(col('price').gt(90000))
          .exec();
        record('Range Query', 'gt (Indexed, ~10%)', size, performance.now() - start, gtResult.length);

        // Test lt: price < 5000 (约 4% 数据)
        start = performance.now();
        const ltResult = await db.select('*').from('products')
          .where(col('price').lt(5000))
          .exec();
        record('Range Query', 'lt (Indexed, ~4%)', size, performance.now() - start, ltResult.length);

        // Test gte: stock >= 400 (约 20% 数据)
        start = performance.now();
        const gteResult = await db.select('*').from('products')
          .where(col('stock').gte(400))
          .exec();
        record('Range Query', 'gte (Indexed, ~20%)', size, performance.now() - start, gteResult.length);

        // Test lte: stock <= 50 (约 10% 数据)
        start = performance.now();
        const lteResult = await db.select('*').from('products')
          .where(col('stock').lte(50))
          .exec();
        record('Range Query', 'lte (Indexed, ~10%)', size, performance.now() - start, lteResult.length);
      });
    }
  });
});

// =============================================================================
// SECTION 3: 过滤器类型（Filter Types）
// =============================================================================
/**
 * 过滤器类型测试说明：
 *
 * 测试各种过滤器操作符的性能：
 * - LIKE: 模式匹配（前缀、后缀、包含）
 * - IN: 多值匹配（使用 IndexInGet 优化）
 * - isNull/isNotNull: 空值检查
 * - 复合条件: AND/OR 组合
 *
 * 重点关注 IN 查询的索引优化效果
 */
describe('3. Filter Types', () => {
  /**
   * 3.1 LIKE 模式匹配
   *
   * 场景：测试不同 LIKE 模式的性能
   * 预期：全表扫描（LIKE 通常不使用索引）
   *
   * 测试用例：
   * - 前缀匹配 'Product 1%': 匹配 Product 1, 10-19, 100-199, 1000-1999...
   * - 后缀匹配 '%00': 匹配 100, 200, ..., 10000
   * - 包含匹配 '%10%': 匹配包含 "10" 的所有产品名
   */
  describe('3.1 LIKE Pattern Matching', () => {
    for (const size of DATA_SIZES) {
      it(`should query with LIKE in ${size} rows`, async () => {
        const db = createTestDb(`like_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        // 前缀匹配: 'Product 1%'
        // 在 10000 行中匹配: 1, 10-19, 100-199, 1000-1999, 10000 = 1+10+100+1000+1 = 1112 行
        let start = performance.now();
        const prefixResult = await db.select('*').from('products')
          .where(col('name').like('Product 1%'))
          .exec();
        record('Filter', 'LIKE prefix (Product 1%)', size, performance.now() - start, prefixResult.length);
        // 验证前缀匹配结果在合理范围内
        expect(prefixResult.length).toBeGreaterThan(0);
        expect(prefixResult.every((r: any) => r.name.startsWith('Product 1'))).toBe(true);

        // 后缀匹配: '%00'
        // 匹配 100, 200, ..., 10000 = size/100 行
        start = performance.now();
        const suffixResult = await db.select('*').from('products')
          .where(col('name').like('%00'))
          .exec();
        record('Filter', 'LIKE suffix (%00)', size, performance.now() - start, suffixResult.length);
        // 验证后缀匹配结果
        expect(suffixResult.length).toBe(size / 100);
        expect(suffixResult.every((r: any) => r.name.endsWith('00'))).toBe(true);

        // 包含匹配: '%10%'
        start = performance.now();
        const containsResult = await db.select('*').from('products')
          .where(col('name').like('%10%'))
          .exec();
        record('Filter', 'LIKE contains (%10%)', size, performance.now() - start, containsResult.length);
        // 验证包含匹配结果
        expect(containsResult.every((r: any) => r.name.includes('10'))).toBe(true);
      });
    }
  });

  /**
   * 3.2 IN 操作符（索引优化重点测试）
   *
   * IN 查询使用 IndexInGet 优化：
   * - 对于索引列，优化器会生成 IndexInGet 计划节点
   * - IndexInGet 对每个值进行索引查找，然后合并结果
   * - 比全表扫描快得多，特别是在大数据集上
   *
   * 测试对比：
   * - 3.2.1 IN + LIMIT（小结果集，体现索引优化效果）
   * - 3.2.2 IN 无 LIMIT（大结果集，体现序列化瓶颈）
   * - 3.2.3 单值 IN（与 eq 对比基准）
   */
  describe('3.2 IN Operator (Index Optimized)', () => {
    /**
     * 3.2.1 IN + LIMIT（小结果集）
     *
     * 场景：IN(['Electronics', 'Books', 'Sports']) + LIMIT 10
     * 预期：IndexInGet 快速定位，只序列化 10 行
     * 返回行数：10 行
     *
     * 这是展示 IN 索引优化效果的最佳测试：
     * - 使用 IndexInGet 而非全表扫描
     * - LIMIT 10 消除序列化瓶颈
     */
    describe('3.2.1 IN with LIMIT (Small Result)', () => {
      for (const size of DATA_SIZES) {
        it(`should query IN with LIMIT in ${size} rows`, async () => {
          const db = createTestDb(`in_limit_${size}`);
          await db.insert('products').values(generateProducts(size)).exec();

          const iterations = 100;
          const start = performance.now();
          for (let i = 0; i < iterations; i++) {
            const result = await db.select('*').from('products')
              .where(col('category').in(['Electronics', 'Books', 'Sports']))
              .limit(10)
              .exec();
            // 验证 LIMIT 生效
            expect(result.length).toBeLessThanOrEqual(10);
          }
          const duration = performance.now() - start;

          record('Filter', 'IN (3 values) + LIMIT 10', size, duration / iterations, 10);
        });
      }
    });

    /**
     * 3.2.2 IN 无 LIMIT（大结果集）
     *
     * 场景：IN(['Electronics', 'Books', 'Sports']) 无 LIMIT
     * 预期：IndexInGet 快速定位，但序列化 60% 数据
     * 返回行数：60% 数据（3/5 分类）
     *
     * 这个测试展示了序列化瓶颈：
     * - 索引查找本身很快
     * - 但返回 6000 行（10000 * 60%）需要大量序列化
     * - 与 3.2.1 对比，可以看到序列化开销的影响
     */
    describe('3.2.2 IN without LIMIT (Large Result)', () => {
      for (const size of DATA_SIZES) {
        it(`should query IN without LIMIT in ${size} rows`, async () => {
          const db = createTestDb(`in_no_limit_${size}`);
          await db.insert('products').values(generateProducts(size)).exec();

          const iterations = 20;
          const start = performance.now();
          for (let i = 0; i < iterations; i++) {
            await db.select('*').from('products')
              .where(col('category').in(['Electronics', 'Books', 'Sports']))
              .exec();
          }
          const duration = performance.now() - start;

          const result = await db.select('*').from('products')
            .where(col('category').in(['Electronics', 'Books', 'Sports']))
            .exec();
          record('Filter', 'IN (3 values) No LIMIT', size, duration / iterations, result.length);

          // 验证返回约 60% 数据（3/5 分类）
          expect(result.length).toBe(size * 3 / 5);
          // 验证返回的数据正确
          expect(result.every((r: any) => ['Electronics', 'Books', 'Sports'].includes(r.category))).toBe(true);
        });
      }
    });

    /**
     * 3.2.3 单值 IN + LIMIT（与 eq 对比基准）
     *
     * 场景：IN(['Electronics']) + LIMIT 10
     * 预期：与 eq('Electronics') 性能相近
     * 返回行数：10 行
     *
     * 这个测试用于验证 IN 单值情况的优化
     */
    describe('3.2.3 Single Value IN (Baseline)', () => {
      for (const size of DATA_SIZES) {
        it(`should query single value IN in ${size} rows`, async () => {
          const db = createTestDb(`in_single_${size}`);
          await db.insert('products').values(generateProducts(size)).exec();

          const iterations = 100;
          const start = performance.now();
          for (let i = 0; i < iterations; i++) {
            const result = await db.select('*').from('products')
              .where(col('category').in(['Electronics']))
              .limit(10)
              .exec();
            // 验证 LIMIT 生效
            expect(result.length).toBeLessThanOrEqual(10);
          }
          const duration = performance.now() - start;

          record('Filter', 'IN (1 value) + LIMIT 10', size, duration / iterations, 10);
        });
      }
    });

    /**
     * 3.2.4 查询计划验证
     *
     * 场景：验证 IN 查询确实使用了 IndexInGet 优化
     * 预期：explain() 返回的物理计划包含 IndexInGet
     *
     * 这个测试确保优化器正确选择了索引查找而非全表扫描
     */
    describe('3.2.4 Query Plan Verification', () => {
      it('should use IndexInGet for IN query on indexed column', async () => {
        const db = createTestDb('in_plan_verify');
        await db.insert('products').values(generateProducts(1000)).exec();

        // 获取查询计划（explain() 返回 { logical, optimized, physical }）
        const plan = db.select('*').from('products')
          .where(col('category').in(['Electronics', 'Books']))
          .explain();

        // 验证物理计划包含 IndexInGet（索引优化）
        expect(plan.physical).toContain('IndexInGet');
        console.log('[Query Plan] IN query uses IndexInGet optimization ✓');
      });

      it('should use IndexGet for eq query on indexed column', async () => {
        const db = createTestDb('eq_plan_verify');
        await db.insert('products').values(generateProducts(1000)).exec();

        const plan = db.select('*').from('products')
          .where(col('category').eq('Electronics'))
          .explain();

        // 验证物理计划包含 IndexGet
        expect(plan.physical).toContain('IndexGet');
        console.log('[Query Plan] eq query uses IndexGet optimization ✓');
      });

      it('should use TableScan for non-indexed column', async () => {
        const db = createTestDb('scan_plan_verify');
        await db.insert('products').values(generateProducts(1000)).exec();

        const plan = db.select('*').from('products')
          .where(col('name').eq('Product 1'))
          .explain();

        // 验证物理计划包含 TableScan（全表扫描）
        expect(plan.physical).toContain('TableScan');
        console.log('[Query Plan] Non-indexed query uses TableScan ✓');
      });
    });

    /**
     * 3.2.5 偏态分布测试（低选择性场景）
     *
     * 场景：某个分类占 80% 数据时的查询性能
     * 预期：
     * - 查询 Electronics（80%）：返回大量数据，序列化是瓶颈
     * - 查询 Clothing（5%）：返回少量数据，索引优化明显
     *
     * 这个测试验证了在低选择性场景下的性能表现
     */
    describe('3.2.5 Skewed Distribution (Low Selectivity)', () => {
      it('should handle skewed data distribution', async () => {
        const db = createTestDb('skewed_dist');
        const size = 10000;
        await db.insert('products').values(generateSkewedProducts(size)).exec();

        // 查询高频分类（80% 数据）- 低选择性
        let start = performance.now();
        const highFreqResult = await db.select('*').from('products')
          .where(col('category').eq('Electronics'))
          .exec();
        const highFreqDuration = performance.now() - start;

        // 验证返回约 80% 数据
        expect(highFreqResult.length).toBe(Math.floor(size * 0.8));
        record('Filter', 'Skewed: High Freq (80%)', size, highFreqDuration, highFreqResult.length);

        // 查询低频分类（5% 数据）- 高选择性
        start = performance.now();
        const lowFreqResult = await db.select('*').from('products')
          .where(col('category').eq('Clothing'))
          .exec();
        const lowFreqDuration = performance.now() - start;

        // 验证返回约 5% 数据
        expect(lowFreqResult.length).toBe(Math.floor(size * 0.2 / 4)); // 20% / 4 categories = 5%
        record('Filter', 'Skewed: Low Freq (5%)', size, lowFreqDuration, lowFreqResult.length);

        // 低频查询应该比高频查询快（因为返回数据少）
        console.log(`[Skewed Distribution] High freq (80%): ${highFreqDuration.toFixed(2)}ms, Low freq (5%): ${lowFreqDuration.toFixed(2)}ms`);
        expect(lowFreqDuration).toBeLessThan(highFreqDuration);
      });

      it('should handle IN query on skewed data with LIMIT', async () => {
        const db = createTestDb('skewed_in_limit');
        const size = 10000;
        await db.insert('products').values(generateSkewedProducts(size)).exec();

        // IN 查询 + LIMIT：即使数据偏态，LIMIT 也能保证快速响应
        const iterations = 50;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          const result = await db.select('*').from('products')
            .where(col('category').in(['Electronics', 'Clothing']))
            .limit(10)
            .exec();
          // 验证 LIMIT 生效
          expect(result.length).toBeLessThanOrEqual(10);
        }
        const duration = performance.now() - start;

        record('Filter', 'Skewed: IN + LIMIT 10', size, duration / iterations, 10);
      });
    });
  });

  /**
   * 3.3 isNull / isNotNull
   *
   * 场景：测试空值检查的性能
   * 预期：全表扫描（空值检查通常不使用索引）
   *
   * 数据分布：
   * - description 为 null: 10% (每 10 行 1 行)
   * - description 不为 null: 90%
   */
  describe('3.3 isNull / isNotNull', () => {
    for (const size of DATA_SIZES) {
      it(`should query with isNull/isNotNull in ${size} rows`, async () => {
        const db = createTestDb(`null_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        // isNull: 约 10% 数据
        let start = performance.now();
        const nullResult = await db.select('*').from('products')
          .where(col('description').isNull())
          .exec();
        record('Filter', 'isNull (~10%)', size, performance.now() - start, nullResult.length);

        // isNotNull: 约 90% 数据
        start = performance.now();
        const notNullResult = await db.select('*').from('products')
          .where(col('description').isNotNull())
          .exec();
        record('Filter', 'isNotNull (~90%)', size, performance.now() - start, notNullResult.length);

        // 验证: null + notNull = total
        expect(nullResult.length + notNullResult.length).toBe(size);
      });
    }
  });

  /**
   * 3.4 复合条件 AND/OR
   *
   * 场景：测试复杂条件组合的性能
   * 预期：根据条件复杂度和索引使用情况而定
   *
   * 测试条件：
   * (category = 'Electronics' OR category = 'Books')
   * AND price > 5000
   * AND stock < 200
   */
  describe('3.4 Complex AND/OR Combinations', () => {
    for (const size of DATA_SIZES) {
      it(`should query with complex AND/OR in ${size} rows`, async () => {
        const db = createTestDb(`complex_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const start = performance.now();
        const result = await db.select('*').from('products')
          .where(
            col('category').eq('Electronics')
              .or(col('category').eq('Books'))
              .and(col('price').gt(5000))
              .and(col('stock').lt(200))
          )
          .exec();
        record('Filter', 'Complex AND/OR', size, performance.now() - start, result.length);
      });
    }
  });
});

// =============================================================================
// SECTION 4: 插入性能（Insert Performance）
// =============================================================================
/**
 * 插入性能测试说明：
 *
 * 测试不同场景下的插入性能：
 * - 单行插入
 * - 批量插入
 * - 有 Live Query 监听时的插入
 *
 * 插入操作需要：
 * 1. 数据验证
 * 2. 索引更新
 * 3. 触发 Live Query 更新（如果有）
 */
describe('4. Insert Performance', () => {
  /**
   * 4.1 单行插入
   *
   * 场景：在已有数据的表中插入单行
   * 预期：亚毫秒级响应
   *
   * 测试要点：
   * - 索引更新开销
   * - 数据验证开销
   */
  describe('4.1 Single Insert', () => {
    for (const size of DATA_SIZES) {
      it(`should measure single insert in ${size} row table`, async () => {
        const db = createTestDb(`single_insert_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const iterations = 100;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.insert('products').values([{
            id: size + i + 1,
            name: `New Product ${i}`,
            category: 'Electronics',
            price: 9999,
            stock: 100,
            rating: 4.5,
            description: 'New product',
            is_active: true,
          }]).exec();
        }
        const duration = performance.now() - start;

        record('Insert', 'Single Insert', size, duration / iterations, 1);
      });
    }
  });

  /**
   * 4.2 批量插入
   *
   * 场景：一次性插入多行数据
   * 预期：比单行插入更高效（摊销索引更新开销）
   *
   * 测试不同批量大小：100, 1000, 10000 行
   */
  describe('4.2 Bulk Insert', () => {
    for (const size of [100, 1000, 10000]) {
      it(`should bulk insert ${size} rows`, async () => {
        const db = createTestDb(`bulk_insert_${size}`);
        const products = generateProducts(size);

        const start = performance.now();
        await db.insert('products').values(products).exec();
        const duration = performance.now() - start;

        record('Insert', 'Bulk Insert', size, duration, size);
        expect(db.totalRowCount()).toBe(size);
      });
    }
  });

  /**
   * 4.3 有 Live Query 监听时的插入（无 LIMIT）
   *
   * 场景：有 Live Query 监听时插入数据
   * 预期：额外的 Live Query 更新开销
   *
   * 这个测试展示了 Live Query 的开销：
   * - 每次插入都会触发 Live Query 重新执行
   * - 无 LIMIT 时，每次都要序列化大量数据
   */
  describe('4.3 Insert with Live Query (No LIMIT)', () => {
    it('should measure insert overhead with live query', async () => {
      const db = createTestDb('insert_live_no_limit');
      await db.insert('products').values(generateProducts(1000)).exec();

      // 创建 Live Query（无 LIMIT，返回约 200 行）
      const stream = db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .changes();
      let updateCount = 0;
      const unsub = stream.subscribe(() => { updateCount++; });

      const iterations = 50;
      const start = performance.now();
      for (let i = 0; i < iterations; i++) {
        await db.insert('products').values([{
          id: 10000 + i,
          name: `Live Insert ${i}`,
          category: 'Electronics',
          price: 9999,
          stock: 100,
          rating: 4.5,
          description: 'Test',
          is_active: true,
        }]).exec();
      }
      const duration = performance.now() - start;

      record('Insert', 'With Live Query (No LIMIT)', 1000, duration / iterations, 1);
      unsub();
    });
  });

  /**
   * 4.4 有 Live Query 监听时的插入（有 LIMIT）
   *
   * 场景：有 LIMIT 的 Live Query 监听时插入数据
   * 预期：比无 LIMIT 更快（序列化开销小）
   *
   * 与 4.3 对比，展示 LIMIT 对 Live Query 性能的影响
   */
  describe('4.4 Insert with Live Query (LIMIT 10)', () => {
    it('should measure insert overhead with limited live query', async () => {
      const db = createTestDb('insert_live_limit');
      await db.insert('products').values(generateProducts(1000)).exec();

      // 创建 Live Query（有 LIMIT，只返回 10 行）
      const stream = db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .limit(10)
        .changes();
      let updateCount = 0;
      const unsub = stream.subscribe(() => { updateCount++; });

      const iterations = 50;
      const start = performance.now();
      for (let i = 0; i < iterations; i++) {
        await db.insert('products').values([{
          id: 10000 + i,
          name: `Live Insert ${i}`,
          category: 'Electronics',
          price: 9999,
          stock: 100,
          rating: 4.5,
          description: 'Test',
          is_active: true,
        }]).exec();
      }
      const duration = performance.now() - start;

      record('Insert', 'With Live Query (LIMIT 10)', 1000, duration / iterations, 1);
      unsub();
    });
  });
});

// =============================================================================
// SECTION 5: 更新性能（Update Performance）
// =============================================================================
/**
 * 更新性能测试说明：
 *
 * 测试不同场景下的更新性能：
 * - 单行更新（通过主键定位）
 * - 批量更新（通过条件匹配）
 * - 有 Live Query 监听时的更新
 *
 * 更新操作需要：
 * 1. 定位目标行（使用索引或全表扫描）
 * 2. 修改数据
 * 3. 更新索引（如果修改了索引列）
 * 4. 触发 Live Query 更新（如果有）
 */
describe('5. Update Performance', () => {
  /**
   * 5.1 单行更新
   *
   * 场景：通过主键定位并更新单行
   * 预期：亚毫秒级响应
   *
   * 注意：更新非索引列（rating）以避免索引更新开销
   */
  describe('5.1 Single Row Update', () => {
    for (const size of DATA_SIZES) {
      it(`should update single row in ${size} row table`, async () => {
        const db = createTestDb(`single_update_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const iterations = 50;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          // 更新 rating（非索引列）以避免唯一约束问题
          await db.update('products')
            .set('rating', 5.0 + i * 0.01)
            .where(col('id').eq((i % size) + 1))
            .exec();
        }
        const duration = performance.now() - start;

        record('Update', 'Single Row Update', size, duration / iterations, 1);
      });
    }
  });

  /**
   * 5.2 批量更新
   *
   * 场景：通过条件匹配更新多行
   * 预期：更新约 20% 数据（一个分类）
   *
   * 注意：更新非索引列（is_active）以避免索引更新开销
   */
  describe('5.2 Bulk Update', () => {
    for (const size of DATA_SIZES) {
      it(`should bulk update in ${size} row table`, async () => {
        const db = createTestDb(`bulk_update_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const start = performance.now();
        // 更新 is_active（非索引列）以避免唯一约束问题
        await db.update('products')
          .set('is_active', false)
          .where(col('category').eq('Electronics'))
          .exec();
        const duration = performance.now() - start;

        const updated = await db.select('*').from('products')
          .where(col('category').eq('Electronics'))
          .exec();
        record('Update', 'Bulk Update (~20%)', size, duration, updated.length);
      });
    }
  });

  /**
   * 5.3 有 Live Query 监听时的更新
   *
   * 场景：有 Live Query 监听时更新数据
   * 预期：触发 Live Query 更新通知
   */
  describe('5.3 Update with Live Query', () => {
    it('should measure update propagation to live query', async () => {
      const db = createTestDb('update_live');
      await db.insert('products').values(generateProducts(1000)).exec();

      const stream = db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .changes();
      let updateCount = 0;
      const unsub = stream.subscribe(() => { updateCount++; });

      const start = performance.now();
      await db.update('products')
        .set('rating', 5.0)
        .where(col('id').eq(1))
        .exec();
      const duration = performance.now() - start;

      record('Update', 'With Live Query Propagation', 1000, duration, 1);
      expect(updateCount).toBe(2); // Initial + update
      unsub();
    });
  });
});

// =============================================================================
// SECTION 6: 删除性能（Delete Performance）
// =============================================================================
/**
 * 删除性能测试说明：
 *
 * 测试不同场景下的删除性能：
 * - 单行删除（通过主键定位）
 * - 批量删除（通过条件匹配）
 * - 有 Live Query 监听时的删除
 *
 * 删除操作需要：
 * 1. 定位目标行
 * 2. 删除数据
 * 3. 更新所有相关索引
 * 4. 触发 Live Query 更新（如果有）
 */
describe('6. Delete Performance', () => {
  /**
   * 6.1 单行删除
   *
   * 场景：通过主键定位并删除单行
   * 预期：亚毫秒级响应
   */
  describe('6.1 Single Row Delete', () => {
    for (const size of DATA_SIZES) {
      it(`should delete single row in ${size} row table`, async () => {
        const db = createTestDb(`single_delete_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const iterations = 50;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.delete('products')
            .where(col('id').eq(i + 1))
            .exec();
        }
        const duration = performance.now() - start;

        record('Delete', 'Single Row Delete', size, duration / iterations, 1);
      });
    }
  });

  /**
   * 6.2 批量删除
   *
   * 场景：通过条件匹配删除多行
   * 预期：删除约 20% 数据（一个分类）
   */
  describe('6.2 Bulk Delete', () => {
    for (const size of DATA_SIZES) {
      it(`should bulk delete in ${size} row table`, async () => {
        const db = createTestDb(`bulk_delete_${size}`);
        await db.insert('products').values(generateProducts(size)).exec();

        const beforeCount = db.totalRowCount();
        const start = performance.now();
        await db.delete('products')
          .where(col('category').eq('Electronics'))
          .exec();
        const duration = performance.now() - start;

        const deleted = beforeCount - db.totalRowCount();
        record('Delete', 'Bulk Delete (~20%)', size, duration, deleted);
      });
    }
  });

  /**
   * 6.3 有 Live Query 监听时的删除
   *
   * 场景：有 Live Query 监听时删除数据
   * 预期：触发 Live Query 更新通知
   */
  describe('6.3 Delete with Live Query', () => {
    it('should measure delete propagation to live query', async () => {
      const db = createTestDb('delete_live');
      await db.insert('products').values(generateProducts(1000)).exec();

      const stream = db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .changes();
      let updateCount = 0;
      let lastData: any[] = [];
      const unsub = stream.subscribe((data: any[]) => { updateCount++; lastData = data; });

      const initialCount = lastData.length;

      const start = performance.now();
      await db.delete('products')
        .where(col('id').eq(1))
        .exec();
      const duration = performance.now() - start;

      record('Delete', 'With Live Query Propagation', 1000, duration, 1);
      expect(updateCount).toBe(2);
      expect(lastData.length).toBe(initialCount - 1);
      unsub();
    });
  });
});

// =============================================================================
// SECTION 7: Live Query 特性（Live Query Features）
// =============================================================================
/**
 * Live Query 特性测试说明：
 *
 * Live Query 是 Cynos 的核心特性，允许订阅查询结果的实时更新。
 * 本节测试 Live Query 的各种优化特性：
 *
 * - Microtask Batching: 合并快速连续的更新通知
 * - LIMIT Pushdown: 限制返回行数以减少序列化开销
 * - Result Comparison: 跳过无变化的更新通知
 * - Concurrent Queries: 多个 Live Query 的性能
 */
describe('7. Live Query Features', () => {
  /**
   * 7.1 Microtask Batching（微任务批处理）
   *
   * 场景：快速连续插入 100 行数据
   * 预期：多次插入合并为少量通知（而非 100 次通知）
   *
   * 这个优化减少了 Live Query 的更新频率，提高性能
   */
  describe('7.1 Microtask Batching', () => {
    it('should coalesce rapid inserts into single notification', async () => {
      const db = createTestDb('microtask_batch');
      await db.insert('products').values(generateProducts(100)).exec();

      const stream = db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .changes();
      let updateCount = 0;
      const unsub = stream.subscribe(() => { updateCount++; });

      // 并发插入 100 行（不等待每次插入完成）
      const start = performance.now();
      const promises = [];
      for (let i = 0; i < 100; i++) {
        promises.push(db.insert('products').values([{
          id: 10000 + i,
          name: `Batch ${i}`,
          category: 'Electronics',
          price: 1000,
          stock: 10,
          rating: 4.0,
          description: 'Batch test',
          is_active: true,
        }]).exec());
      }
      await Promise.all(promises);
      await new Promise(resolve => setTimeout(resolve, 10));
      const duration = performance.now() - start;

      console.log(`[Microtask Batching] 100 concurrent inserts: ${duration.toFixed(2)}ms, ${updateCount} notifications`);
      // 应该只有 1 次初始通知 + 少量批处理通知（而非 100 次）
      expect(updateCount).toBeLessThanOrEqual(5);
      unsub();
    });
  });

  /**
   * 7.2 LIMIT Pushdown 优化
   *
   * 场景：对比有 LIMIT 和无 LIMIT 的查询性能
   * 预期：有 LIMIT 的查询显著更快
   *
   * 这个测试展示了 LIMIT 对大数据集查询的优化效果：
   * - 无 LIMIT: 需要序列化所有匹配行
   * - 有 LIMIT: 只序列化指定数量的行
   */
  describe('7.2 LIMIT Pushdown Optimization', () => {
    it('should benefit from LIMIT pushdown on large dataset', async () => {
      const db = createTestDb('limit_pushdown');
      await db.insert('products').values(generateProducts(LARGE_SIZE)).exec();

      // 无 LIMIT: 返回约 10000 行（50000 * 20%）
      const startNoLimit = performance.now();
      const noLimitResult = await db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .exec();
      const noLimitDuration = performance.now() - startNoLimit;

      // 有 LIMIT: 只返回 10 行
      const startLimit = performance.now();
      await db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .limit(10)
        .exec();
      const limitDuration = performance.now() - startLimit;

      const speedup = noLimitDuration / limitDuration;
      console.log(`[LIMIT Pushdown] No LIMIT: ${noLimitDuration.toFixed(2)}ms (${noLimitResult.length} rows)`);
      console.log(`[LIMIT Pushdown] LIMIT 10: ${limitDuration.toFixed(2)}ms`);
      console.log(`[LIMIT Pushdown] Speedup: ${speedup.toFixed(1)}x`);

      expect(limitDuration).toBeLessThan(noLimitDuration);
    });
  });

  /**
   * 7.3 Result Comparison 优化
   *
   * 场景：插入不匹配 Live Query 条件的数据
   * 预期：不触发额外的更新通知
   *
   * 这个优化避免了不必要的 UI 更新
   */
  describe('7.3 Result Comparison Optimization', () => {
    it('should skip notification when results unchanged', async () => {
      const db = createTestDb('result_compare');
      await db.insert('products').values(generateProducts(1000)).exec();

      const stream = db.select('*').from('products')
        .where(col('category').eq('Electronics'))
        .changes();
      let updateCount = 0;
      const unsub = stream.subscribe(() => { updateCount++; });

      // 插入不匹配条件的数据（category = 'Clothing'）
      await db.insert('products').values([{
        id: 99999,
        name: 'Non-matching',
        category: 'Clothing', // 不匹配 Electronics
        price: 1000,
        stock: 10,
        rating: 4.0,
        description: 'Test',
        is_active: true,
      }]).exec();

      // 应该只有初始通知，没有额外通知
      expect(updateCount).toBe(1);
      unsub();
    });
  });

  /**
   * 7.4 多个并发 Live Query
   *
   * 场景：同时运行 5 个 Live Query
   * 预期：所有 Live Query 都能正常工作，性能可接受
   *
   * 这个测试验证了多 Live Query 场景的性能
   */
  describe('7.4 Multiple Concurrent Live Queries', () => {
    it('should handle multiple live queries efficiently', async () => {
      const db = createTestDb('concurrent_live');
      await db.insert('products').values(generateProducts(10000)).exec();

      const categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports'];
      const streams: { unsub: () => void; count: number }[] = [];

      // 创建 5 个 Live Query，每个监听不同分类
      const setupStart = performance.now();
      for (const cat of categories) {
        const stream = db.select('*').from('products')
          .where(col('category').eq(cat))
          .limit(10)
          .changes();
        const state = { unsub: () => {}, count: 0 };
        state.unsub = stream.subscribe(() => { state.count++; });
        streams.push(state);
      }
      const setupDuration = performance.now() - setupStart;

      console.log(`[Concurrent Live] Setup 5 live queries: ${setupDuration.toFixed(2)}ms`);

      // 插入一条数据，只影响一个 Live Query
      const insertStart = performance.now();
      await db.insert('products').values([{
        id: 99999,
        name: 'New Electronics',
        category: 'Electronics',
        price: 1000,
        stock: 10,
        rating: 4.0,
        description: 'Test',
        is_active: true,
      }]).exec();
      const insertDuration = performance.now() - insertStart;

      console.log(`[Concurrent Live] Insert + propagate: ${insertDuration.toFixed(2)}ms`);

      // 清理
      streams.forEach(s => s.unsub());
    });
  });
});

// =============================================================================
// SECTION 8: GIN 索引优化（GIN Index Optimization）
// =============================================================================
/**
 * GIN 索引优化测试说明：
 *
 * GIN (Generalized Inverted Index) 索引用于 JSONB 列的路径查询优化。
 * 本节测试 GIN 索引的各种优化特性：
 *
 * - 单谓词查询：单个 JSONB 路径等值查询
 * - GIN Predicate Combination：多个 JSONB 路径 AND 组合查询优化
 *   - 优化器将多个 JSONB 路径谓词合并为单次索引查找
 *   - 在索引层面进行 PostingList 交集运算，避免内存中过滤
 */
describe('8. GIN Index Optimization', () => {
  /**
   * 创建带 JSONB 列的测试数据库
   */
  function createJsonbTestDb(name: string, withIndex: boolean) {
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

  /**
   * 生成 JSONB 测试数据
   *
   * 数据分布：
   * - category: 5 个分类均匀分布 (20% each)
   * - status: 3 个状态均匀分布 (33% each)
   * - priority: 1-5 均匀分布 (20% each)
   *
   * 组合查询选择度：
   * - category = X AND status = Y: ~6.7% (20% * 33%)
   * - category = X AND status = Y AND priority = Z: ~1.3% (20% * 33% * 20%)
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

  /**
   * 8.1 单谓词 GIN 查询
   *
   * 场景：单个 JSONB 路径等值查询
   * 预期：GIN 索引显著提升查询性能
   */
  describe('8.1 Single Predicate GIN Query', () => {
    for (const size of DATA_SIZES) {
      it(`should query single JSONB path in ${size} rows (no index)`, async () => {
        const db = createJsonbTestDb(`gin_single_no_idx_${size}`, false);
        await db.insert('documents').values(generateJsonbDocuments(size)).exec();

        const iterations = 20;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('documents')
            .where(col('metadata').get('$.category').eq('tech'))
            .exec();
        }
        const duration = performance.now() - start;

        const result = await db.select('*').from('documents')
          .where(col('metadata').get('$.category').eq('tech'))
          .exec();
        record('GIN Index', 'Single Predicate (No Index)', size, duration / iterations, result.length);
      });

      it(`should query single JSONB path in ${size} rows (with GIN index)`, async () => {
        const db = createJsonbTestDb(`gin_single_idx_${size}`, true);
        await db.insert('documents').values(generateJsonbDocuments(size)).exec();

        const iterations = 20;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('documents')
            .where(col('metadata').get('$.category').eq('tech'))
            .exec();
        }
        const duration = performance.now() - start;

        const result = await db.select('*').from('documents')
          .where(col('metadata').get('$.category').eq('tech'))
          .exec();
        record('GIN Index', 'Single Predicate (GIN Index)', size, duration / iterations, result.length);
      });
    }
  });

  /**
   * 8.2 GIN Predicate Combination（多谓词组合优化）
   *
   * 场景：多个 JSONB 路径 AND 组合查询
   * 预期：GIN Predicate Combination 优化将多个谓词合并为单次索引查找
   *
   * 优化原理：
   * - 无优化：每个谓词单独查询，结果在内存中交集
   * - 有优化：多个谓词合并，在索引层面进行 PostingList 交集
   */
  describe('8.2 GIN Predicate Combination (Multi-Predicate AND)', () => {
    for (const size of DATA_SIZES) {
      it(`should query 2 JSONB predicates AND in ${size} rows (no index)`, async () => {
        const db = createJsonbTestDb(`gin_multi2_no_idx_${size}`, false);
        await db.insert('documents').values(generateJsonbDocuments(size)).exec();

        const iterations = 20;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('documents')
            .where(
              col('metadata').get('$.category').eq('tech')
                .and(col('metadata').get('$.status').eq('published'))
            )
            .exec();
        }
        const duration = performance.now() - start;

        const result = await db.select('*').from('documents')
          .where(
            col('metadata').get('$.category').eq('tech')
              .and(col('metadata').get('$.status').eq('published'))
          )
          .exec();
        record('GIN Index', '2 Predicates AND (No Index)', size, duration / iterations, result.length);
      });

      it(`should query 2 JSONB predicates AND in ${size} rows (with GIN index)`, async () => {
        const db = createJsonbTestDb(`gin_multi2_idx_${size}`, true);
        await db.insert('documents').values(generateJsonbDocuments(size)).exec();

        const iterations = 20;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('documents')
            .where(
              col('metadata').get('$.category').eq('tech')
                .and(col('metadata').get('$.status').eq('published'))
            )
            .exec();
        }
        const duration = performance.now() - start;

        const result = await db.select('*').from('documents')
          .where(
            col('metadata').get('$.category').eq('tech')
              .and(col('metadata').get('$.status').eq('published'))
          )
          .exec();
        record('GIN Index', '2 Predicates AND (GIN Index)', size, duration / iterations, result.length);
      });

      it(`should query 3 JSONB predicates AND in ${size} rows (no index)`, async () => {
        const db = createJsonbTestDb(`gin_multi3_no_idx_${size}`, false);
        await db.insert('documents').values(generateJsonbDocuments(size)).exec();

        const iterations = 20;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('documents')
            .where(
              col('metadata').get('$.category').eq('tech')
                .and(col('metadata').get('$.status').eq('published'))
                .and(col('metadata').get('$.priority').eq(1))
            )
            .exec();
        }
        const duration = performance.now() - start;

        const result = await db.select('*').from('documents')
          .where(
            col('metadata').get('$.category').eq('tech')
              .and(col('metadata').get('$.status').eq('published'))
              .and(col('metadata').get('$.priority').eq(1))
          )
          .exec();
        record('GIN Index', '3 Predicates AND (No Index)', size, duration / iterations, result.length);
      });

      it(`should query 3 JSONB predicates AND in ${size} rows (with GIN index)`, async () => {
        const db = createJsonbTestDb(`gin_multi3_idx_${size}`, true);
        await db.insert('documents').values(generateJsonbDocuments(size)).exec();

        const iterations = 20;
        const start = performance.now();
        for (let i = 0; i < iterations; i++) {
          await db.select('*').from('documents')
            .where(
              col('metadata').get('$.category').eq('tech')
                .and(col('metadata').get('$.status').eq('published'))
                .and(col('metadata').get('$.priority').eq(1))
            )
            .exec();
        }
        const duration = performance.now() - start;

        const result = await db.select('*').from('documents')
          .where(
            col('metadata').get('$.category').eq('tech')
              .and(col('metadata').get('$.status').eq('published'))
              .and(col('metadata').get('$.priority').eq(1))
          )
          .exec();
        record('GIN Index', '3 Predicates AND (GIN Index)', size, duration / iterations, result.length);
      });
    }
  });

  /**
   * 8.3 查询计划验证
   *
   * 验证 GIN Predicate Combination 优化是否正确生成 GinIndexScanMulti 计划
   */
  describe('8.3 Query Plan Verification', () => {
    it('should use GinIndexScanMulti for multi-predicate AND query', async () => {
      const db = createJsonbTestDb('gin_plan_verify', true);
      await db.insert('documents').values(generateJsonbDocuments(1000)).exec();

      const plan = db.select('*').from('documents')
        .where(
          col('metadata').get('$.category').eq('tech')
            .and(col('metadata').get('$.status').eq('published'))
        )
        .explain();

      // 验证物理计划包含 GinIndexScanMulti（多谓词优化）
      expect(plan.physical).toContain('GinIndexScanMulti');
      console.log('[Query Plan] Multi-predicate AND uses GinIndexScanMulti optimization ✓');
    });

    it('should use GinIndexScan for single predicate query', async () => {
      const db = createJsonbTestDb('gin_plan_single', true);
      await db.insert('documents').values(generateJsonbDocuments(1000)).exec();

      const plan = db.select('*').from('documents')
        .where(col('metadata').get('$.category').eq('tech'))
        .explain();

      // 验证物理计划包含 GinIndexScan（单谓词）
      expect(plan.physical).toContain('GinIndexScan');
      console.log('[Query Plan] Single predicate uses GinIndexScan optimization ✓');
    });
  });
});

// =============================================================================
// SECTION 9: 性能总结（Performance Summary）
// =============================================================================
/**
 * 性能总结
 *
 * 本节汇总所有测试结果，生成性能报告。
 * 报告按类别分组，显示每个操作的：
 * - 执行时间（毫秒）
 * - 影响行数
 * - 吞吐量（行/秒）
 */
describe('9. Performance Summary', () => {
  it('should print comprehensive summary', () => {
    const grouped = perfResults.reduce((acc, r) => {
      const key = r.category;
      if (!acc[key]) acc[key] = [];
      acc[key].push(r);
      return acc;
    }, {} as Record<string, PerfResult[]>);

    for (const [category, results] of Object.entries(grouped)) {
      console.log(`\n${category}:`);
      console.log('-'.repeat(90));
      console.log('  Operation'.padEnd(45) + 'Data Size'.padStart(10) + 'Duration'.padStart(12) + 'Rows'.padStart(10) + 'Throughput'.padStart(15));
      console.log('-'.repeat(90));
      for (const r of results) {
        const throughput = r.rowsAffected / (r.duration / 1000);
        console.log(
          `  ${r.operation.padEnd(43)}` +
          `${r.dataSize.toLocaleString().padStart(10)}` +
          `${r.duration.toFixed(2).padStart(10)}ms` +
          `${r.rowsAffected.toLocaleString().padStart(10)}` +
          `${Math.round(throughput).toLocaleString().padStart(12)}/s`
        );
      }
    }

  });
});
