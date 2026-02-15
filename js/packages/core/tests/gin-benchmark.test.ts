/**
 * GIN 索引性能基准测试
 *
 * 测试 JSONB 路径查询在有/无 GIN 索引优化时的性能差异
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

// 生成测试数据 - 增加 status 字段用于多谓词测试
function generateProducts(count: number) {
  const categories = ['Electronics', 'Furniture', 'Clothing', 'Books', 'Sports'];
  const statuses = ['active', 'inactive', 'pending'];
  const products = [];

  for (let i = 1; i <= count; i++) {
    products.push({
      id: i,
      name: `Product ${i}`,
      metadata: {
        category: categories[i % categories.length],
        status: statuses[i % statuses.length],
        price: Math.floor(Math.random() * 1000) + 100,
        tags: [`tag${i % 10}`, `tag${(i + 1) % 10}`],
        rating: (Math.random() * 5).toFixed(1),
      }
    });
  }

  return products;
}

describe('GIN 索引性能基准测试', () => {
  const DATA_SIZES = [1000, 5000, 10000];
  const QUERY_ITERATIONS = 10;

  for (const dataSize of DATA_SIZES) {
    describe(`数据量: ${dataSize} 行`, () => {

      it(`单谓词查询 (无 GIN 索引)`, async () => {
        const db = new Database(`gin_bench_no_index_${dataSize}`);
        const builder = db.createTable('products')
          .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
          .column('name', JsDataType.String, null)
          .column('metadata', JsDataType.Jsonb, null);
          // 不创建索引
        db.registerTable(builder);

        // 插入数据
        const products = generateProducts(dataSize);
        const batchSize = 500;
        for (let i = 0; i < products.length; i += batchSize) {
          const batch = products.slice(i, i + batchSize);
          await db.insert('products').values(batch).exec();
        }

        // 预热
        await db.select('*').from('products')
          .where(col('metadata').get('$.category').eq('Electronics'))
          .exec();

        // 基准测试
        const startTime = performance.now();
        let totalResults = 0;

        for (let i = 0; i < QUERY_ITERATIONS; i++) {
          const result = await db.select('*').from('products')
            .where(col('metadata').get('$.category').eq('Electronics'))
            .exec();
          totalResults += result.length;
        }

        const endTime = performance.now();
        const avgTime = (endTime - startTime) / QUERY_ITERATIONS;

        console.log(`[无索引-单谓词] ${dataSize} 行, 平均查询时间: ${avgTime.toFixed(2)}ms, 结果数: ${totalResults / QUERY_ITERATIONS}`);

        expect(totalResults).toBeGreaterThan(0);
      });

      it(`单谓词查询 (有 GIN 索引)`, async () => {
        const db = new Database(`gin_bench_with_index_${dataSize}`);
        const builder = db.createTable('products')
          .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
          .column('name', JsDataType.String, null)
          .column('metadata', JsDataType.Jsonb, null)
          .index('idx_metadata', 'metadata');  // 创建 GIN 索引
        db.registerTable(builder);

        // 插入数据
        const products = generateProducts(dataSize);
        const batchSize = 500;
        for (let i = 0; i < products.length; i += batchSize) {
          const batch = products.slice(i, i + batchSize);
          await db.insert('products').values(batch).exec();
        }

        // 预热
        await db.select('*').from('products')
          .where(col('metadata').get('$.category').eq('Electronics'))
          .exec();

        // 基准测试
        const startTime = performance.now();
        let totalResults = 0;

        for (let i = 0; i < QUERY_ITERATIONS; i++) {
          const result = await db.select('*').from('products')
            .where(col('metadata').get('$.category').eq('Electronics'))
            .exec();
          totalResults += result.length;
        }

        const endTime = performance.now();
        const avgTime = (endTime - startTime) / QUERY_ITERATIONS;

        console.log(`[有索引-单谓词] ${dataSize} 行, 平均查询时间: ${avgTime.toFixed(2)}ms, 结果数: ${totalResults / QUERY_ITERATIONS}`);

        expect(totalResults).toBeGreaterThan(0);
      });

      // 多谓词组合测试 - 测试 GIN Predicate Combination 优化效果
      it(`多谓词 AND 查询 (无 GIN 索引)`, async () => {
        const db = new Database(`gin_bench_multi_no_index_${dataSize}`);
        const builder = db.createTable('products')
          .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
          .column('name', JsDataType.String, null)
          .column('metadata', JsDataType.Jsonb, null);
        db.registerTable(builder);

        const products = generateProducts(dataSize);
        const batchSize = 500;
        for (let i = 0; i < products.length; i += batchSize) {
          const batch = products.slice(i, i + batchSize);
          await db.insert('products').values(batch).exec();
        }

        // 预热
        await db.select('*').from('products')
          .where(col('metadata').get('$.category').eq('Electronics'))
          .exec();

        // 多谓词 AND 查询: category = 'Electronics' AND status = 'active'
        const startTime = performance.now();
        let totalResults = 0;

        for (let i = 0; i < QUERY_ITERATIONS; i++) {
          const result = await db.select('*').from('products')
            .where(
              col('metadata').get('$.category').eq('Electronics')
                .and(col('metadata').get('$.status').eq('active'))
            )
            .exec();
          totalResults += result.length;
        }

        const endTime = performance.now();
        const avgTime = (endTime - startTime) / QUERY_ITERATIONS;

        console.log(`[无索引-多谓词AND] ${dataSize} 行, 平均查询时间: ${avgTime.toFixed(2)}ms, 结果数: ${totalResults / QUERY_ITERATIONS}`);

        expect(totalResults).toBeGreaterThan(0);
      });

      it(`多谓词 AND 查询 (有 GIN 索引)`, async () => {
        const db = new Database(`gin_bench_multi_with_index_${dataSize}`);
        const builder = db.createTable('products')
          .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
          .column('name', JsDataType.String, null)
          .column('metadata', JsDataType.Jsonb, null)
          .index('idx_metadata', 'metadata');
        db.registerTable(builder);

        const products = generateProducts(dataSize);
        const batchSize = 500;
        for (let i = 0; i < products.length; i += batchSize) {
          const batch = products.slice(i, i + batchSize);
          await db.insert('products').values(batch).exec();
        }

        // 预热
        await db.select('*').from('products')
          .where(col('metadata').get('$.category').eq('Electronics'))
          .exec();

        // 多谓词 AND 查询: category = 'Electronics' AND status = 'active'
        const startTime = performance.now();
        let totalResults = 0;

        for (let i = 0; i < QUERY_ITERATIONS; i++) {
          const result = await db.select('*').from('products')
            .where(
              col('metadata').get('$.category').eq('Electronics')
                .and(col('metadata').get('$.status').eq('active'))
            )
            .exec();
          totalResults += result.length;
        }

        const endTime = performance.now();
        const avgTime = (endTime - startTime) / QUERY_ITERATIONS;

        console.log(`[有索引-多谓词AND] ${dataSize} 行, 平均查询时间: ${avgTime.toFixed(2)}ms, 结果数: ${totalResults / QUERY_ITERATIONS}`);

        expect(totalResults).toBeGreaterThan(0);
      });
    });
  }
});
