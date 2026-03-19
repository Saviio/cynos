import { beforeAll, describe, expect, it } from 'vitest';
import init, {
  ColumnOptions,
  Database,
  JsDataType,
  col,
} from '../wasm/cynos_database.js';

beforeAll(async () => {
  await init();
});

describe('GIN regression suite', () => {
  it('uses GIN for nested JSON path equality once nested postings are indexed', async () => {
    const db = new Database('gin_reg_nested_path');
    const builder = db.createTable('profiles')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('profile', JsDataType.Jsonb, null)
      .index('idx_profile', 'profile');
    db.registerTable(builder);

    await db.insert('profiles').values([
      {
        id: 1,
        name: 'Alice',
        profile: { address: { city: 'Beijing', zip: '100000' }, active: true },
      },
      {
        id: 2,
        name: 'Bob',
        profile: { address: { city: 'Shanghai', zip: '200000' }, active: true },
      },
      {
        id: 3,
        name: 'Cara',
        profile: { address: { city: 'Beijing', zip: '100001' }, active: false },
      },
    ]).exec();

    const query = db.select('*')
      .from('profiles')
      .where(col('profile').get('$.address.city').eq('Beijing'));

    const plan = query.explain();
    const rows = await query.exec();

    expect(rows.map((row: any) => row.name).sort()).toEqual(['Alice', 'Cara']);
    expect(String(plan.physical)).toContain('GinIndexScan');
    expect(String(plan.physical)).toContain('key: "address.city"');
    expect(String(plan.physical)).toContain('query_type: "eq"');
  });

  it('preserves unhandled GIN predicates when eq is combined with exists', async () => {
    const db = new Database('gin_reg_mixed_predicates');
    const builder = db.createTable('articles')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('title', JsDataType.String, null)
      .column('tags', JsDataType.Jsonb, null)
      .index('idx_tags', 'tags');
    db.registerTable(builder);

    await db.insert('articles').values([
      {
        id: 1,
        title: 'Article 1',
        tags: { primary: 'tech', secondary: ['ai', 'ml'] },
      },
      {
        id: 2,
        title: 'Article 2',
        tags: { primary: 'tech' },
      },
      {
        id: 3,
        title: 'Article 3',
        tags: { primary: 'news', secondary: ['world'] },
      },
    ]).exec();

    const query = db.select('*')
      .from('articles')
      .where(
        col('tags').get('$.primary').eq('tech')
          .and(col('tags').get('$.secondary').exists()),
      );

    const plan = query.explain();
    const optimized = String(plan.optimized).toLowerCase();
    const rows = await query.exec();

    expect(rows.map((row: any) => row.title)).toEqual(['Article 1']);
    expect(
      optimized.includes('jsonb_exists') || optimized.includes('ginindexscanmulti'),
    ).toBe(true);
  });

  it('evaluates jsonb contains against the provided value instead of path existence', async () => {
    const db = new Database('gin_reg_contains_value');
    const builder = db.createTable('products')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('metadata', JsDataType.Jsonb, null)
      .index('idx_metadata', 'metadata');
    db.registerTable(builder);

    await db.insert('products').values([
      {
        id: 1,
        name: 'Laptop',
        metadata: { tags: ['computer', 'portable'] },
      },
      {
        id: 2,
        name: 'Phone',
        metadata: { tags: ['mobile'] },
      },
      {
        id: 3,
        name: 'Tablet',
        metadata: { tags: ['portable', 'touch'] },
      },
    ]).exec();

    const rows = await db.select('*')
      .from('products')
      .where(col('metadata').get('$.tags').contains('portable'))
      .exec();

    expect(rows.map((row: any) => row.name).sort()).toEqual(['Laptop', 'Tablet']);
  });
});
