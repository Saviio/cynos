import fs from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(SCRIPT_DIR, '..');
const TMP_DIR = path.join(ROOT_DIR, 'tmp');
const REPORT_PATH = path.join(TMP_DIR, 'engine_compare.md');
const CYNOS_WASM_PATH = path.join(ROOT_DIR, 'js', 'packages', 'core', 'dist', 'cynos.wasm');

const EXTERNAL_RESOLVE_BASES = [
  process.env.CYNOS_ENGINE_COMPARE_NODE_MODULES,
  SCRIPT_DIR,
  '/tmp/cynos-engine-compare',
].filter(Boolean);

function resolveExternal(specifier) {
  for (const base of EXTERNAL_RESOLVE_BASES) {
    const directCandidate = path.join(base, specifier);
    if (existsSync(directCandidate)) return directCandidate;

    const nodeModulesCandidate = path.join(base, 'node_modules', specifier);
    if (existsSync(nodeModulesCandidate)) return nodeModulesCandidate;
  }

  const searched = EXTERNAL_RESOLVE_BASES.join(', ');
  throw new Error(
    `Unable to resolve ${specifier}. Install benchmark dependencies in ${SCRIPT_DIR} or set CYNOS_ENGINE_COMPARE_NODE_MODULES. Searched: ${searched}`,
  );
}

function moduleUrl(filePath) {
  return pathToFileURL(filePath).href;
}

const cynosModule = await import(
  moduleUrl(path.join(ROOT_DIR, 'js', 'packages', 'core', 'dist', 'wasm.js')),
);
const resultSetModule = await import(
  moduleUrl(path.join(ROOT_DIR, 'js', 'packages', 'core', 'dist', 'result-set.js')),
);
const pgliteModule = await import(
  moduleUrl(resolveExternal('@electric-sql/pglite/dist/index.js')),
);
const pgliteLiveModule = await import(
  moduleUrl(resolveExternal('@electric-sql/pglite/dist/live/index.js')),
);
const sqlJsModule = await import(
  moduleUrl(resolveExternal('sql.js/dist/sql-wasm.js')),
);
const rxdbModule = await import(
  moduleUrl(resolveExternal('rxdb/dist/esm/index.js')),
);
const rxdbStorageMemoryModule = await import(
  moduleUrl(resolveExternal('rxdb/dist/esm/plugins/storage-memory/index.js')),
);

const {
  default: cynosInit,
  Database: CynosDatabase,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  col,
} = cynosModule;
const { ResultSet } = resultSetModule;
const { PGlite } = pgliteModule;
const { live } = pgliteLiveModule;
const { default: initSqlJs } = sqlJsModule;
const { createRxDatabase } = rxdbModule;
const { getRxStorageMemory } = rxdbStorageMemoryModule;

const SQLJS_WASM_DIR = path.dirname(resolveExternal('sql.js/dist/sql-wasm.js'));

const DEPT_COUNT = 100;
const DEFAULT_DATASET_SIZES = [10_000, 100_000];
const QUERY_REPEATS = 9;
const WARMUP_ROUNDS = 5;
const LIVE_UPDATES = 12;
const LIVE_WARMUP_UPDATES = 3;
const LIVE_TIMEOUT_MS = 2_000;
const USER_BATCH_SIZE = 1_000;
const DOC_BATCH_SIZE = 500;

function parseDatasetSizes(raw) {
  const values = String(raw ?? '')
    .split(',')
    .map((value) => Number.parseInt(value.trim(), 10))
    .filter((value) => Number.isFinite(value) && value > 0);

  return values.length > 0 ? values : DEFAULT_DATASET_SIZES;
}

const DATASET_SIZES = parseDatasetSizes(process.env.CYNOS_ENGINE_COMPARE_SIZES);

function uniqueId(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];
}

function mean(values) {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTimeout(promise, ms, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`Timed out waiting for ${label} after ${ms}ms`));
        }, ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function fmtMs(value) {
  if (value == null || Number.isNaN(value)) return 'N/A';
  if (value < 1) return `${value.toFixed(3)} ms`;
  return `${value.toFixed(2)} ms`;
}

function fmtX(value) {
  if (value == null || Number.isNaN(value)) return 'N/A';
  return `${value.toFixed(2)}x`;
}

function fmtBytes(value) {
  if (value == null || Number.isNaN(value)) return 'N/A';
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(2)} MiB`;
}

function sqlString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function formatCountLabel(count) {
  if (count % 1000 === 0) return `${count / 1000}K`;
  return count.toLocaleString();
}

const USER_JOIN_CYNOS_COLUMNS = [
  'users.id',
  'users.age',
  'users.dept_id',
  'users.score',
  'users.active',
  'users.tier',
  'users.name',
  'users.city',
  'departments.name',
  'departments.region',
];

const USER_JOIN_SQL_SELECT = `
  select
    u.id as id,
    u.age as age,
    u.dept_id as dept_id,
    u.score as score,
    u.active as active,
    u.tier as tier,
    u.name as "users.name",
    u.city as city,
    d.name as "departments.name",
    d.region as region
  from users u
  join departments d on u.dept_id = d.id
  where u.age > $1
  limit 1000
`;

const USER_JOIN_SQLITE_SELECT = `
  select
    u.id as id,
    u.age as age,
    u.dept_id as dept_id,
    u.score as score,
    u.active as active,
    u.tier as tier,
    u.name as "users.name",
    u.city as city,
    d.name as "departments.name",
    d.region as region
  from users u
  join departments d on u.dept_id = d.id
  where u.age > ?
  limit 1000
`;

let preparedStatementCounter = 0;

function nextPreparedStatementName(prefix) {
  preparedStatementCounter += 1;
  return `${prefix}_${preparedStatementCounter}`;
}

function sqlLiteral(value) {
  if (value == null) return 'null';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw new Error(`Cannot serialize non-finite SQL literal: ${value}`);
    }
    return String(value);
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  if (typeof value === 'string') {
    return sqlString(value);
  }
  throw new Error(`Unsupported SQL literal type: ${typeof value}`);
}

async function preparePGliteStatement(db, prefix, prepareSql) {
  const name = nextPreparedStatementName(prefix);
  await db.exec(prepareSql.replaceAll('__NAME__', name));

  return {
    async execObject(params = [], mapRow) {
      const result = await db.query(
        `execute ${name}${params.length ? `(${params.map(sqlLiteral).join(', ')})` : ''}`,
        [],
        { rowMode: 'object' },
      );
      return mapRow ? result.rows.map(mapRow) : result.rows;
    },
    async execArray(params = []) {
      const result = await db.query(
        `execute ${name}${params.length ? `(${params.map(sqlLiteral).join(', ')})` : ''}`,
        [],
        { rowMode: 'array' },
      );
      return result.rows;
    },
  };
}

function executeSqliteStatement(stmt, params = [], { objectMode = true, mapRow } = {}) {
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) {
    const row = objectMode ? stmt.getAsObject() : stmt.get();
    rows.push(mapRow ? mapRow(row) : row);
  }
  stmt.reset();
  return rows;
}

function normalizeDocumentRow(row) {
  if (!row || typeof row !== 'object') return row;
  if (typeof row.metadata === 'string') {
    return {
      ...row,
      metadata: JSON.parse(row.metadata),
    };
  }
  return row;
}

function makeUsers(userCount, deptCount) {
  const tiers = ['bronze', 'silver', 'gold', 'platinum'];
  const cities = ['shanghai', 'beijing', 'shenzhen', 'hangzhou', 'chengdu'];
  const rows = [];
  for (let i = 1; i <= userCount; i++) {
    rows.push({
      id: i,
      age: 20 + (i % 50),
      dept_id: i % deptCount,
      score: Number(((i % 1000) / 10).toFixed(1)),
      active: i % 3 !== 0,
      tier: tiers[i % tiers.length],
      name: `user_${i}`,
      city: cities[i % cities.length],
    });
  }
  return rows;
}

function makeDepartments(deptCount) {
  const regions = ['east', 'north', 'south', 'west'];
  const rows = [];
  for (let i = 0; i < deptCount; i++) {
    rows.push({
      id: i,
      name: `dept_${i}`,
      region: regions[i % regions.length],
    });
  }
  return rows;
}

function toRxUser(row) {
  return {
    id: String(row.id),
    age: row.age,
    deptId: row.dept_id,
    score: row.score,
    active: row.active,
    tier: row.tier,
    name: row.name,
    city: row.city,
  };
}

function makeDocuments(docCount) {
  const categories = ['tech', 'business', 'science', 'health', 'sports'];
  const statuses = ['published', 'draft', 'archived'];
  const regions = ['apac', 'emea', 'amer'];
  const rows = [];
  for (let i = 1; i <= docCount; i++) {
    const category = categories[(i - 1) % categories.length];
    const status = statuses[(i - 1) % statuses.length];
    const priority = ((i - 1) % 5) + 1;
    const active = i % 2 === 0;
    rows.push({
      id: i,
      title: `doc_${i}`,
      updated_at: 1_700_000_000 + i,
      metadata: {
        category,
        status,
        priority,
        views: (i * 17) % 100_000,
        active,
        region: regions[(i - 1) % regions.length],
        tags: [category, active ? 'featured' : 'standard', `p${priority}`],
        author: `author_${i % 200}`,
      },
    });
  }
  return rows;
}

function toRxDocument(row) {
  return {
    id: String(row.id),
    title: row.title,
    updatedAt: row.updated_at,
    metadata: row.metadata,
  };
}

function makeMatchingDocument(id) {
  return {
    id,
    title: `matching_${id}`,
    updated_at: 1_800_000_000 + id,
    metadata: {
      category: 'tech',
      status: 'published',
      priority: 1,
      views: 900_000 + (id % 10_000),
      active: true,
      region: 'apac',
      tags: ['tech', 'featured', 'p1'],
      author: `author_${id % 200}`,
    },
  };
}

function makeLiveUserRow(id) {
  return {
    id,
    age: 30,
    dept_id: 42,
    score: 88.8,
    active: true,
    tier: 'gold',
    name: `live_${id}`,
    city: 'shanghai',
  };
}

const datasetCache = new Map();

function getDatasetFixture(size) {
  if (datasetCache.has(size)) {
    return datasetCache.get(size);
  }

  const users = makeUsers(size, DEPT_COUNT);
  const departments = makeDepartments(DEPT_COUNT);
  const documents = makeDocuments(size);
  const fixture = {
    label: formatCountLabel(size),
    userCount: size,
    docCount: size,
    deptCount: DEPT_COUNT,
    pointLookupId: Math.max(1, Math.floor(size * 0.9)),
    users,
    departments,
    rxUsers: users.map(toRxUser),
    documents,
    rxDocuments: documents.map(toRxDocument),
  };

  datasetCache.set(size, fixture);
  return fixture;
}

let cynosReady;
async function ensureCynosReady() {
  if (!cynosReady) {
    cynosReady = fs.readFile(CYNOS_WASM_PATH).then((bytes) =>
      cynosInit({ module_or_path: bytes }),
    );
  }
  await cynosReady;
}

let sqlJsReady;
async function ensureSqlJsReady() {
  if (!sqlJsReady) {
    sqlJsReady = initSqlJs({
      locateFile: (file) => path.join(SQLJS_WASM_DIR, file),
    });
  }
  return sqlJsReady;
}

async function benchmark(fn, { repeats = QUERY_REPEATS, warmup = WARMUP_ROUNDS } = {}) {
  for (let i = 0; i < warmup; i++) {
    await fn({ phase: 'warmup', iteration: i });
  }
  const samples = [];
  for (let i = 0; i < repeats; i++) {
    const start = performance.now();
    await fn({ phase: 'measure', iteration: i });
    samples.push(performance.now() - start);
  }
  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function withCynos(fixture, fn) {
  await ensureCynosReady();
  const db = new CynosDatabase(uniqueId('cmp_cynos'));
  const users = db.createTable('users')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('age', JsDataType.Int32, null)
    .column('dept_id', JsDataType.Int32, null)
    .column('score', JsDataType.Float64, null)
    .column('active', JsDataType.Boolean, null)
    .column('tier', JsDataType.String, null)
    .column('name', JsDataType.String, null)
    .column('city', JsDataType.String, null)
    .index('idx_users_age', 'age')
    .index('idx_users_dept', 'dept_id');
  const departments = db.createTable('departments')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('name', JsDataType.String, null)
    .column('region', JsDataType.String, null);
  db.registerTable(users);
  db.registerTable(departments);
  await db.insert('users').values(fixture.users).exec();
  await db.insert('departments').values(fixture.departments).exec();
  try {
    return await fn(db);
  } finally {
    if (typeof db.free === 'function') db.free();
  }
}

async function withPGlite(fixture, fn) {
  const db = new PGlite({ extensions: { live } });
  await db.exec(`
    create table users(
      id integer primary key,
      age integer not null,
      dept_id integer not null,
      score double precision not null,
      active boolean not null,
      tier text not null,
      name text not null,
      city text not null
    );
    create index idx_users_age on users(age);
    create index idx_users_dept on users(dept_id);
    create table departments(
      id integer primary key,
      name text not null,
      region text not null
    );
  `);

  await db.exec('begin');
  const deptValues = fixture.departments
    .map((row) => `(${row.id}, ${sqlString(row.name)}, ${sqlString(row.region)})`)
    .join(',');
  await db.exec(`insert into departments(id, name, region) values ${deptValues};`);

  for (let start = 0; start < fixture.users.length; start += USER_BATCH_SIZE) {
    const batch = fixture.users.slice(start, start + USER_BATCH_SIZE)
      .map((row) => `(${row.id}, ${row.age}, ${row.dept_id}, ${row.score}, ${row.active}, ${sqlString(row.tier)}, ${sqlString(row.name)}, ${sqlString(row.city)})`)
      .join(',');
    await db.exec(`insert into users(id, age, dept_id, score, active, tier, name, city) values ${batch};`);
  }
  await db.exec('commit');

  try {
    return await fn(db);
  } finally {
    await db.close();
  }
}

async function withSqlite(fixture, fn) {
  const SQL = await ensureSqlJsReady();
  const db = new SQL.Database();
  db.run(`
    create table users(
      id integer primary key,
      age integer not null,
      dept_id integer not null,
      score real not null,
      active integer not null,
      tier text not null,
      name text not null,
      city text not null
    );
    create index idx_users_age on users(age);
    create index idx_users_dept on users(dept_id);
    create table departments(
      id integer primary key,
      name text not null,
      region text not null
    );
  `);

  const insertDept = db.prepare('insert into departments(id, name, region) values (?, ?, ?)');
  for (const row of fixture.departments) insertDept.run([row.id, row.name, row.region]);
  insertDept.free();

  db.run('begin transaction');
  const insertUser = db.prepare(
    'insert into users(id, age, dept_id, score, active, tier, name, city) values (?, ?, ?, ?, ?, ?, ?, ?)',
  );
  for (const row of fixture.users) {
    insertUser.run([row.id, row.age, row.dept_id, row.score, row.active ? 1 : 0, row.tier, row.name, row.city]);
  }
  insertUser.free();
  db.run('commit');

  try {
    return await fn(db);
  } finally {
    db.close();
  }
}

async function withRxdb(fixture, fn) {
  const db = await createRxDatabase({
    name: uniqueId('cmp_rxdb'),
    storage: getRxStorageMemory(),
  });

  const schema = {
    title: 'user schema',
    version: 0,
    primaryKey: 'id',
    type: 'object',
    properties: {
      id: { type: 'string', maxLength: 100 },
      age: { type: 'integer', minimum: 0, maximum: 200, multipleOf: 1 },
      deptId: { type: 'integer', minimum: 0, maximum: 1000, multipleOf: 1 },
      score: { type: 'number', minimum: 0, maximum: 1000, multipleOf: 0.1 },
      active: { type: 'boolean' },
      tier: { type: 'string', maxLength: 100 },
      name: { type: 'string', maxLength: 100 },
      city: { type: 'string', maxLength: 100 },
    },
    required: ['id', 'age', 'deptId', 'score', 'active', 'tier', 'name', 'city'],
    indexes: ['age', 'deptId', 'active'],
  };

  const { users } = await db.addCollections({ users: { schema } });
  await users.bulkInsert(fixture.rxUsers);

  try {
    return await fn({ db, users });
  } finally {
    await db.close();
  }
}

async function withCynosDocuments(fixture, fn, { metadataIndex = true } = {}) {
  await ensureCynosReady();
  const db = new CynosDatabase(uniqueId('cmp_cynos_docs'));
  let documents = db.createTable('documents')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('title', JsDataType.String, null)
    .column('updated_at', JsDataType.Int64, null)
    .column('metadata', JsDataType.Jsonb, null)
    .index('idx_documents_updated_at', 'updated_at');
  if (metadataIndex) {
    documents = documents.index('idx_documents_metadata', 'metadata');
  }
  db.registerTable(documents);
  await db.insert('documents').values(fixture.documents).exec();
  try {
    return await fn(db);
  } finally {
    if (typeof db.free === 'function') db.free();
  }
}

async function withPGliteDocuments(fixture, fn) {
  const db = new PGlite();
  await db.exec(`
    create table documents(
      id bigint primary key,
      title text not null,
      updated_at bigint not null,
      metadata jsonb not null
    );
    create index idx_documents_category on documents ((metadata->>'category'));
    create index idx_documents_compound
      on documents (
        (metadata->>'category'),
        (metadata->>'status'),
        (((metadata->>'priority')::int)),
        updated_at desc
      );
    create index idx_documents_updated_at on documents(updated_at desc);
  `);

  await db.exec('begin');
  for (let start = 0; start < fixture.documents.length; start += DOC_BATCH_SIZE) {
    const batch = fixture.documents.slice(start, start + DOC_BATCH_SIZE)
      .map((row) => `(${row.id}, ${sqlString(row.title)}, ${row.updated_at}, ${sqlString(JSON.stringify(row.metadata))}::jsonb)`)
      .join(',');
    await db.exec(`insert into documents(id, title, updated_at, metadata) values ${batch};`);
  }
  await db.exec('commit');

  try {
    return await fn(db);
  } finally {
    await db.close();
  }
}

async function withSqliteDocuments(fixture, fn) {
  const SQL = await ensureSqlJsReady();
  const db = new SQL.Database();
  db.run(`
    create table documents(
      id integer primary key,
      title text not null,
      updated_at integer not null,
      metadata text not null
    );
    create index idx_documents_category
      on documents(json_extract(metadata, '$.category'));
    create index idx_documents_compound
      on documents(
        json_extract(metadata, '$.category'),
        json_extract(metadata, '$.status'),
        json_extract(metadata, '$.priority'),
        updated_at desc
      );
    create index idx_documents_updated_at on documents(updated_at desc);
  `);

  db.run('begin transaction');
  const stmt = db.prepare(
    'insert into documents(id, title, updated_at, metadata) values (?, ?, ?, ?)',
  );
  for (const row of fixture.documents) {
    stmt.run([row.id, row.title, row.updated_at, JSON.stringify(row.metadata)]);
  }
  stmt.free();
  db.run('commit');

  try {
    return await fn(db);
  } finally {
    db.close();
  }
}

async function withRxdbDocuments(fixture, fn) {
  const db = await createRxDatabase({
    name: uniqueId('cmp_rxdb_docs'),
    storage: getRxStorageMemory(),
  });

  const schema = {
    title: 'document schema',
    version: 0,
    primaryKey: 'id',
    type: 'object',
    properties: {
      id: { type: 'string', maxLength: 100 },
      title: { type: 'string', maxLength: 100 },
      updatedAt: { type: 'integer', minimum: 0, maximum: 3_000_000_000, multipleOf: 1 },
      metadata: {
        type: 'object',
        properties: {
          category: { type: 'string', maxLength: 100 },
          status: { type: 'string', maxLength: 100 },
          priority: { type: 'integer', minimum: 0, maximum: 10, multipleOf: 1 },
          views: { type: 'integer', minimum: 0, maximum: 1_000_000, multipleOf: 1 },
          active: { type: 'boolean' },
          region: { type: 'string', maxLength: 100 },
          author: { type: 'string', maxLength: 100 },
          tags: {
            type: 'array',
            items: { type: 'string' },
          },
        },
        required: ['category', 'status', 'priority', 'views', 'active', 'region', 'author', 'tags'],
      },
    },
    required: ['id', 'title', 'updatedAt', 'metadata'],
    indexes: [
      ['metadata.category', 'metadata.status', 'metadata.priority', 'updatedAt'],
      'metadata.category',
      'updatedAt',
    ],
  };

  const { documents } = await db.addCollections({ documents: { schema } });
  await documents.bulkInsert(fixture.rxDocuments);

  try {
    return await fn({ db, documents });
  } finally {
    await db.close();
  }
}

async function measureCynos(fixture) {
  return withCynos(fixture, async (db) => {
    const pointQuery = db.select('*').from('users').where(col('id').eq(fixture.pointLookupId)).prepare();
    const filter100Query = db.select('*').from('users').where(col('age').gt(60)).limit(100).prepare();
    const scan5000Query = db.select('*').from('users').limit(5000).prepare();
    const orderedRange500Query = db
      .select('*')
      .from('users')
      .where(col('age').between(30, 40))
      .orderBy('id', JsSortOrder.Asc)
      .limit(500)
      .prepare();
    const join1000Query = db
      .select(USER_JOIN_CYNOS_COLUMNS)
      .from('users')
      .innerJoin('departments', col('dept_id').eq('id'))
      .where(col('age').gt(60))
      .limit(1000)
      .prepare();
    const aggregateQuery = db.select('*').from('users').groupBy('dept_id').count().prepare();

    const point = await benchmark(async () => {
      const rows = await pointQuery.exec();
      if (rows.length !== 1) throw new Error(`unexpected Cynos point rows: ${rows.length}`);
    });
    const filter100 = await benchmark(async () => {
      const rows = await filter100Query.exec();
      if (rows.length !== 100) throw new Error(`unexpected Cynos filter rows: ${rows.length}`);
    });
    const scan5000 = await benchmark(async () => {
      const rows = await scan5000Query.exec();
      if (rows.length !== 5000) throw new Error(`unexpected Cynos scan rows: ${rows.length}`);
    });
    const orderedRange500 = await benchmark(async () => {
      const rows = await orderedRange500Query.exec();
      if (rows.length !== 500) throw new Error(`unexpected Cynos ordered range rows: ${rows.length}`);
    });
    const joinProbeRows = await join1000Query.exec();
    const join1000 = joinProbeRows.length === 1000
      ? await benchmark(async () => {
          const rows = await join1000Query.exec();
          if (rows.length !== 1000) throw new Error(`unexpected Cynos join rows: ${rows.length}`);
        })
      : null;
    const aggregate = await benchmark(async () => {
      const rows = await aggregateQuery.exec();
      if (rows.length !== fixture.deptCount) throw new Error(`unexpected Cynos aggregate rows: ${rows.length}`);
    });

    const filter100Layout = filter100Query.getSchemaLayout();
    const scan5000Layout = scan5000Query.getSchemaLayout();
    const pointLayout = pointQuery.getSchemaLayout();

    const pointBinaryOnly = await benchmark(async () => {
      const result = await pointQuery.execBinary();
      if (result.isEmpty()) throw new Error('unexpected empty Cynos binary point result');
      result.free();
    });
    const pointBinaryDecode = await benchmark(async () => {
      const result = await pointQuery.execBinary();
      const rs = new ResultSet(result, pointLayout);
      const rows = rs.toArray();
      if (rows.length !== 1) throw new Error(`unexpected Cynos point decode rows: ${rows.length}`);
      rs.free();
    });
    const filter100BinaryOnly = await benchmark(async () => {
      const result = await filter100Query.execBinary();
      if (result.isEmpty()) throw new Error('unexpected empty Cynos binary filter result');
      result.free();
    });
    const filter100BinaryDecode = await benchmark(async () => {
      const result = await filter100Query.execBinary();
      const rs = new ResultSet(result, filter100Layout);
      const rows = rs.toArray();
      if (rows.length !== 100) throw new Error(`unexpected Cynos filter decode rows: ${rows.length}`);
      rs.free();
    });
    const scan5000BinaryOnly = await benchmark(async () => {
      const result = await scan5000Query.execBinary();
      if (result.isEmpty()) throw new Error('unexpected empty Cynos binary scan result');
      result.free();
    });
    const scan5000BinaryDecode = await benchmark(async () => {
      const result = await scan5000Query.execBinary();
      const rs = new ResultSet(result, scan5000Layout);
      const rows = rs.toArray();
      if (rows.length !== 5000) throw new Error(`unexpected Cynos scan decode rows: ${rows.length}`);
      rs.free();
    });

    const objectMaterialization = {
      pointObject: point,
      pointBinaryDecode: pointBinaryDecode,
      pointBinaryOnly: pointBinaryOnly,
      scan5000Object: scan5000,
      scan5000BinaryDecode: scan5000BinaryDecode,
      scan5000BinaryOnly: scan5000BinaryOnly,
    };

    const pointBuffer = await pointQuery.execBinary();
    const pointBinaryBytes = pointBuffer.len();
    pointBuffer.free();
    const filterBuffer = await filter100Query.execBinary();
    const filter100BinaryBytes = filterBuffer.len();
    filterBuffer.free();
    const scanBuffer = await scan5000Query.execBinary();
    const scan5000BinaryBytes = scanBuffer.len();
    scanBuffer.free();

    const liveChanges = await measureCynosLive(db, fixture, 'changes', 1000);
    const liveTrace = await measureCynosLive(db, fixture, 'trace', 2000);

    return {
      point,
      filter100,
      scan5000,
      orderedRange500,
      join1000,
      joinProbeRowCount: joinProbeRows.length,
      aggregate,
      pointBinaryOnly,
      pointBinaryDecode,
      pointBinaryBytes,
      filter100BinaryOnly,
      filter100BinaryDecode,
      filter100BinaryBytes,
      scan5000BinaryOnly,
      scan5000BinaryDecode,
      scan5000BinaryBytes,
      objectMaterialization,
      liveChanges,
      liveTrace,
    };
  });
}

async function measureCynosLive(db, fixture, mode, baseOffset) {
  const query = db.select('*').from('users').where(col('dept_id').eq(42));
  const observable = mode === 'trace' ? query.trace() : query.changes();

  let resolveNext;
  let expectInitial = mode === 'changes';
  let sawInitial = false;
  const latencies = [];

  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  const unsubscribe = observable.subscribe((payload) => {
    if (expectInitial && !sawInitial) {
      sawInitial = true;
      return;
    }
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(payload);
    }
  });

  if (mode === 'changes') {
    await delay(20);
    if (!sawInitial) expectInitial = false;
  }

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const id = fixture.userCount + baseOffset + i;
    const wait = nextUpdate();
    await db.insert('users').values([{ ...makeLiveUserRow(id), name: `warm_${id}` }]).exec();
    await withTimeout(wait, LIVE_TIMEOUT_MS, `Cynos ${mode} warmup update`);
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const id = fixture.userCount + baseOffset + LIVE_WARMUP_UPDATES + i;
    const wait = nextUpdate();
    const start = performance.now();
    await db.insert('users').values([{ ...makeLiveUserRow(id), name: `live_${id}` }]).exec();
    await withTimeout(wait, LIVE_TIMEOUT_MS, `Cynos ${mode} update`);
    latencies.push(performance.now() - start);
  }
  unsubscribe();

  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

async function measurePGlite(fixture) {
  return withPGlite(fixture, async (db) => {
    const pointStmt = await preparePGliteStatement(
      db,
      'cmp_point',
      'prepare __NAME__(integer) as select * from users where id = $1',
    );
    const filterStmt = await preparePGliteStatement(
      db,
      'cmp_filter',
      'prepare __NAME__(integer) as select * from users where age > $1 limit 100',
    );
    const scanStmt = await preparePGliteStatement(
      db,
      'cmp_scan',
      'prepare __NAME__ as select * from users limit 5000',
    );
    const orderedRangeStmt = await preparePGliteStatement(
      db,
      'cmp_ordered',
      'prepare __NAME__(integer, integer) as select * from users where age between $1 and $2 order by id asc limit 500',
    );
    const joinStmt = await preparePGliteStatement(
      db,
      'cmp_join',
      `prepare __NAME__(integer) as ${USER_JOIN_SQL_SELECT}`,
    );
    const aggregateStmt = await preparePGliteStatement(
      db,
      'cmp_aggregate',
      'prepare __NAME__ as select dept_id, count(*) as count from users group by dept_id',
    );

    const point = await benchmark(async () => {
      const rows = await pointStmt.execObject([fixture.pointLookupId]);
      if (rows.length !== 1) throw new Error(`unexpected PGlite point rows: ${rows.length}`);
    });
    const filter100 = await benchmark(async () => {
      const rows = await filterStmt.execObject([60]);
      if (rows.length !== 100) throw new Error(`unexpected PGlite filter rows: ${rows.length}`);
    });
    const scan5000 = await benchmark(async () => {
      const rows = await scanStmt.execObject();
      if (rows.length !== 5000) throw new Error(`unexpected PGlite scan rows: ${rows.length}`);
    });
    const orderedRange500 = await benchmark(async () => {
      const rows = await orderedRangeStmt.execObject([30, 40]);
      if (rows.length !== 500) throw new Error(`unexpected PGlite ordered range rows: ${rows.length}`);
    });
    const join1000 = await benchmark(async () => {
      const rows = await joinStmt.execObject([60]);
      if (rows.length !== 1000) throw new Error(`unexpected PGlite join rows: ${rows.length}`);
    });
    const aggregate = await benchmark(async () => {
      const rows = await aggregateStmt.execObject();
      if (rows.length !== fixture.deptCount) throw new Error(`unexpected PGlite aggregate rows: ${rows.length}`);
    });

    const pointArray = await benchmark(async () => {
      const rows = await pointStmt.execArray([fixture.pointLookupId]);
      if (rows.length !== 1) throw new Error(`unexpected PGlite point array rows: ${rows.length}`);
    });
    const scan5000Array = await benchmark(async () => {
      const rows = await scanStmt.execArray();
      if (rows.length !== 5000) throw new Error(`unexpected PGlite scan array rows: ${rows.length}`);
    });

    const liveQuery = await measurePGliteLive(db, fixture);
    return {
      point,
      filter100,
      scan5000,
      orderedRange500,
      join1000,
      aggregate,
      objectMaterialization: {
        pointObject: point,
        pointArray,
        scan5000Object: scan5000,
        scan5000Array,
      },
      liveQuery,
    };
  });
}

async function measurePGliteLive(db, fixture) {
  const query = await db.live.incrementalQuery(
    'select * from users where dept_id = $1 order by id',
    [42],
    'id',
  );

  let resolveNext;
  const latencies = [];
  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  query.subscribe((payload) => {
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(payload);
    }
  });

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const id = fixture.userCount + 3000 + i;
    const wait = nextUpdate();
    await db.query(
      'insert into users(id, age, dept_id, score, active, tier, name, city) values ($1, $2, $3, $4, $5, $6, $7, $8)',
      [id, 30, 42, 88.8, true, 'gold', `warm_${id}`, 'shanghai'],
    );
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'PGlite live warmup update');
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const id = fixture.userCount + 3000 + LIVE_WARMUP_UPDATES + i;
    const wait = nextUpdate();
    const start = performance.now();
    await db.query(
      'insert into users(id, age, dept_id, score, active, tier, name, city) values ($1, $2, $3, $4, $5, $6, $7, $8)',
      [id, 30, 42, 88.8, true, 'gold', `live_${id}`, 'shanghai'],
    );
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'PGlite live update');
    latencies.push(performance.now() - start);
  }

  await query.unsubscribe();
  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

async function measureSqlite(fixture) {
  return withSqlite(fixture, async (db) => {
    const pointStmt = db.prepare('select * from users where id = ?');
    const filterStmt = db.prepare('select * from users where age > ? limit 100');
    const scanStmt = db.prepare('select * from users limit 5000');
    const orderedRangeStmt = db.prepare('select * from users where age between ? and ? order by id asc limit 500');
    const joinStmt = db.prepare(USER_JOIN_SQLITE_SELECT);
    const aggStmt = db.prepare('select dept_id, count(*) as count from users group by dept_id');

    const point = await benchmark(async () => {
      const rows = executeSqliteStatement(pointStmt, [fixture.pointLookupId]);
      if (rows.length !== 1) throw new Error(`unexpected SQLite point rows: ${rows.length}`);
    });
    const filter100 = await benchmark(async () => {
      const rows = executeSqliteStatement(filterStmt, [60]);
      if (rows.length !== 100) throw new Error(`unexpected SQLite filter rows: ${rows.length}`);
    });
    const scan5000 = await benchmark(async () => {
      const rows = executeSqliteStatement(scanStmt);
      if (rows.length !== 5000) throw new Error(`unexpected SQLite scan rows: ${rows.length}`);
    });
    const orderedRange500 = await benchmark(async () => {
      const rows = executeSqliteStatement(orderedRangeStmt, [30, 40]);
      if (rows.length !== 500) throw new Error(`unexpected SQLite ordered range rows: ${rows.length}`);
    });
    const join1000 = await benchmark(async () => {
      const rows = executeSqliteStatement(joinStmt, [60]);
      if (rows.length !== 1000) throw new Error(`unexpected SQLite join rows: ${rows.length}`);
    });
    const aggregate = await benchmark(async () => {
      const rows = executeSqliteStatement(aggStmt);
      if (rows.length !== fixture.deptCount) throw new Error(`unexpected SQLite aggregate rows: ${rows.length}`);
    });

    const pointArray = await benchmark(async () => {
      const rows = executeSqliteStatement(pointStmt, [fixture.pointLookupId], { objectMode: false });
      if (rows.length !== 1) throw new Error(`unexpected SQLite point array rows: ${rows.length}`);
    });
    const scan5000Array = await benchmark(async () => {
      const rows = executeSqliteStatement(scanStmt, [], { objectMode: false });
      if (rows.length !== 5000) throw new Error(`unexpected SQLite scan array rows: ${rows.length}`);
    });

    pointStmt.free();
    filterStmt.free();
    scanStmt.free();
    orderedRangeStmt.free();
    joinStmt.free();
    aggStmt.free();
    return {
      point,
      filter100,
      scan5000,
      orderedRange500,
      join1000,
      aggregate,
      objectMaterialization: {
        pointObject: point,
        pointArray,
        scan5000Object: scan5000,
        scan5000Array,
      },
    };
  });
}

async function measureRxdb(fixture) {
  return withRxdb(fixture, async ({ users }) => {
    const point = await benchmark(async () => {
      const doc = await users.findOne(String(fixture.pointLookupId)).exec();
      if (!doc) throw new Error('RxDB point lookup failed');
    });
    const filter100 = await benchmark(async () => {
      const docs = await users.find({ selector: { age: { $gt: 60 } }, limit: 100 }).exec();
      if (docs.length !== 100) throw new Error(`unexpected RxDB filter docs: ${docs.length}`);
    });
    const scan5000 = await benchmark(async () => {
      const docs = await users.find({ selector: {}, limit: 5000 }).exec();
      if (docs.length !== 5000) throw new Error(`unexpected RxDB scan docs: ${docs.length}`);
    });
    const liveQuery = await measureRxdbLive(users, fixture);
    return { point, filter100, scan5000, liveQuery };
  });
}

async function measureRxdbLive(users, fixture) {
  const query = users.find({ selector: { deptId: { $eq: 42 } } });
  let expectInitial = true;
  let initialSeen = false;
  let resolveNext;
  const latencies = [];
  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  const subscription = query.$.subscribe((docs) => {
    if (expectInitial && !initialSeen) {
      initialSeen = true;
      return;
    }
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(docs);
    }
  });

  await delay(20);
  if (!initialSeen) expectInitial = false;

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const id = String(fixture.userCount + 4000 + i);
    const wait = nextUpdate();
    await users.insert({ id, age: 30, deptId: 42, score: 88.8, active: true, tier: 'gold', name: `warm_${id}`, city: 'shanghai' });
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'RxDB live warmup update');
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const id = String(fixture.userCount + 4000 + LIVE_WARMUP_UPDATES + i);
    const wait = nextUpdate();
    const start = performance.now();
    await users.insert({ id, age: 30, deptId: 42, score: 88.8, active: true, tier: 'gold', name: `live_${id}`, city: 'shanghai' });
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'RxDB live update');
    latencies.push(performance.now() - start);
  }
  subscription.unsubscribe();

  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

function makeCynosDocumentQueries(db) {
  const jsonSingleQuery = db
    .select('*')
    .from('documents')
    .where(col('metadata').get('$.category').eq('tech'))
    .limit(100);
  const jsonCompoundQuery = db
    .select('*')
    .from('documents')
    .where(
      col('metadata').get('$.category').eq('tech')
        .and(col('metadata').get('$.status').eq('published'))
        .and(col('metadata').get('$.priority').eq(1))
    )
    .limit(100);
  const complexSortedQuery = db
    .select('*')
    .from('documents')
    .where(
      col('metadata').get('$.category').eq('tech')
        .and(col('metadata').get('$.status').eq('published'))
        .and(col('metadata').get('$.priority').eq(1))
    )
    .orderBy('updated_at', JsSortOrder.Desc)
    .limit(20);

  return {
    jsonSingleQuery,
    jsonCompoundQuery,
    complexSortedQuery,
  };
}

async function measureCynosDocumentQueries(db) {
  const { jsonSingleQuery, jsonCompoundQuery, complexSortedQuery } = makeCynosDocumentQueries(db);
  const preparedJsonSingle = jsonSingleQuery.prepare();
  const preparedJsonCompound = jsonCompoundQuery.prepare();
  const preparedComplexSorted = complexSortedQuery.prepare();
  const jsonSingleExpected = (await preparedJsonSingle.exec()).length;
  const jsonCompoundExpected = (await preparedJsonCompound.exec()).length;
  const complexSortedExpected = (await preparedComplexSorted.exec()).length;

  const jsonSingle = await benchmark(async () => {
    const rows = await preparedJsonSingle.exec();
    if (rows.length !== jsonSingleExpected) throw new Error(`unexpected Cynos JSON single rows: ${rows.length}`);
  });
  const jsonCompound = await benchmark(async () => {
    const rows = await preparedJsonCompound.exec();
    if (rows.length !== jsonCompoundExpected) throw new Error(`unexpected Cynos JSON compound rows: ${rows.length}`);
  });
  const complexSorted = await benchmark(async () => {
    const rows = await preparedComplexSorted.exec();
    if (rows.length !== complexSortedExpected) throw new Error(`unexpected Cynos JSON sorted rows: ${rows.length}`);
  });

  return {
    jsonSingle,
    jsonCompound,
    complexSorted,
  };
}

async function measureCynosDocuments(fixture) {
  return withCynosDocuments(fixture, async (db) => {
    const documentQueries = await measureCynosDocumentQueries(db);
    const mutationDriven = await measureCynosDocumentMutation(db, fixture);
    const reactiveChanges = await measureCynosDocumentReactive(db, fixture, 'changes', 10_000);
    const reactiveTrace = await measureCynosDocumentReactive(db, fixture, 'trace', 20_000);

    return {
      ...documentQueries,
      mutationDriven,
      reactiveChanges,
      reactiveTrace,
    };
  });
}

function pickCynosDocumentQueryMetrics(measurement) {
  return {
    jsonSingle: measurement.jsonSingle,
    jsonCompound: measurement.jsonCompound,
    complexSorted: measurement.complexSorted,
  };
}

async function measureCynosJsonIndexImpact(fixture, withIndexMeasurement) {
  const withoutIndex = await withCynosDocuments(
    fixture,
    async (db) => measureCynosDocumentQueries(db),
    { metadataIndex: false },
  );

  return {
    withIndex: pickCynosDocumentQueryMetrics(withIndexMeasurement),
    withoutIndex,
  };
}

async function measureCynosDocumentMutation(db, fixture) {
  const query = db
    .select('*')
    .from('documents')
    .where(
      col('metadata').get('$.category').eq('tech')
        .and(col('metadata').get('$.status').eq('published'))
        .and(col('metadata').get('$.priority').eq(1))
    )
    .orderBy('updated_at', JsSortOrder.Desc)
    .limit(20)
    .prepare();

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = makeMatchingDocument(fixture.docCount + 1_000 + i);
    await db.insert('documents').values([row]).exec();
    const result = await query.exec();
    if (result[0]?.updated_at !== row.updated_at) {
      throw new Error('unexpected Cynos mutation warmup ordering');
    }
  }

  const samples = [];
  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = makeMatchingDocument(fixture.docCount + 1_000 + LIVE_WARMUP_UPDATES + i);
    const start = performance.now();
    await db.insert('documents').values([row]).exec();
    const result = await query.exec();
    if (result[0]?.updated_at !== row.updated_at) {
      throw new Error('unexpected Cynos mutation ordering');
    }
    samples.push(performance.now() - start);
  }

  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function measureCynosDocumentReactive(db, fixture, mode, baseOffset) {
  const query = db
    .select('*')
    .from('documents')
    .where(
      col('metadata').get('$.category').eq('tech')
        .and(col('metadata').get('$.status').eq('published'))
        .and(col('metadata').get('$.priority').eq(1))
    );
  const observable = mode === 'trace' ? query.trace() : query.changes();

  let resolveNext;
  let expectInitial = mode === 'changes';
  let sawInitial = false;
  const latencies = [];
  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  const unsubscribe = observable.subscribe((payload) => {
    if (expectInitial && !sawInitial) {
      sawInitial = true;
      return;
    }
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(payload);
    }
  });

  if (mode === 'changes') {
    await delay(20);
    if (!sawInitial) expectInitial = false;
  }

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = makeMatchingDocument(fixture.docCount + baseOffset + i);
    const wait = nextUpdate();
    await db.insert('documents').values([row]).exec();
    await withTimeout(wait, LIVE_TIMEOUT_MS, `Cynos JSON ${mode} warmup update`);
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = makeMatchingDocument(fixture.docCount + baseOffset + LIVE_WARMUP_UPDATES + i);
    const wait = nextUpdate();
    const start = performance.now();
    await db.insert('documents').values([row]).exec();
    await withTimeout(wait, LIVE_TIMEOUT_MS, `Cynos JSON ${mode} update`);
    latencies.push(performance.now() - start);
  }
  unsubscribe();

  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

async function measureRxdbDocuments(fixture) {
  return withRxdbDocuments(fixture, async ({ documents }) => {
    const jsonSingleQuery = {
      selector: { 'metadata.category': { $eq: 'tech' } },
      limit: 100,
    };
    const jsonCompoundQuery = {
      selector: {
        'metadata.category': { $eq: 'tech' },
        'metadata.status': { $eq: 'published' },
        'metadata.priority': { $eq: 1 },
      },
      limit: 100,
    };
    const complexSortedQuery = {
      selector: {
        'metadata.category': { $eq: 'tech' },
        'metadata.status': { $eq: 'published' },
        'metadata.priority': { $eq: 1 },
      },
      sort: [{ updatedAt: 'desc' }],
      limit: 20,
    };

    const jsonSingleExpected = (await documents.find(jsonSingleQuery).exec()).length;
    const jsonCompoundExpected = (await documents.find(jsonCompoundQuery).exec()).length;
    const complexSortedExpected = (await documents.find(complexSortedQuery).exec()).length;

    const jsonSingle = await benchmark(async () => {
      const docs = await documents.find(jsonSingleQuery).exec();
      if (docs.length !== jsonSingleExpected) throw new Error(`unexpected RxDB JSON single docs: ${docs.length}`);
    });
    const jsonCompound = await benchmark(async () => {
      const docs = await documents.find(jsonCompoundQuery).exec();
      if (docs.length !== jsonCompoundExpected) throw new Error(`unexpected RxDB JSON compound docs: ${docs.length}`);
    });
    const complexSorted = await benchmark(async () => {
      const docs = await documents.find(complexSortedQuery).exec();
      if (docs.length !== complexSortedExpected) throw new Error(`unexpected RxDB JSON sorted docs: ${docs.length}`);
    });

    const mutationDriven = await measureRxdbDocumentMutation(documents, fixture);
    const reactiveQuery = await measureRxdbDocumentReactive(documents, fixture);

    return {
      jsonSingle,
      jsonCompound,
      complexSorted,
      mutationDriven,
      reactiveQuery,
    };
  });
}

async function measurePGliteDocuments(fixture) {
  return withPGliteDocuments(fixture, async (db) => {
    const jsonSingleStmt = await preparePGliteStatement(
      db,
      'cmp_docs_single',
      `prepare __NAME__(text) as
        select *
        from documents
        where metadata->>'category' = $1
        limit 100`,
    );
    const jsonCompoundStmt = await preparePGliteStatement(
      db,
      'cmp_docs_compound',
      `prepare __NAME__(text, text, integer) as
        select *
        from documents
        where metadata->>'category' = $1
          and metadata->>'status' = $2
          and ((metadata->>'priority')::int) = $3
        limit 100`,
    );
    const complexSortedStmt = await preparePGliteStatement(
      db,
      'cmp_docs_sorted',
      `prepare __NAME__(text, text, integer) as
        select *
        from documents
        where metadata->>'category' = $1
          and metadata->>'status' = $2
          and ((metadata->>'priority')::int) = $3
        order by updated_at desc
        limit 20`,
    );

    const jsonSingleExpected = (await jsonSingleStmt.execObject(['tech'], normalizeDocumentRow)).length;
    const jsonCompoundExpected = (await jsonCompoundStmt.execObject(['tech', 'published', 1], normalizeDocumentRow)).length;
    const complexSortedExpected = (await complexSortedStmt.execObject(['tech', 'published', 1], normalizeDocumentRow)).length;

    const jsonSingle = await benchmark(async () => {
      const rows = await jsonSingleStmt.execObject(['tech'], normalizeDocumentRow);
      if (rows.length !== jsonSingleExpected) {
        throw new Error(`unexpected PGlite JSON single rows: ${rows.length}`);
      }
    });
    const jsonCompound = await benchmark(async () => {
      const rows = await jsonCompoundStmt.execObject(['tech', 'published', 1], normalizeDocumentRow);
      if (rows.length !== jsonCompoundExpected) {
        throw new Error(`unexpected PGlite JSON compound rows: ${rows.length}`);
      }
    });
    const complexSorted = await benchmark(async () => {
      const rows = await complexSortedStmt.execObject(['tech', 'published', 1], normalizeDocumentRow);
      if (rows.length !== complexSortedExpected) {
        throw new Error(`unexpected PGlite JSON sorted rows: ${rows.length}`);
      }
    });

    const mutationDriven = await measurePGliteDocumentMutation(db, fixture);

    return {
      jsonSingle,
      jsonCompound,
      complexSorted,
      mutationDriven,
    };
  });
}

async function measurePGliteDocumentMutation(db, fixture) {
  const query = await preparePGliteStatement(
    db,
    'cmp_docs_mutation',
    `prepare __NAME__(text, text, integer) as
      select *
      from documents
      where metadata->>'category' = $1
        and metadata->>'status' = $2
        and ((metadata->>'priority')::int) = $3
      order by updated_at desc
      limit 20`,
  );

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = makeMatchingDocument(fixture.docCount + 7_000 + i);
    await db.query(
      'insert into documents(id, title, updated_at, metadata) values ($1, $2, $3, $4::jsonb)',
      [row.id, row.title, row.updated_at, JSON.stringify(row.metadata)],
    );
    const rows = await query.execObject(['tech', 'published', 1], normalizeDocumentRow);
    if (rows[0]?.updated_at !== row.updated_at) {
      throw new Error('unexpected PGlite mutation warmup ordering');
    }
  }

  const samples = [];
  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = makeMatchingDocument(fixture.docCount + 7_000 + LIVE_WARMUP_UPDATES + i);
    const start = performance.now();
    await db.query(
      'insert into documents(id, title, updated_at, metadata) values ($1, $2, $3, $4::jsonb)',
      [row.id, row.title, row.updated_at, JSON.stringify(row.metadata)],
    );
    const rows = await query.execObject(['tech', 'published', 1], normalizeDocumentRow);
    if (rows[0]?.updated_at !== row.updated_at) {
      throw new Error('unexpected PGlite mutation ordering');
    }
    samples.push(performance.now() - start);
  }

  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function measureSqliteDocuments(fixture) {
  return withSqliteDocuments(fixture, async (db) => {
    const jsonSingleStmt = db.prepare(`
      select *
      from documents
      where json_extract(metadata, '$.category') = ?
      limit 100
    `);
    const jsonCompoundStmt = db.prepare(`
      select *
      from documents
      where json_extract(metadata, '$.category') = ?
        and json_extract(metadata, '$.status') = ?
        and json_extract(metadata, '$.priority') = ?
      limit 100
    `);
    const complexSortedStmt = db.prepare(`
      select *
      from documents
      where json_extract(metadata, '$.category') = ?
        and json_extract(metadata, '$.status') = ?
        and json_extract(metadata, '$.priority') = ?
      order by updated_at desc
      limit 20
    `);

    const jsonSingleExpected = executeSqliteStatement(
      jsonSingleStmt,
      ['tech'],
      { mapRow: normalizeDocumentRow },
    ).length;
    const jsonCompoundExpected = executeSqliteStatement(
      jsonCompoundStmt,
      ['tech', 'published', 1],
      { mapRow: normalizeDocumentRow },
    ).length;
    const complexSortedExpected = executeSqliteStatement(
      complexSortedStmt,
      ['tech', 'published', 1],
      { mapRow: normalizeDocumentRow },
    ).length;

    const jsonSingle = await benchmark(async () => {
      const rows = executeSqliteStatement(
        jsonSingleStmt,
        ['tech'],
        { mapRow: normalizeDocumentRow },
      );
      if (rows.length !== jsonSingleExpected) throw new Error(`unexpected SQLite JSON single rows: ${rows.length}`);
    });
    const jsonCompound = await benchmark(async () => {
      const rows = executeSqliteStatement(
        jsonCompoundStmt,
        ['tech', 'published', 1],
        { mapRow: normalizeDocumentRow },
      );
      if (rows.length !== jsonCompoundExpected) throw new Error(`unexpected SQLite JSON compound rows: ${rows.length}`);
    });
    const complexSorted = await benchmark(async () => {
      const rows = executeSqliteStatement(
        complexSortedStmt,
        ['tech', 'published', 1],
        { mapRow: normalizeDocumentRow },
      );
      if (rows.length !== complexSortedExpected) throw new Error(`unexpected SQLite JSON sorted rows: ${rows.length}`);
    });

    const mutationDriven = await measureSqliteDocumentMutation(db, fixture);

    jsonSingleStmt.free();
    jsonCompoundStmt.free();
    complexSortedStmt.free();

    return {
      jsonSingle,
      jsonCompound,
      complexSorted,
      mutationDriven,
    };
  });
}

async function measureSqliteDocumentMutation(db, fixture) {
  const query = db.prepare(`
    select *
    from documents
    where json_extract(metadata, '$.category') = ?
      and json_extract(metadata, '$.status') = ?
      and json_extract(metadata, '$.priority') = ?
    order by updated_at desc
    limit 20
  `);
  const insert = db.prepare(
    'insert into documents(id, title, updated_at, metadata) values (?, ?, ?, ?)',
  );

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = makeMatchingDocument(fixture.docCount + 9_000 + i);
    insert.run([row.id, row.title, row.updated_at, JSON.stringify(row.metadata)]);
    const rows = executeSqliteStatement(
      query,
      ['tech', 'published', 1],
      { mapRow: normalizeDocumentRow },
    );
    const current = rows[0];
    if (!current) {
      throw new Error('unexpected empty SQLite mutation warmup result');
    }
    if (current.updated_at !== row.updated_at) {
      throw new Error('unexpected SQLite mutation warmup ordering');
    }
  }

  const samples = [];
  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = makeMatchingDocument(fixture.docCount + 9_000 + LIVE_WARMUP_UPDATES + i);
    const start = performance.now();
    insert.run([row.id, row.title, row.updated_at, JSON.stringify(row.metadata)]);
    const rows = executeSqliteStatement(
      query,
      ['tech', 'published', 1],
      { mapRow: normalizeDocumentRow },
    );
    const current = rows[0];
    if (!current) {
      throw new Error('unexpected empty SQLite mutation result');
    }
    if (current.updated_at !== row.updated_at) {
      throw new Error('unexpected SQLite mutation ordering');
    }
    samples.push(performance.now() - start);
  }

  query.free();
  insert.free();

  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function measureRxdbDocumentMutation(documents, fixture) {
  const query = {
    selector: {
      'metadata.category': { $eq: 'tech' },
      'metadata.status': { $eq: 'published' },
      'metadata.priority': { $eq: 1 },
    },
    sort: [{ updatedAt: 'desc' }],
    limit: 20,
  };

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = toRxDocument(makeMatchingDocument(fixture.docCount + 3_000 + i));
    await documents.insert(row);
    const result = await documents.find(query).exec();
    if (result[0]?.get('updatedAt') !== row.updatedAt) {
      throw new Error('unexpected RxDB mutation warmup ordering');
    }
  }

  const samples = [];
  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = toRxDocument(makeMatchingDocument(fixture.docCount + 3_000 + LIVE_WARMUP_UPDATES + i));
    const start = performance.now();
    await documents.insert(row);
    const result = await documents.find(query).exec();
    if (result[0]?.get('updatedAt') !== row.updatedAt) {
      throw new Error('unexpected RxDB mutation ordering');
    }
    samples.push(performance.now() - start);
  }

  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function measureRxdbDocumentReactive(documents, fixture) {
  const query = documents.find({
    selector: {
      'metadata.category': { $eq: 'tech' },
      'metadata.status': { $eq: 'published' },
      'metadata.priority': { $eq: 1 },
    },
  });

  let expectInitial = true;
  let initialSeen = false;
  let resolveNext;
  const latencies = [];
  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  const subscription = query.$.subscribe((docs) => {
    if (expectInitial && !initialSeen) {
      initialSeen = true;
      return;
    }
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(docs);
    }
  });

  await delay(20);
  if (!initialSeen) expectInitial = false;

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = toRxDocument(makeMatchingDocument(fixture.docCount + 5_000 + i));
    const wait = nextUpdate();
    await documents.insert(row);
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'RxDB JSON live warmup update');
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = toRxDocument(makeMatchingDocument(fixture.docCount + 5_000 + LIVE_WARMUP_UPDATES + i));
    const wait = nextUpdate();
    const start = performance.now();
    await documents.insert(row);
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'RxDB JSON live update');
    latencies.push(performance.now() - start);
  }
  subscription.unsubscribe();

  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

async function measureInsertTimes(fixture) {
  const cynos = await (async () => {
    await ensureCynosReady();
    const start = performance.now();
    const db = new CynosDatabase(uniqueId('cmp_cynos_insert'));
    const users = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('age', JsDataType.Int32, null)
      .column('dept_id', JsDataType.Int32, null)
      .column('score', JsDataType.Float64, null)
      .column('active', JsDataType.Boolean, null)
      .column('tier', JsDataType.String, null)
      .column('name', JsDataType.String, null)
      .column('city', JsDataType.String, null);
    db.registerTable(users);
    await db.insert('users').values(fixture.users).exec();
    const elapsed = performance.now() - start;
    if (typeof db.free === 'function') db.free();
    return elapsed;
  })();

  const pglite = await (async () => {
    const start = performance.now();
    const db = new PGlite();
    await db.exec(`
      create table users(
        id integer primary key,
        age integer not null,
        dept_id integer not null,
        score double precision not null,
        active boolean not null,
        tier text not null,
        name text not null,
        city text not null
      );
    `);
    await db.exec('begin');
    for (let offset = 0; offset < fixture.users.length; offset += USER_BATCH_SIZE) {
      const batch = fixture.users.slice(offset, offset + USER_BATCH_SIZE)
        .map((row) => `(${row.id}, ${row.age}, ${row.dept_id}, ${row.score}, ${row.active}, ${sqlString(row.tier)}, ${sqlString(row.name)}, ${sqlString(row.city)})`)
        .join(',');
      await db.exec(`insert into users(id, age, dept_id, score, active, tier, name, city) values ${batch};`);
    }
    await db.exec('commit');
    const elapsed = performance.now() - start;
    await db.close();
    return elapsed;
  })();

  const sqlite = await (async () => {
    const SQL = await ensureSqlJsReady();
    const start = performance.now();
    const db = new SQL.Database();
    db.run(`
      create table users(
        id integer primary key,
        age integer not null,
        dept_id integer not null,
        score real not null,
        active integer not null,
        tier text not null,
        name text not null,
        city text not null
      );
    `);
    db.run('begin transaction');
    const stmt = db.prepare(
      'insert into users(id, age, dept_id, score, active, tier, name, city) values (?, ?, ?, ?, ?, ?, ?, ?)',
    );
    for (const row of fixture.users) {
      stmt.run([row.id, row.age, row.dept_id, row.score, row.active ? 1 : 0, row.tier, row.name, row.city]);
    }
    stmt.free();
    db.run('commit');
    const elapsed = performance.now() - start;
    db.close();
    return elapsed;
  })();

  const rxdb = await (async () => {
    const start = performance.now();
    const db = await createRxDatabase({
      name: uniqueId('cmp_rxdb_insert'),
      storage: getRxStorageMemory(),
    });
    const schema = {
      title: 'user schema',
      version: 0,
      primaryKey: 'id',
      type: 'object',
      properties: {
        id: { type: 'string', maxLength: 100 },
        age: { type: 'integer', minimum: 0, maximum: 200, multipleOf: 1 },
        deptId: { type: 'integer', minimum: 0, maximum: 1000, multipleOf: 1 },
        score: { type: 'number', minimum: 0, maximum: 1000, multipleOf: 0.1 },
        active: { type: 'boolean' },
        tier: { type: 'string', maxLength: 100 },
        name: { type: 'string', maxLength: 100 },
        city: { type: 'string', maxLength: 100 },
      },
      required: ['id', 'age', 'deptId', 'score', 'active', 'tier', 'name', 'city'],
    };
    const { users } = await db.addCollections({ users: { schema } });
    await users.bulkInsert(fixture.rxUsers);
    const elapsed = performance.now() - start;
    await db.close();
    return elapsed;
  })();

  return { cynos, pglite, sqlite, rxdb };
}

async function packageVersion(packageJsonSpecifier) {
  const raw = await fs.readFile(resolveExternal(packageJsonSpecifier), 'utf8');
  return JSON.parse(raw).version;
}

function renderDatasetSection(lines, datasetRun) {
  const {
    fixture,
    insert,
    cynos,
    cynosDocs,
    cynosJsonIndex,
    pglite,
    pgliteDocs,
    sqlite,
    sqliteDocs,
    rxdb,
    rxdbDocs,
  } = datasetRun;

  const cynosJoinCell = cynos.join1000
    ? fmtMs(cynos.join1000.median)
    : `incorrect in current local build (${cynos.joinProbeRowCount} rows)`;
  const cynosJsonIndexSingleSpeedup = cynosJsonIndex.withoutIndex.jsonSingle.median / cynosJsonIndex.withIndex.jsonSingle.median;
  const cynosJsonIndexCompoundSpeedup = cynosJsonIndex.withoutIndex.jsonCompound.median / cynosJsonIndex.withIndex.jsonCompound.median;
  const cynosJsonIndexComplexSpeedup = cynosJsonIndex.withoutIndex.complexSorted.median / cynosJsonIndex.withIndex.complexSorted.median;

  lines.push(`## ${fixture.label} Dataset`);
  lines.push('');
  lines.push(`Dataset shape: ${fixture.userCount.toLocaleString()} users, ${fixture.deptCount} departments, ${fixture.docCount.toLocaleString()} documents.`);
  lines.push('');
  lines.push(`### Insert ${fixture.label} Rows`);
  lines.push('');
  lines.push('| Engine | Time |');
  lines.push('| --- | ---: |');
  lines.push(`| Cynos | ${fmtMs(insert.cynos)} |`);
  lines.push(`| PGlite | ${fmtMs(insert.pglite)} |`);
  lines.push(`| SQLite (sql.js) | ${fmtMs(insert.sqlite)} |`);
  lines.push(`| RxDB | ${fmtMs(insert.rxdb)} |`);
  lines.push('');
  lines.push('### Prepared Relational Queries (Object Materialized)');
  lines.push('');
  lines.push('Each engine reuses a prepared query path and materializes full JS objects for the main numbers below.');
  lines.push('');
  lines.push('| Benchmark | Cynos | PGlite | SQLite (sql.js) |');
  lines.push('| --- | ---: | ---: | ---: |');
  lines.push(`| Point lookup (` + '`id`' + ` near 90th percentile) | ${fmtMs(cynos.point.median)} | ${fmtMs(pglite.point.median)} | ${fmtMs(sqlite.point.median)} |`);
  lines.push(`| Indexed filter (` + '`age > 60 LIMIT 100`' + `) | ${fmtMs(cynos.filter100.median)} | ${fmtMs(pglite.filter100.median)} | ${fmtMs(sqlite.filter100.median)} |`);
  lines.push(`| Wide scan (` + '`SELECT * LIMIT 5000`' + `) | ${fmtMs(cynos.scan5000.median)} | ${fmtMs(pglite.scan5000.median)} | ${fmtMs(sqlite.scan5000.median)} |`);
  lines.push(`| Ordered range (` + '`age BETWEEN 30 AND 40 ORDER BY id LIMIT 500`' + `) | ${fmtMs(cynos.orderedRange500.median)} | ${fmtMs(pglite.orderedRange500.median)} | ${fmtMs(sqlite.orderedRange500.median)} |`);
  lines.push(`| Join (` + '`users JOIN departments WHERE age > 60 LIMIT 1000`' + `) | ${cynosJoinCell} | ${fmtMs(pglite.join1000.median)} | ${fmtMs(sqlite.join1000.median)} |`);
  lines.push(`| Aggregate (` + '`GROUP BY dept_id COUNT(*)`' + `) | ${fmtMs(cynos.aggregate.median)} | ${fmtMs(pglite.aggregate.median)} | ${fmtMs(sqlite.aggregate.median)} |`);
  if (!cynos.join1000) {
    lines.push('');
    lines.push(`Note: the current local Cynos JS/WASM build returned ${cynos.joinProbeRowCount} rows for the join workload, so I left the Cynos join cell as correctness-invalid instead of publishing a misleading timing.`);
  }
  lines.push('');
  lines.push('### Object Materialization / Lower-Overhead Paths');
  lines.push('');
  lines.push('This section keeps the same prepared queries but changes the result shape: object rows vs lower-overhead array/binary paths.');
  lines.push('');
  lines.push('| Workload | Cynos object | Cynos binary+decode | Cynos binary-only | PGlite object | PGlite array | SQLite object | SQLite array |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  lines.push(`| Point lookup (1 row) | ${fmtMs(cynos.objectMaterialization.pointObject.median)} | ${fmtMs(cynos.objectMaterialization.pointBinaryDecode.median)} | ${fmtMs(cynos.objectMaterialization.pointBinaryOnly.median)} | ${fmtMs(pglite.objectMaterialization.pointObject.median)} | ${fmtMs(pglite.objectMaterialization.pointArray.median)} | ${fmtMs(sqlite.objectMaterialization.pointObject.median)} | ${fmtMs(sqlite.objectMaterialization.pointArray.median)} |`);
  lines.push(`| Wide scan (5000 rows) | ${fmtMs(cynos.objectMaterialization.scan5000Object.median)} | ${fmtMs(cynos.objectMaterialization.scan5000BinaryDecode.median)} | ${fmtMs(cynos.objectMaterialization.scan5000BinaryOnly.median)} | ${fmtMs(pglite.objectMaterialization.scan5000Object.median)} | ${fmtMs(pglite.objectMaterialization.scan5000Array.median)} | ${fmtMs(sqlite.objectMaterialization.scan5000Object.median)} | ${fmtMs(sqlite.objectMaterialization.scan5000Array.median)} |`);
  lines.push('');
  lines.push('| Cynos binary payload | Size | Object vs binary-only | Object vs binary+decode |');
  lines.push('| --- | ---: | ---: | ---: |');
  lines.push(`| Point lookup (1 row) | ${fmtBytes(cynos.pointBinaryBytes)} | ${fmtX(cynos.objectMaterialization.pointObject.median / cynos.objectMaterialization.pointBinaryOnly.median)} | ${fmtX(cynos.objectMaterialization.pointObject.median / cynos.objectMaterialization.pointBinaryDecode.median)} |`);
  lines.push(`| Indexed filter (100 rows) | ${fmtBytes(cynos.filter100BinaryBytes)} | ${fmtX(cynos.filter100.median / cynos.filter100BinaryOnly.median)} | ${fmtX(cynos.filter100.median / cynos.filter100BinaryDecode.median)} |`);
  lines.push(`| Wide scan (5000 rows) | ${fmtBytes(cynos.scan5000BinaryBytes)} | ${fmtX(cynos.objectMaterialization.scan5000Object.median / cynos.objectMaterialization.scan5000BinaryOnly.median)} | ${fmtX(cynos.objectMaterialization.scan5000Object.median / cynos.objectMaterialization.scan5000BinaryDecode.median)} |`);
  lines.push('');
  lines.push('### Reactive / Live Query Latency');
  lines.push('');
  lines.push('| Mode | Median latency |');
  lines.push('| --- | ---: |');
  lines.push(`| Cynos ` + '`changes()`' + ` | ${fmtMs(cynos.liveChanges.median)} |`);
  lines.push(`| Cynos ` + '`trace()`' + ` | ${fmtMs(cynos.liveTrace.median)} |`);
  lines.push(`| PGlite ` + '`live.incrementalQuery()`' + ` | ${fmtMs(pglite.liveQuery.median)} |`);
  lines.push(`| RxDB ` + '`RxQuery.$`' + ` | ${fmtMs(rxdb.liveQuery.median)} |`);
  lines.push(`| SQLite (sql.js) | N/A |`);
  lines.push('');
  lines.push('### RxDB Document / Reactive Reference');
  lines.push('');
  lines.push('| Benchmark | Cynos | RxDB |');
  lines.push('| --- | ---: | ---: |');
  lines.push(`| Point lookup | ${fmtMs(cynos.point.median)} | ${fmtMs(rxdb.point.median)} |`);
  lines.push(`| Filter (` + '`age > 60 LIMIT 100`' + `) | ${fmtMs(cynos.filter100.median)} | ${fmtMs(rxdb.filter100.median)} |`);
  lines.push(`| Wide scan (` + '`LIMIT 5000`' + `) | ${fmtMs(cynos.scan5000.median)} | ${fmtMs(rxdb.scan5000.median)} |`);
  lines.push(`| Live query update | ${fmtMs(cynos.liveChanges.median)} | ${fmtMs(rxdb.liveQuery.median)} |`);
  lines.push('');
  lines.push('### JSON / Document Query Benchmarks');
  lines.push('');
  lines.push('This section uses a same-size document dataset with nested metadata. Cynos, PGlite, and SQLite run structured JSON predicates inside SQL/WASM engines; RxDB runs nested document selectors over its document store.');
  lines.push('');
  lines.push('| Benchmark | Cynos | PGlite | SQLite (sql.js) | RxDB |');
  lines.push('| --- | ---: | ---: | ---: | ---: |');
  lines.push(`| JSON filter (` + '`metadata.category = tech LIMIT 100`' + `) | ${fmtMs(cynosDocs.jsonSingle.median)} | ${fmtMs(pgliteDocs.jsonSingle.median)} | ${fmtMs(sqliteDocs.jsonSingle.median)} | ${fmtMs(rxdbDocs.jsonSingle.median)} |`);
  lines.push(`| Compound JSON filter (` + '`category + status + priority`' + `) | ${fmtMs(cynosDocs.jsonCompound.median)} | ${fmtMs(pgliteDocs.jsonCompound.median)} | ${fmtMs(sqliteDocs.jsonCompound.median)} | ${fmtMs(rxdbDocs.jsonCompound.median)} |`);
  lines.push(`| Complex doc query (` + '`category + status + priority ORDER BY updatedAt DESC LIMIT 20`' + `) | ${fmtMs(cynosDocs.complexSorted.median)} | ${fmtMs(pgliteDocs.complexSorted.median)} | ${fmtMs(sqliteDocs.complexSorted.median)} | ${fmtMs(rxdbDocs.complexSorted.median)} |`);
  lines.push(`| Mutation-driven requery (` + '`insert + complex query`' + `) | ${fmtMs(cynosDocs.mutationDriven.median)} | ${fmtMs(pgliteDocs.mutationDriven.median)} | ${fmtMs(sqliteDocs.mutationDriven.median)} | ${fmtMs(rxdbDocs.mutationDriven.median)} |`);
  lines.push('');
  lines.push('### Cynos JSONB Index Impact');
  lines.push('');
  lines.push('This section isolates the Cynos `metadata` secondary index. Both sides keep the same document dataset and `updated_at` index; the only difference is whether the JSONB metadata index exists.');
  lines.push('');
  lines.push('| Benchmark | With metadata index | Without metadata index | Speedup |');
  lines.push('| --- | ---: | ---: | ---: |');
  lines.push(`| JSON filter (` + '`metadata.category = tech LIMIT 100`' + `) | ${fmtMs(cynosJsonIndex.withIndex.jsonSingle.median)} | ${fmtMs(cynosJsonIndex.withoutIndex.jsonSingle.median)} | ${fmtX(cynosJsonIndexSingleSpeedup)} |`);
  lines.push(`| Compound JSON filter (` + '`category + status + priority`' + `) | ${fmtMs(cynosJsonIndex.withIndex.jsonCompound.median)} | ${fmtMs(cynosJsonIndex.withoutIndex.jsonCompound.median)} | ${fmtX(cynosJsonIndexCompoundSpeedup)} |`);
  lines.push(`| Complex doc query (` + '`category + status + priority ORDER BY updated_at DESC LIMIT 20`' + `) | ${fmtMs(cynosJsonIndex.withIndex.complexSorted.median)} | ${fmtMs(cynosJsonIndex.withoutIndex.complexSorted.median)} | ${fmtX(cynosJsonIndexComplexSpeedup)} |`);
  lines.push('');
  lines.push('| Reactive complex document update | Cynos `changes()` | Cynos `trace()` | RxDB `RxQuery.$` |');
  lines.push('| --- | ---: | ---: | ---: |');
  lines.push(`| Insert matching doc into compound nested query | ${fmtMs(cynosDocs.reactiveChanges.median)} | ${fmtMs(cynosDocs.reactiveTrace.median)} | ${fmtMs(rxdbDocs.reactiveQuery.median)} |`);
  lines.push('');
}

function buildReport(datasetRuns, versions) {
  const lines = [];
  const sizeSummary = datasetRuns
    .map((datasetRun) => datasetRun.fixture.label)
    .join(', ');

  lines.push('# Temporary Engine Comparison');
  lines.push('');
  lines.push(`Generated on: ${new Date().toISOString()}`);
  lines.push('');
  lines.push('Scope: Node.js local benchmark with Cynos (WASM), SQLite via sql.js (WASM), PGlite (Postgres-on-WASM), plus a separate RxDB reactive/document reference section.');
  lines.push('');
  lines.push('## Method');
  lines.push('');
  lines.push(`- Dataset sizes in this run: ${sizeSummary}. Each size uses ${DEPT_COUNT} departments and a same-size nested-document dataset.`);
  lines.push(`- Point lookup targets the 90th-percentile id for each size, so the lookup remains meaningful at both 10K and 100K scales.`);
  lines.push(`- User rows are intentionally wider than the first draft: integers + float + boolean + multiple strings, so JS/WASM result materialization cost shows up more clearly.`);
  lines.push('- Main Cynos / PGlite / SQLite relational numbers now use aligned semantics: prepared query reuse plus full JS object materialization of the same selected columns.');
  lines.push('- The join workload is explicitly aligned across engines, including duplicate-name handling, instead of comparing `SELECT *` on one side against a narrower projected join on another.');
  lines.push('- SQLite document rows normalize `metadata` into parsed JS objects so the document benchmarks match Cynos/PGlite result semantics instead of comparing structured objects against raw JSON text.');
  lines.push('- Document rows intentionally use nested metadata, multi-predicate filters, sort+limit, and mutation-driven requery so the JSON section is not just measuring warm-cache steady-state reads.');
  lines.push('- JSON query indexes are enabled in the main document comparison for each engine where applicable; Cynos also gets a separate index on/off isolation section.');
  lines.push(`- Each query benchmark uses ${WARMUP_ROUNDS} warmup runs, then ${QUERY_REPEATS} measured runs; tables below report median latency.`);
  lines.push(`- Live-query latency uses ${LIVE_WARMUP_UPDATES} unmeasured warmup inserts, then ${LIVE_UPDATES} measured inserts.`);
  lines.push('- Cynos is already measured as Node.js + WASM here, so the runtime is aligned with sql.js and PGlite.');
  lines.push('- RxDB is not a relational WASM SQL engine; it is shown separately as a JS document/reactive baseline, not as an apples-to-apples SQL engine peer.');
  lines.push('- RxDB repeated read numbers on an unchanged dataset are heavily shaped by its query cache and document cache, so treat them as app-layer cache-hit latency, not raw relational scan throughput.');
  lines.push(`- Override dataset sizes with ` + '`CYNOS_ENGINE_COMPARE_SIZES=10000,100000`' + ` if you want a different mix.`);
  lines.push('');
  lines.push('## Versions');
  lines.push('');
  lines.push(`- Node.js: ${process.version}`);
  lines.push('- Cynos: local workspace JS/WASM build');
  lines.push(`- PGlite: ${versions.pglite}`);
  lines.push(`- RxDB: ${versions.rxdb}`);
  lines.push(`- sql.js: ${versions.sqljs}`);
  lines.push('');
  lines.push('## Capability Snapshot');
  lines.push('');
  lines.push('| Capability | Cynos | PGlite | SQLite (sql.js) | RxDB |');
  lines.push('| --- | --- | --- | --- | --- |');
  lines.push('| Relational joins | Yes | Yes | Yes | No built-in cross-collection relational join engine |');
  lines.push('| GROUP BY / aggregates | Yes | Yes | Yes | No built-in relational aggregate query layer |');
  lines.push('| Structured JSON query in this harness | Yes (`JSONB` path query + metadata index) | Yes (`jsonb` + expression indexes) | Yes (JSON1 `json_extract` + expression indexes) | Yes (nested Mango selectors) |');
  lines.push('| Live full-result query API | Yes (`changes()`) | Yes (`live.incrementalQuery()`) | No built-in API in this harness | Yes (`RxQuery.$`) |');
  lines.push('| Delta-first reactive path | Yes (`trace()`) | Partial (`live.changes`) | No | No comparable built-in delta stream |');
  lines.push('| Binary result transport | Yes (`execBinary()`) | No comparable engine-native path in this harness | No comparable engine-native path in this harness | No |');
  lines.push('');

  for (const datasetRun of datasetRuns) {
    renderDatasetSection(lines, datasetRun);
  }

  lines.push('## What This Suggests');
  lines.push('');
  lines.push('- The 10K runs are useful as a fast sanity-check, but the 100K runs are usually the better guide for sustained scan, join, JSON, and reactive costs.');
  lines.push('- Cynos is not trying to beat every mature SQL engine on every scalar query. In this harness, SQLite(sql.js) remains excellent on very small point lookups, scalar JSON reads, and some aggregate-heavy paths.');
  lines.push('- Once the query semantics are aligned, the cross-engine object-materialization table is often as important as the raw relational table: some apparent “query speed” differences were really result-shape differences.');
  lines.push('- Cynos becomes more differentiated once you care about one engine doing joins, aggregates, structured JSON queries, and reactive updates together inside a compact embedded runtime.');
  lines.push('- `execBinary()` is still the clearest Cynos-specific advantage in JS/WASM embeddings: when result sets get wider or larger, it can skip or defer JS object materialization entirely.');
  lines.push('- Relative to PGlite, Cynos trades SQL breadth and Postgres compatibility for a tighter app-runtime execution path and much cheaper live-query updates in this workload.');
  lines.push('- Relative to SQLite and PGlite, Cynos can expose structured JSON query plus fine-grained reactive APIs without switching to a separate sync or query layer.');
  lines.push('- Relative to RxDB, the real Cynos advantage is not “single flat query faster on warm cache”; it is that the same engine can do relational joins, aggregates, JSONB path filters, binary result transport, and delta-style reactivity without switching query models.');
  lines.push('- The JSONB index-isolation section tells you whether Cynos path-query wins are coming from real index leverage or only from upper-layer caching; keep an eye on that delta when evaluating production schema choices.');
  lines.push('');
  lines.push('## Fact-Check Pointers');
  lines.push('');
  lines.push('- RxDB `RxQuery`, query cache, and EventReduce: https://rxdb.info/rx-query.html');
  lines.push('- RxDB Query Optimizer (premium / optional): https://rxdb.info/query-optimizer.html');
  lines.push('- PGlite live queries: https://pglite.dev/docs/live-queries');
  lines.push('- PostgreSQL JSON functions/operators (PGlite compatibility baseline): https://www.postgresql.org/docs/current/functions-json.html');
  lines.push('- SQLite JSON1 extension: https://www.sqlite.org/json1.html');
  lines.push('');
  return lines.join('\n');
}

async function runDatasetBenchmarks(fixture) {
  console.error(`[${fixture.label}] measuring inserts`);
  const insert = await measureInsertTimes(fixture);
  console.error(`[${fixture.label}] measuring Cynos relational path`);
  const cynos = await measureCynos(fixture);
  console.error(`[${fixture.label}] measuring Cynos document path`);
  const cynosDocs = await measureCynosDocuments(fixture);
  console.error(`[${fixture.label}] measuring Cynos JSONB index impact`);
  const cynosJsonIndex = await measureCynosJsonIndexImpact(fixture, cynosDocs);
  console.error(`[${fixture.label}] measuring PGlite relational path`);
  const pglite = await measurePGlite(fixture);
  console.error(`[${fixture.label}] measuring PGlite document path`);
  const pgliteDocs = await measurePGliteDocuments(fixture);
  console.error(`[${fixture.label}] measuring SQLite relational path`);
  const sqlite = await measureSqlite(fixture);
  console.error(`[${fixture.label}] measuring SQLite document path`);
  const sqliteDocs = await measureSqliteDocuments(fixture);
  console.error(`[${fixture.label}] measuring RxDB relational/document baseline`);
  const rxdb = await measureRxdb(fixture);
  const rxdbDocs = await measureRxdbDocuments(fixture);

  return { fixture, insert, cynos, cynosDocs, cynosJsonIndex, pglite, pgliteDocs, sqlite, sqliteDocs, rxdb, rxdbDocs };
}

async function main() {
  const versions = {
    pglite: await packageVersion('@electric-sql/pglite/package.json'),
    rxdb: await packageVersion('rxdb/package.json'),
    sqljs: await packageVersion('sql.js/package.json'),
  };

  const datasetRuns = [];
  for (const size of DATASET_SIZES) {
    const fixture = getDatasetFixture(size);
    datasetRuns.push(await runDatasetBenchmarks(fixture));
  }

  const report = buildReport(datasetRuns, versions);
  await fs.mkdir(TMP_DIR, { recursive: true });
  await fs.writeFile(REPORT_PATH, report, 'utf8');
  console.log(report);
  console.error(`\nReport written to ${REPORT_PATH}`);
}

await main();
