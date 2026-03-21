/**
 * GraphQL Worker
 *
 * Runs Cynos GraphQL adapter in a Web Worker.
 */

import {
  initCynos,
  createDatabase,
  JsDataType,
  ColumnOptions,
  ForeignKeyOptions,
  type Database,
  type JsGraphqlSubscription,
} from '@cynos/core'

let db: Database | null = null

// Message types
export type GqlWorkerMessage =
  | { type: 'init' }
  | { type: 'getSchema' }
  | { type: 'execute'; query: string; variables?: Record<string, unknown> }
  | { type: 'subscribe'; query: string }
  | { type: 'unsubscribe' }

export type GqlMainMessage =
  | { type: 'ready'; userCount: number; postCount: number }
  | { type: 'schema'; sdl: string }
  | { type: 'result'; data: unknown; execTime: number }
  | { type: 'subscriptionData'; data: unknown }
  | { type: 'error'; message: string }
  | { type: 'counts'; userCount: number; postCount: number }

function postToMain(msg: GqlMainMessage) {
  self.postMessage(msg)
}

let currentSubscription: JsGraphqlSubscription | null = null
let currentUnsubscribe: (() => void) | null = null

async function initDatabase() {
  await initCynos()
  db = createDatabase('gql-demo-worker')

  // Create users table
  if (!db.tableNames().includes('users')) {
    const users = db
      .createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('email', JsDataType.String, null)
      .column('role', JsDataType.String, null)
      .index('idx_users_role', 'role')
    db.registerTable(users)
  }

  // Create posts table with foreign key to users
  if (!db.tableNames().includes('posts')) {
    const posts = db
      .createTable('posts')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('author_id', JsDataType.Int64, null)
      .column('title', JsDataType.String, null)
      .column('content', JsDataType.String, null)
      .column('published', JsDataType.Boolean, null)
      .foreignKey(
        'fk_posts_author',
        'author_id',
        'users',
        'id',
        new ForeignKeyOptions().fieldName('author').reverseFieldName('posts')
      )
      .index('idx_posts_author', 'author_id')
      .index('idx_posts_published', 'published')
    db.registerTable(posts)
  }

  // Create comments table with foreign key to posts
  if (!db.tableNames().includes('comments')) {
    const comments = db
      .createTable('comments')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('post_id', JsDataType.Int64, null)
      .column('author_id', JsDataType.Int64, null)
      .column('text', JsDataType.String, null)
      .foreignKey(
        'fk_comments_post',
        'post_id',
        'posts',
        'id',
        new ForeignKeyOptions().fieldName('post').reverseFieldName('comments')
      )
      .foreignKey(
        'fk_comments_author',
        'author_id',
        'users',
        'id',
        new ForeignKeyOptions().fieldName('author').reverseFieldName('comments')
      )
    db.registerTable(comments)
  }

  // Insert sample data if empty
  const userCount = await db.select('id').from('users').exec()
  if (userCount.length === 0) {
    await db.insert('users').values([
      { id: 1, name: 'Alice Chen', email: 'alice@example.com', role: 'admin' },
      { id: 2, name: 'Bob Smith', email: 'bob@example.com', role: 'editor' },
      { id: 3, name: 'Carol White', email: 'carol@example.com', role: 'user' },
    ]).exec()

    await db.insert('posts').values([
      { id: 1, author_id: 1, title: 'Getting Started with Cynos', content: 'Cynos is a reactive database...', published: true },
      { id: 2, author_id: 1, title: 'GraphQL Integration', content: 'Learn how to use GraphQL with Cynos...', published: true },
      { id: 3, author_id: 2, title: 'Performance Tips', content: 'Optimize your queries with IVM...', published: false },
    ]).exec()

    await db.insert('comments').values([
      { id: 1, post_id: 1, author_id: 2, text: 'Great introduction!' },
      { id: 2, post_id: 1, author_id: 3, text: 'Very helpful, thanks!' },
      { id: 3, post_id: 2, author_id: 3, text: 'Looking forward to more!' },
    ]).exec()
  }

  const users = await db.select('id').from('users').exec()
  const posts = await db.select('id').from('posts').exec()

  postToMain({ type: 'ready', userCount: users.length, postCount: posts.length })
}

function getSchema() {
  if (!db) return
  const sdl = db.graphqlSchema()
  postToMain({ type: 'schema', sdl })
}

function executeQuery(query: string, variables?: Record<string, unknown>) {
  if (!db) return

  const start = performance.now()
  try {
    const result = db.graphql(query, variables)
    const execTime = performance.now() - start
    postToMain({ type: 'result', data: result, execTime })

    // Send updated counts after mutations
    if (query.includes('mutation')) {
      sendCounts()
    }
  } catch (err) {
    postToMain({ type: 'error', message: String(err) })
  }
}

function subscribeToQuery(query: string) {
  if (!db) return

  // Cleanup previous subscription
  if (currentUnsubscribe) {
    currentUnsubscribe()
    currentUnsubscribe = null
  }
  if (currentSubscription) {
    currentSubscription = null
  }

  try {
    currentSubscription = db.subscribeGraphql(query)

    // Send initial result
    const initialResult = currentSubscription.getResult()
    postToMain({ type: 'subscriptionData', data: initialResult })

    // Subscribe to changes
    currentUnsubscribe = currentSubscription.subscribe((payload: unknown) => {
      postToMain({ type: 'subscriptionData', data: payload })
    }) as () => void
  } catch (err) {
    postToMain({ type: 'error', message: String(err) })
  }
}

function unsubscribe() {
  if (currentUnsubscribe) {
    currentUnsubscribe()
    currentUnsubscribe = null
  }
  currentSubscription = null
}

async function sendCounts() {
  if (!db) return
  const users = await db.select('id').from('users').exec()
  const posts = await db.select('id').from('posts').exec()
  postToMain({ type: 'counts', userCount: users.length, postCount: posts.length })
}

// Handle messages from main thread
self.onmessage = async (e: MessageEvent<GqlWorkerMessage>) => {
  const msg = e.data

  try {
    switch (msg.type) {
      case 'init':
        await initDatabase()
        break
      case 'getSchema':
        getSchema()
        break
      case 'execute':
        executeQuery(msg.query, msg.variables)
        break
      case 'subscribe':
        subscribeToQuery(msg.query)
        break
      case 'unsubscribe':
        unsubscribe()
        break
    }
  } catch (err) {
    postToMain({ type: 'error', message: String(err) })
  }
}
