import { beforeAll, describe, expect, it } from 'vitest';
import init, {
  ColumnOptions,
  Database,
  ForeignKeyOptions,
  JsDataType,
} from '../wasm/cynos_database.js';

beforeAll(async () => {
  await init();
});

let dbCounter = 0;

function createGraphqlDb(): Database {
  dbCounter += 1;

  const db = new Database(`graphql_${dbCounter}`);
  const users = db
    .createTable('users')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('name', JsDataType.String, null);
  db.registerTable(users);

  const posts = db
    .createTable('posts')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('author_id', JsDataType.Int64, null)
    .column('title', JsDataType.String, null)
    .foreignKey(
      'fk_posts_author',
      'author_id',
      'users',
      'id',
      new ForeignKeyOptions().fieldName('author').reverseFieldName('posts')
    );
  db.registerTable(posts);

  return db;
}

async function flushGraphqlReactivity(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await Promise.resolve();
}

function dataOf<T>(payload: { data: T }): T {
  return payload.data;
}

describe('GraphQL', () => {
  it('exposes schema, root planner filters, and nested relations', async () => {
    const db = createGraphqlDb();

    await db.insert('users').values([
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]).exec();
    await db.insert('posts').values([
      { id: 10, author_id: 1, title: 'Hello' },
      { id: 11, author_id: 1, title: 'World' },
      { id: 12, author_id: 2, title: 'Other' },
    ]).exec();

    const sdl = db.graphqlSchema();
    expect(sdl).toContain('type Query');
    expect(sdl).toContain('type Mutation');
    expect(sdl).toContain('type Subscription');
    expect(sdl).toContain('insertUsers');
    expect(sdl).toContain('posts(');
    expect(sdl).toContain('author: Users');

    const result = db.graphql(`
      {
        users(
          where: { id: { eq: 1 } }
          orderBy: [{ field: ID, direction: ASC }]
        ) {
          id
          name
          posts(orderBy: [{ field: ID, direction: ASC }]) {
            id
            title
          }
        }
      }
    `);

    expect(dataOf(result)).toEqual({
      users: [
        {
          id: 1,
          name: 'Alice',
          posts: [
            { id: 10, title: 'Hello' },
            { id: 11, title: 'World' },
          ],
        },
      ],
    });
  });

  it('supports mutation-driven subscriptions end to end', async () => {
    const db = createGraphqlDb();
    const subscription = db.subscribeGraphql(`
      subscription {
        users(orderBy: [{ field: ID, direction: ASC }]) {
          id
          name
        }
      }
    `);

    expect(dataOf(subscription.getResult())).toEqual({ users: [] });

    const seen: Array<{ data: { users: Array<{ id: number; name: string }> } }> = [];
    const unsubscribe = subscription.subscribe((payload) => {
      seen.push(payload);
    });
    expect(subscription.subscriptionCount()).toBe(1);

    const inserted = db.graphql(`
      mutation {
        insertUsers(input: [{ id: 1, name: "Alice" }]) {
          id
          name
        }
      }
    `);
    expect(dataOf(inserted)).toEqual({
      insertUsers: [{ id: 1, name: 'Alice' }],
    });

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alice' }],
    });

    const updated = db.graphql(`
      mutation {
        updateUsers(where: { id: { eq: 1 } }, set: { name: "Alicia" }) {
          id
          name
        }
      }
    `);
    expect(dataOf(updated)).toEqual({
      updateUsers: [{ id: 1, name: 'Alicia' }],
    });

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 1, name: 'Alicia' }],
    });
    expect(seen.at(-1)).toEqual({
      data: { users: [{ id: 1, name: 'Alicia' }] },
    });

    unsubscribe();
    expect(subscription.subscriptionCount()).toBe(0);
  });

  it('supports prepared GraphQL mutation and subscription documents', async () => {
    const db = createGraphqlDb();
    const preparedSubscription = db.prepareGraphql(
      `
        subscription UserFeed {
          users(orderBy: [{ field: ID, direction: ASC }]) {
            id
            name
          }
        }
      `,
      'UserFeed'
    );
    const subscription = preparedSubscription.subscribe();

    const preparedMutation = db.prepareGraphql(
      `
        mutation AddUser($id: Long!, $name: String!) {
          insertUsers(input: [{ id: $id, name: $name }]) {
            id
            name
          }
        }
      `,
      'AddUser'
    );

    const inserted = preparedMutation.exec({ id: 2, name: 'Bob' });
    expect(dataOf(inserted)).toEqual({
      insertUsers: [{ id: 2, name: 'Bob' }],
    });

    await flushGraphqlReactivity();
    expect(dataOf(subscription.getResult())).toEqual({
      users: [{ id: 2, name: 'Bob' }],
    });
  });
});
