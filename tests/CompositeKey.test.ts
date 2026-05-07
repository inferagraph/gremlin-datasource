import { describe, it, expect, vi, beforeEach } from 'vitest';
import { GremlinDataSource } from '../src/GremlinDataSource.js';
import type { GremlinDataSourceConfig } from '../src/types.js';

// Mock @inferagraph/core
vi.mock('@inferagraph/core', () => {
  class DataSource {}
  return { DataSource };
});

// Mock gremlin module
const mockOpen = vi.fn().mockResolvedValue(undefined);
const mockClose = vi.fn().mockResolvedValue(undefined);
const mockSubmit = vi.fn().mockResolvedValue({ _items: [] });

vi.mock('gremlin', () => {
  return {
    default: {
      driver: {
        Client: vi.fn().mockImplementation(() => ({
          open: mockOpen,
          close: mockClose,
          submit: mockSubmit,
        })),
        auth: {
          PlainTextSaslAuthenticator: vi.fn().mockImplementation(() => ({})),
        },
      },
    },
  };
});

const neutralConfig: GremlinDataSourceConfig = {
  endpoint: 'wss://localhost:8182/',
};

const cosmosConfig: GremlinDataSourceConfig = {
  endpoint: 'wss://my-cosmos.gremlin.cosmos.azure.com:443/',
  key: 'my-primary-key',
  database: 'mydb',
  container: 'mygraph',
  // Bible Graph upserts vertices with partitionKey == id, so the
  // composite key is [id, id]. This is purely host-supplied — the
  // library does NOT bake in any partition scheme.
  getCompositeKey: (id: string) => [id, id] as [string, string],
};

/**
 * Recursively assert that `value` is a Cosmos-Gremlin-bindings-safe value:
 * scalars (string, number, boolean) or arrays of scalars. Cosmos rejects
 * lists-of-tuples and bare tuples — see the binding-shape docs in
 * GremlinDataSource.ts.
 */
function assertScalarOrListOfScalars(value: unknown, path: string): void {
  if (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  ) {
    return;
  }
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) {
      const item = value[i];
      if (
        typeof item !== 'string' &&
        typeof item !== 'number' &&
        typeof item !== 'boolean'
      ) {
        throw new Error(
          `bindings ${path}[${i}] is not a scalar (got ${JSON.stringify(item)}); ` +
            `Cosmos Gremlin only accepts scalars or arrays of scalars in bindings`,
        );
      }
    }
    return;
  }
  throw new Error(
    `bindings ${path} is not a scalar nor an array of scalars (got ${JSON.stringify(value)})`,
  );
}

describe('GremlinDataSource composite-key option', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('passes the bare id inlined into g.V(...) when getCompositeKey is NOT configured (TinkerPop default)', async () => {
    const ds = new GremlinDataSource(neutralConfig);
    await ds.connect();
    mockSubmit.mockResolvedValueOnce({ _items: [] });

    await ds.getNode('alice');

    const [query, bindings] = mockSubmit.mock.calls[0];
    expect(query).toBe(`g.V('alice')`);
    expect(bindings ?? {}).not.toHaveProperty('id');
  });

  it('inlines the composite key tuple into g.V(...) when getCompositeKey is configured (Cosmos)', async () => {
    const ds = new GremlinDataSource(cosmosConfig);
    await ds.connect();
    mockSubmit.mockResolvedValueOnce({ _items: [] });

    await ds.getNode('alice');

    const [query, bindings] = mockSubmit.mock.calls[0];
    expect(query).toBe(`g.V(['alice', 'alice'])`);
    expect(bindings ?? {}).not.toHaveProperty('id');
  });

  it('inlines every id at multi-id sites (getNeighbors edge fetch) and keeps no id bindings', async () => {
    const ds = new GremlinDataSource(cosmosConfig);
    await ds.connect();

    // 1st submit: neighbors traversal (origin = 'alice')
    // 2nd submit: origin lookup
    // 3rd submit: edge fetch over all ids
    mockSubmit
      .mockResolvedValueOnce({
        _items: [{ id: 'bob', label: 'person', properties: {} }],
      })
      .mockResolvedValueOnce({
        _items: [{ id: 'alice', label: 'person', properties: {} }],
      })
      .mockResolvedValueOnce({ _items: [] });

    await ds.getNeighbors('alice', 1);

    // First call: g.V(<inlined>)... with composite key for 'alice' inlined
    const [traversalQuery, traversalBindings] = mockSubmit.mock.calls[0];
    expect(traversalQuery).toBe(
      `g.V(['alice', 'alice']).repeat(both().simplePath()).times(depth).dedup()`,
    );
    expect(traversalBindings).toEqual({ depth: 1 });

    // Second call: origin lookup
    const [originQuery, originBindings] = mockSubmit.mock.calls[1];
    expect(originQuery).toBe(`g.V(['alice', 'alice'])`);
    expect(originBindings ?? {}).not.toHaveProperty('nodeId');

    // Third call: edge fetch — multi-arg form with each id inlined
    const [edgeQuery, edgeBindings] = mockSubmit.mock.calls[2];
    expect(edgeQuery).toBe(
      `g.V(['alice', 'alice'], ['bob', 'bob']).bothE().dedup()`,
    );
    expect(edgeBindings ?? {}).not.toHaveProperty('ids');
  });

  it('inlines the fromId in findPath but leaves toId bare (used by has(T.id, toId))', async () => {
    const ds = new GremlinDataSource(cosmosConfig);
    await ds.connect();
    mockSubmit.mockResolvedValueOnce({ _items: [] });

    await ds.findPath('v1', 'v2');

    const [query, bindings] = mockSubmit.mock.calls[0];
    expect(query).toContain(`g.V(['v1', 'v1'])`);
    expect(query).toContain('has(T.id, toId)');
    expect(bindings).not.toHaveProperty('fromId');
    // toId stays as a scalar binding (it's compared against T.id, which
    // accepts a plain id, not a composite tuple).
    expect(bindings).toEqual({ toId: 'v2' });
  });

  it('inlines the nodeId in getContent and removes it from bindings', async () => {
    const ds = new GremlinDataSource(cosmosConfig);
    await ds.connect();
    mockSubmit.mockResolvedValueOnce({ _items: [] });

    await ds.getContent('v1');

    const [query, bindings] = mockSubmit.mock.calls[0];
    expect(query).toBe(`g.V(['v1', 'v1']).has('content')`);
    expect(bindings ?? {}).not.toHaveProperty('nodeId');
  });

  it('inlines bulk edge ids in getInitialView edge fetch', async () => {
    const ds = new GremlinDataSource(cosmosConfig);
    await ds.connect();

    mockSubmit
      .mockResolvedValueOnce({
        _items: [
          { id: 'v1', label: 'person', properties: {} },
          { id: 'v2', label: 'person', properties: {} },
        ],
      })
      .mockResolvedValueOnce({ _items: [] });

    await ds.getInitialView();

    const [edgeQuery, edgeBindings] = mockSubmit.mock.calls[1];
    expect(edgeQuery).toBe(
      `g.V(['v1', 'v1'], ['v2', 'v2']).bothE().dedup()`,
    );
    expect(edgeBindings ?? {}).not.toHaveProperty('ids');
  });

  it('escapes single quotes in inlined ids', async () => {
    const ds = new GremlinDataSource(cosmosConfig);
    await ds.connect();
    mockSubmit.mockResolvedValueOnce({ _items: [] });

    await ds.getNode("o'brien");

    const [query] = mockSubmit.mock.calls[0];
    // The Gremlin literal is g.V(['o\'brien', 'o\'brien']); in a JS
    // string that source is "g.V(['o\\'brien', 'o\\'brien'])".
    expect(query).toBe(`g.V(['o\\'brien', 'o\\'brien'])`);
  });

  it('escapes backslashes in inlined ids (neutral / non-composite)', async () => {
    // Neutral config: a single string literal, no tuple wrapping.
    // Input id contains a literal backslash. The escape doubles it.
    const ds = new GremlinDataSource(neutralConfig);
    await ds.connect();
    mockSubmit.mockResolvedValueOnce({ _items: [] });

    // JS string "a\\b" is the 3-char string a, \, b.
    await ds.getNode('a\\b');

    const [query] = mockSubmit.mock.calls[0];
    // Gremlin literal is g.V('a\\b'); in JS source that's "g.V('a\\\\b')".
    expect(query).toBe(`g.V('a\\\\b')`);
  });

  it('contract: with getCompositeKey configured, getInitialView bindings are scalar-or-list-of-scalars', async () => {
    // This is the contract test that would have caught the original 0.1.3 bug.
    // Cosmos rejects bindings whose values are tuples or arrays-of-tuples;
    // verify every value the library passes is scalar or a list of scalars.
    const ds = new GremlinDataSource(cosmosConfig);
    await ds.connect();

    mockSubmit
      .mockResolvedValueOnce({
        _items: [
          { id: 'v1', label: 'person', properties: {} },
          { id: 'v2', label: 'person', properties: {} },
        ],
      })
      .mockResolvedValueOnce({ _items: [] });

    await ds.getInitialView();

    for (let i = 0; i < mockSubmit.mock.calls.length; i++) {
      const [, bindings] = mockSubmit.mock.calls[i] as [string, Record<string, unknown> | undefined];
      if (!bindings) continue;
      for (const [key, value] of Object.entries(bindings)) {
        assertScalarOrListOfScalars(value, `call[${i}].${key}`);
      }
    }
  });
});
