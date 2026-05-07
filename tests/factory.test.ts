import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  gremlinDataSource,
  GremlinDataSource,
} from '../src/index.js';
import type { GremlinDataSourceConfig } from '../src/index.js';

// Mock @inferagraph/core
vi.mock('@inferagraph/core', () => {
  class DataSource {}
  return { DataSource };
});

// Mock the gremlin SDK so the factory can construct a client without
// touching the network. Wave 2b parity: storage owns SDK construction;
// the factory must build the client internally from config (no pre-built
// client required from the host).
const { mockOpen, mockClose, mockSubmit, mockClient, mockAuth } = vi.hoisted(() => {
  const mockOpen = vi.fn().mockResolvedValue(undefined);
  const mockClose = vi.fn().mockResolvedValue(undefined);
  const mockSubmit = vi.fn().mockResolvedValue({ _items: [] });
  const mockClient = vi.fn().mockImplementation(() => ({
    open: mockOpen,
    close: mockClose,
    submit: mockSubmit,
  }));
  const mockAuth = vi.fn().mockImplementation(() => ({}));
  return { mockOpen, mockClose, mockSubmit, mockClient, mockAuth };
});

vi.mock('gremlin', () => {
  return {
    default: {
      driver: {
        Client: mockClient,
        auth: { PlainTextSaslAuthenticator: mockAuth },
      },
    },
  };
});

const tinkerpopConfig: GremlinDataSourceConfig = {
  endpoint: 'wss://localhost:8182/',
};

const cosmosConfig: GremlinDataSourceConfig = {
  endpoint: 'wss://my-cosmos.gremlin.cosmos.azure.com:443/',
  key: 'my-primary-key',
  database: 'mydb',
  container: 'mygraph',
};

describe('gremlinDataSource factory', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns a DataSource-shaped instance (extends core DataSource, exposes name + lifecycle)', () => {
    const ds = gremlinDataSource(tinkerpopConfig);
    expect(ds).toBeInstanceOf(GremlinDataSource);
    expect(ds.name).toBe('gremlin');
    expect(typeof ds.connect).toBe('function');
    expect(typeof ds.disconnect).toBe('function');
    expect(typeof ds.isConnected).toBe('function');
    expect(typeof ds.getInitialView).toBe('function');
    expect(typeof ds.getNode).toBe('function');
    expect(typeof ds.getNeighbors).toBe('function');
    expect(typeof ds.findPath).toBe('function');
    expect(typeof ds.search).toBe('function');
    expect(typeof ds.filter).toBe('function');
    expect(typeof ds.getContent).toBe('function');
  });

  it('constructs the Gremlin SDK client internally from config (no pre-built client required)', async () => {
    const ds = gremlinDataSource(cosmosConfig);
    await ds.connect();
    // The factory's instance built the client itself using the SDK.
    expect(mockClient).toHaveBeenCalledOnce();
    expect(mockClient).toHaveBeenCalledWith(
      'wss://my-cosmos.gremlin.cosmos.azure.com:443/',
      expect.objectContaining({ traversalsource: 'g' }),
    );
    // Cosmos auth wired from config (database/container/key).
    expect(mockAuth).toHaveBeenCalledWith(
      '/dbs/mydb/colls/mygraph',
      'my-primary-key',
    );
    expect(ds.isConnected()).toBe(true);
  });
});

describe('GremlinDataSource class (renamed from GremlinDatasource)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('is exported under the new PascalCase name and instantiates with config (escape hatch)', async () => {
    // Direct class construction is the escape hatch for callers that
    // want to subclass or otherwise bypass the factory.
    const ds = new GremlinDataSource(tinkerpopConfig);
    expect(ds.name).toBe('gremlin');
    expect(ds.isConnected()).toBe(false);

    await ds.connect();
    expect(mockClient).toHaveBeenCalledOnce();
    expect(ds.isConnected()).toBe(true);

    await ds.disconnect();
    expect(ds.isConnected()).toBe(false);
  });

  it('is the same shape produced by the factory', () => {
    const fromFactory = gremlinDataSource(tinkerpopConfig);
    const fromClass = new GremlinDataSource(tinkerpopConfig);
    expect(fromFactory.constructor).toBe(fromClass.constructor);
  });
});
