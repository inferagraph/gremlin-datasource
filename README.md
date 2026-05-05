# @inferagraph/gremlin-datasource

Gremlin datasource for [@inferagraph/core](https://github.com/inferagraph/core).
Targets the Gremlin protocol — works against any TinkerPop-compatible server,
including Apache TinkerPop and Azure Cosmos DB Gremlin API.

## Installation

```bash
pnpm add @inferagraph/gremlin-datasource @inferagraph/core
```

## Quick start

```typescript
import { GremlinDatasource } from '@inferagraph/gremlin-datasource';

const datasource = new GremlinDatasource({
  endpoint: 'wss://your-gremlin-server:443/',
});

await datasource.connect();
const graph = await datasource.getInitialView({ limit: 50 });
await datasource.disconnect();
```

## Configuration

`GremlinDatasourceConfig` options:

| Option            | Type                                                       | Description                                                                                              |
| ----------------- | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `endpoint`        | `string`                                                   | Gremlin server WebSocket URL (`wss://host:port/`). Required.                                             |
| `key`             | `string`                                                   | Auth key. Required for Cosmos DB; omit for an unauthenticated TinkerPop server.                          |
| `database`        | `string`                                                   | Cosmos DB database id. Used in the SASL username `/dbs/<database>/colls/<container>`.                    |
| `container`       | `string`                                                   | Cosmos DB container (graph) id.                                                                          |
| `ssl`             | `boolean`                                                  | Enable TLS (rarely needed — `wss://` already enables it).                                                |
| `poolSize`        | `number`                                                   | Connection pool size hint.                                                                               |
| `getCompositeKey` | `(id: string) => string \| [string, string]`               | Map a logical vertex id to the value(s) passed to `g.V(...)`. Default: identity. See [Composite keys](#composite-keys-cosmos-db). |
| `getType`         | `(vertex: GremlinVertex) => string \| undefined`           | Resolve the semantic type of a vertex. Default: `v.label`. See [Vertex type resolution](#vertex-type-resolution). |
| `nameProperty`    | `string`                                                   | Vertex property used for `search()` and `filter({ search })`. Default: `'name'`. See [Name property](#name-property). |

### Composite keys (Cosmos DB)

Cosmos DB Gremlin containers are partitioned. Looking a vertex up by id alone
does not work — you must pass the partition value too, in the form
`g.V(['<partitionValue>', '<id>'])`.

Configure `getCompositeKey` to translate logical ids into the tuples Cosmos
needs. The library uses this everywhere a vertex is identified — `getNode`,
`getNeighbors`, `getContent`, the path traversal in `findPath`, and the bulk
edge fetches.

```typescript
// Cosmos with partitionKey == id (the simplest scheme — what Bible Graph uses):
new GremlinDatasource({
  endpoint: 'wss://my-cosmos.gremlin.cosmos.azure.com:443/',
  key: process.env.COSMOS_KEY,
  database: 'mydb',
  container: 'mygraph',
  getCompositeKey: (id) => [id, id],
});

// Cosmos with a partition value derived from the id:
new GremlinDatasource({
  // ...endpoint, key, database, container...
  getCompositeKey: (id) => [getPartitionFor(id), id],
});

// Apache TinkerPop / unpartitioned: omit getCompositeKey entirely.
new GremlinDatasource({ endpoint: 'wss://localhost:8182/' });
```

The library does **not** bake in any partition scheme. Every host-specific
mapping is supplied via this option.

### Vertex type resolution

By TinkerPop convention, the vertex label IS the type — `addV('person')`
makes the vertex a person. Some hosts use a single label for every vertex
and store the actual type in a property instead. `getType` lets such hosts
tell the library where to find the real type.

```typescript
// Default behavior — equivalent to NOT supplying getType:
new GremlinDatasource({
  endpoint,
  getType: (v) => v.label,
});

// Host stores type in a `type` property (every vertex has the same label):
new GremlinDatasource({
  endpoint,
  getType: (v) => {
    const t = v.properties?.type as Array<{ value?: unknown }> | undefined;
    return (t?.[0]?.value as string | undefined) ?? v.label;
  },
});
```

If `getType` returns `undefined`, the library falls back to `v.label`.

`GremlinVertex` is exported for typing custom resolvers:

```typescript
import type { GremlinVertex } from '@inferagraph/gremlin-datasource';
```

### Name property

`search(query)` and `filter({ search })` issue
`g.V().has('<nameProperty>', TextP.containing(query))`. By default
`<nameProperty>` is `'name'` — override when your vertices store the
searchable display name on a different property.

```typescript
// Default — searches the `name` property:
new GremlinDatasource({ endpoint });

// Host whose vertices use `title` as the display name:
new GremlinDatasource({ endpoint, nameProperty: 'title' });
```

## Usage

```typescript
await datasource.connect();

const graph     = await datasource.getInitialView({ limit: 50 });
const node      = await datasource.getNode('some-vertex-id');
const neighbors = await datasource.getNeighbors('some-vertex-id', 2);
const path      = await datasource.findPath('alice', 'bob');
const results   = await datasource.search('search term');
const filtered  = await datasource.filter({ types: ['person'] });
const content   = await datasource.getContent('some-vertex-id');

await datasource.disconnect();
```

## Cosmos DB Gremlin notes

- Cosmos containers are partitioned. Always configure `getCompositeKey`.
- The library uses the composite-key-safe traversal `g.V(<keys>).bothE().dedup()`
  for bulk edge fetches and filters dangling endpoints client-side, instead of
  the more obvious `where(otherV().hasId(within(ids)))`. The latter is
  unreliable on Cosmos when `<ids>` is a list of partition tuples.
- `findPath` terminates with `has(T.id, toId)` rather than `hasId(toId)`. On
  Cosmos, `T.id` compares the document id (the second half of the composite
  key); on TinkerPop it compares the vertex id directly.

## Compatibility

Tested against:

- Apache TinkerPop (Gremlin Server)
- Azure Cosmos DB Gremlin API

The library targets the Gremlin protocol, so it should work against any
TinkerPop-compatible server. Hosts whose vertex shape diverges from the
defaults can adapt via `getCompositeKey` and `getType`.

## License

MIT
