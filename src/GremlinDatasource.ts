import { DataSource } from '@inferagraph/core';
import type {
  DataAdapterConfig, GraphData, NodeId, NodeData, EdgeData,
  ContentData, PaginationOptions, PaginatedResult, DataFilter,
} from '@inferagraph/core';
import gremlin from 'gremlin';
import type { GremlinDataSourceConfig, GremlinVertex } from './types.js';

const { driver } = gremlin;

export class GremlinDataSource extends DataSource {
  readonly name = 'gremlin';
  private client: InstanceType<typeof driver.Client> | null = null;
  private config: GremlinDataSourceConfig;
  private nameProperty: string;

  constructor(config: GremlinDataSourceConfig) {
    super();
    this.config = config;
    this.nameProperty = config.nameProperty ?? 'name';
  }

  async connect(): Promise<void> {
    const authenticator = this.config.key
      ? new driver.auth.PlainTextSaslAuthenticator(
          `/dbs/${this.config.database}/colls/${this.config.container}`,
          this.config.key,
        )
      : undefined;

    this.client = new driver.Client(this.config.endpoint, {
      authenticator,
      traversalsource: 'g',
      rejectUnauthorized: true,
      mimeType: 'application/vnd.gremlin-v2.0+json',
    });

    await this.client.open();
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      await this.client.close();
      this.client = null;
    }
  }

  isConnected(): boolean {
    return this.client !== null;
  }

  async getInitialView(config?: DataAdapterConfig): Promise<GraphData> {
    this.ensureConnected();
    const limit = (config?.limit as number) ?? 100;

    // Get vertices
    const vertexResult = await this.client!.submit(`g.V().limit(${limit})`);
    const nodes = this.transformVertices(vertexResult._items || []);

    // Get edges between those vertices
    const nodeIds = nodes.map(n => n.id);
    if (nodeIds.length === 0) return { nodes: [], edges: [] };

    const edges = await this.fetchEdgesAmongNodes(nodeIds);
    return { nodes, edges };
  }

  async getNode(id: NodeId): Promise<NodeData | undefined> {
    this.ensureConnected();
    const result = await this.client!.submit(`g.V(${this.formatKey(this.resolveKey(id))})`);
    const items = result._items || [];
    if (items.length === 0) return undefined;
    return this.transformVertices(items)[0];
  }

  async getNeighbors(nodeId: NodeId, depth: number = 1): Promise<GraphData> {
    this.ensureConnected();

    const inlinedKey = this.formatKey(this.resolveKey(nodeId));

    // Get neighbors up to depth
    const vertexResult = await this.client!.submit(
      `g.V(${inlinedKey}).repeat(both().simplePath()).times(depth).dedup()`,
      { depth },
    );
    const neighborNodes = this.transformVertices(vertexResult._items || []);

    // Also get the origin node
    const originResult = await this.client!.submit(`g.V(${inlinedKey})`);
    const originNodes = this.transformVertices(originResult._items || []);

    const allNodes = [...originNodes, ...neighborNodes];
    const allNodeIds = allNodes.map(n => n.id);

    const edges = await this.fetchEdgesAmongNodes(allNodeIds);
    return { nodes: allNodes, edges };
  }

  async findPath(fromId: NodeId, toId: NodeId): Promise<GraphData> {
    this.ensureConnected();

    // Termination uses `has(T.id, toId)` rather than `hasId(toId)`. The
    // canonical T.id token compares the document id on Cosmos and the
    // vertex id on TinkerPop, and unlike `hasId()` it accepts a plain
    // id (NOT a composite-key tuple) — which is what we want here, since
    // the bound `toId` is the raw vertex id, not the partition pair.
    //
    // `fromId` is inlined as a Gremlin literal because Cosmos parameter
    // bindings reject lists of tuples; see formatKey().
    const inlinedFrom = this.formatKey(this.resolveKey(fromId));
    const result = await this.client!.submit(
      `g.V(${inlinedFrom}).repeat(both().simplePath()).until(has(T.id, toId)).limit(1).path()`,
      { toId },
    );

    const items = result._items || [];
    if (items.length === 0) return { nodes: [], edges: [] };

    // Extract path objects
    const pathObjects = items[0]?.objects || [];
    const nodes: NodeData[] = [];

    for (const obj of pathObjects) {
      if (obj.id && obj.label && !obj.inV) {
        nodes.push(this.transformVertex(obj));
      }
    }

    // Get edges between path nodes
    const nodeIds = nodes.map(n => n.id);
    const pathEdges = await this.fetchEdgesAmongNodes(nodeIds);
    return { nodes, edges: pathEdges };
  }

  async search(query: string, pagination?: PaginationOptions): Promise<PaginatedResult<NodeData>> {
    this.ensureConnected();

    const result = await this.client!.submit(
      `g.V().has('${this.nameProperty}', TextP.containing(query))`,
      { query },
    );

    const allItems = this.transformVertices(result._items || []);
    return this.paginate(allItems, pagination);
  }

  async filter(filter: DataFilter, pagination?: PaginationOptions): Promise<PaginatedResult<NodeData>> {
    this.ensureConnected();

    // Build Gremlin traversal dynamically
    let traversal = 'g.V()';
    const bindings: Record<string, unknown> = {};

    if (filter.types?.length) {
      traversal += `.has('type', within(types))`;
      bindings.types = filter.types;
    }
    if (filter.search) {
      traversal += `.has('${this.nameProperty}', TextP.containing(searchText))`;
      bindings.searchText = filter.search;
    }
    if (filter.attributes) {
      let i = 0;
      for (const [key, value] of Object.entries(filter.attributes)) {
        traversal += `.has(attrKey${i}, attrVal${i})`;
        bindings[`attrKey${i}`] = key;
        bindings[`attrVal${i}`] = value;
        i++;
      }
    }

    const result = await this.client!.submit(traversal, bindings);
    const allItems = this.transformVertices(result._items || []);
    return this.paginate(allItems, pagination);
  }

  async getContent(nodeId: NodeId): Promise<ContentData | undefined> {
    this.ensureConnected();

    const inlinedKey = this.formatKey(this.resolveKey(nodeId));
    const result = await this.client!.submit(
      `g.V(${inlinedKey}).has('content')`,
    );

    const items = result._items || [];
    if (items.length === 0) return undefined;

    const vertex = items[0];
    const content = this.getProperty(vertex, 'content');
    if (!content) return undefined;

    return {
      nodeId,
      content: String(content),
      contentType: (this.getProperty(vertex, 'contentType') as string) ?? 'text',
    };
  }

  // --- Private Helpers ---

  private ensureConnected(): void {
    if (!this.client) {
      throw new Error('GremlinDataSource is not connected. Call connect() first.');
    }
  }

  /**
   * Resolve a vertex id into the value passed to g.V(...).
   * Default: identity (TinkerPop / unpartitioned).
   * If `getCompositeKey` is configured (e.g. for Cosmos DB partitioned
   * containers), delegates to that callback.
   */
  private resolveKey(id: string): string | [string, string] {
    return this.config.getCompositeKey?.(id) ?? id;
  }

  /**
   * Format a resolved key as a Gremlin literal to inline into the query.
   *
   * Why inline rather than bind? Cosmos DB Gremlin parameter bindings only
   * accept scalars or arrays of scalars — bindings can NOT carry tuples
   * (`[pk, id]`) nor arrays of tuples. When `getCompositeKey` returns a
   * tuple, the only portable option is to inline the value into the query
   * string. We always inline (even for the scalar case) for consistency.
   *
   * String escaping: Gremlin string literals use single quotes. The
   * untrusted-input risk is small (ids come from host code) but the library
   * defends anyway: backslashes become `\\` and single quotes become `\'`.
   */
  private formatKey(key: string | [string, string]): string {
    if (Array.isArray(key)) {
      return `[${this.escapeString(key[0])}, ${this.escapeString(key[1])}]`;
    }
    return this.escapeString(key);
  }

  private escapeString(value: string): string {
    return `'${value.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`;
  }

  /**
   * Fetch edges among a known set of vertices.
   *
   * The traversal is `g.V(<inlined keys>).bothE().dedup()` — composite-key
   * safe (each key may be a tuple, inlined directly into the query) and
   * avoids `within(<list-of-tuples>)`, which is unreliable on Cosmos DB
   * Gremlin. We then drop edges with an endpoint outside the `nodeIds`
   * set client-side, since `bothE()` includes edges to neighbors we did
   * not request.
   */
  private async fetchEdgesAmongNodes(nodeIds: string[]): Promise<EdgeData[]> {
    const inlinedKeys = nodeIds
      .map(nid => this.formatKey(this.resolveKey(nid)))
      .join(', ');
    const result = await this.client!.submit(
      `g.V(${inlinedKeys}).bothE().dedup()`,
    );
    const allEdges = this.transformEdges(result._items || []);
    const idSet = new Set(nodeIds);
    return allEdges.filter(e => idSet.has(e.sourceId) && idSet.has(e.targetId));
  }

  private transformVertices(items: unknown[]): NodeData[] {
    return items.map(item => this.transformVertex(item));
  }

  private transformVertex(vertex: unknown): NodeData {
    const v = vertex as Record<string, unknown>;
    const id = String(v.id);
    const attributes: Record<string, unknown> = {};

    // Resolve the semantic type of the vertex. Default = Gremlin label
    // (TinkerPop convention). Hosts whose data stores a constant label
    // and the real type in a property (e.g. Bible Graph: every vertex
    // labeled 'Unit', actual type on a `type` property) override via
    // `getType` to surface the right value. If getType returns undefined,
    // fall back to the label.
    const resolvedType = this.config.getType?.(v as unknown as GremlinVertex) ?? (v.label as string | undefined);
    if (resolvedType !== undefined) attributes.type = resolvedType;

    // Gremlin properties can be nested objects
    const properties = v.properties as Record<string, unknown> | undefined;
    if (properties) {
      for (const [key, val] of Object.entries(properties)) {
        if (Array.isArray(val)) {
          // Multi-value property
          attributes[key] = val.length === 1
            ? (val[0] as Record<string, unknown>)?.value ?? val[0]
            : val.map((item: unknown) => (item as Record<string, unknown>)?.value ?? item);
        } else if (typeof val === 'object' && val !== null && 'value' in (val as Record<string, unknown>)) {
          attributes[key] = (val as Record<string, unknown>).value;
        } else {
          attributes[key] = val;
        }
      }
    }

    return { id, attributes };
  }

  private transformEdges(items: unknown[]): EdgeData[] {
    return items.map((edge: unknown) => {
      const e = edge as Record<string, unknown>;
      const outV = e.outV as Record<string, unknown> | string | undefined;
      const inV = e.inV as Record<string, unknown> | string | undefined;
      return {
        id: String(e.id),
        sourceId: String(typeof outV === 'object' && outV !== null ? outV.id : outV),
        targetId: String(typeof inV === 'object' && inV !== null ? inV.id : inV),
        attributes: {
          type: (e.label as string) ?? '',
          ...((e.properties as Record<string, unknown>) || {}),
        },
      };
    });
  }

  private getProperty(vertex: unknown, key: string): unknown {
    const v = vertex as Record<string, unknown>;
    const properties = v.properties as Record<string, unknown> | undefined;
    if (properties?.[key]) {
      const prop = properties[key];
      if (Array.isArray(prop)) return (prop[0] as Record<string, unknown>)?.value ?? prop[0];
      if (typeof prop === 'object' && prop !== null && 'value' in (prop as Record<string, unknown>)) return (prop as Record<string, unknown>).value;
      return prop;
    }
    return undefined;
  }

  private paginate(items: NodeData[], pagination?: PaginationOptions): PaginatedResult<NodeData> {
    const total = items.length;
    if (!pagination) return { items, total, hasMore: false };
    const { offset, limit } = pagination;
    const sliced = items.slice(offset, offset + limit);
    return { items: sliced, total, hasMore: offset + limit < total };
  }
}
