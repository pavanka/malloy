/*
 * Copyright 2023 Google LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files
 * (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

import {
  Connection,
  FetchSchemaOptions,
  MalloyQueryData,
  FieldAtomicTypeDef,
  FieldTypeDef,
  NamedStructDefs,
  PersistSQLResults,
  PooledConnection,
  StreamingConnection,
  QueryRunStats,
  RunSQLOptions,
  SQLBlock,
  StructDef,
  DatabricksDialect,
  TestableConnection,
  QueryData,
} from '@malloydata/malloy';
import {DatabricksConnOptions, DatabricksExecutor} from './databricks_executor';

const structLen = 'struct'.length;
const arrayLen = 'array'.length;

export class DatabricksConnection
  implements Connection, PersistSQLResults, TestableConnection
{
  private readonly dialect = new DatabricksDialect();
  private executor: DatabricksExecutor;
  static DEFAULT_QUERY_OPTIONS: RunSQLOptions = {
    rowLimit: 10,
  };

  private schemaCache = new Map<
    string,
    | {schema: StructDef; error?: undefined; timestamp: number}
    | {error: string; schema?: undefined; timestamp: number}
  >();
  private sqlSchemaCache = new Map<
    string,
    | {
        structDef: StructDef;
        error?: undefined;
        timestamp: number;
      }
    | {error: string; structDef?: undefined; timestamp: number}
  >();

  private queryOptions?: RunSQLOptions;

  constructor(
    public readonly name: string,
    options: DatabricksConnOptions,
    queryOptions?: RunSQLOptions
  ) {
    this.executor = new DatabricksExecutor(options);
    this.queryOptions =
      queryOptions || DatabricksConnection.DEFAULT_QUERY_OPTIONS;
  }

  get dialectName(): string {
    return 'databricks';
  }

  public async estimateQueryCost(_sqlCommand: string): Promise<QueryRunStats> {
    return {};
  }

  public isPool(): this is PooledConnection {
    return false;
  }

  public canPersist(): this is PersistSQLResults {
    return true;
  }

  public canStream(): this is StreamingConnection {
    return false;
  }

  public async test(): Promise<void> {
    await this.runSQL('SELECT 1');
  }

  async close(): Promise<void> {
    await this.executor.close();
  }

  public get supportsNesting(): boolean {
    return false;
  }

  public async manifestTemporaryTable(_sqlCommand: string): Promise<string> {
    throw new Error('not implemented');
  }

  public async runSQL(
    sql: string,
    options?: RunSQLOptions
  ): Promise<MalloyQueryData> {
    const rowLimit = options?.rowLimit ?? this.queryOptions?.rowLimit;
    let rows = await this.executor.executeStatement(sql);
    if (rowLimit !== undefined && rows.length > rowLimit) {
      rows = rows.slice(0, rowLimit);
    }
    return {rows, totalRows: rows.length};
  }

  private splitDesc(desc: string): string[] {
    const res: string[] = [];
    let start = 0;
    let opened = 0;
    for (let i = 0; i < desc.length; i++) {
      if (desc[i] === '<') {
        opened += 1;
      } else if (desc[i] === '>') {
        opened -= 1;
      } else if (desc[i] === ',' && opened === 0) {
        res.push(desc.substring(start, i));
        start = i + 1;
      }
    }
    res.push(desc.substring(start));
    return res;
  }

  private malloyTypeFromDatabricksType(
    name: string,
    dtype: string
  ): FieldAtomicTypeDef | StructDef {
    let malloyType: FieldAtomicTypeDef | StructDef;

    if (dtype.startsWith('struct')) {
      malloyType = {
        name,
        type: 'struct',
        dialect: this.dialectName,
        structSource: {type: 'inline'},
        structRelationship: {type: 'inline'},
        fields: [],
      };

      const elems = this.splitDesc(
        dtype.substring(structLen + 1, dtype.length - 1)
      );
      for (const elem of elems) {
        const pos = elem.indexOf(':');
        malloyType.fields.push({
          name: elem.substring(0, pos),
          ...this.malloyTypeFromDatabricksType(
            elem.substring(0, pos),
            elem.substring(pos + 1)
          ),
        });
      }
    } else if (dtype.startsWith('array')) {
      const innerType = this.malloyTypeFromDatabricksType(
        name,
        dtype.substring(arrayLen + 1, dtype.length - 1)
      );
      if (innerType.type === 'struct') {
        malloyType = {...innerType, structSource: {type: 'nested'}};
        malloyType.structRelationship = {
          type: 'nested',
          fieldName: name,
          isArray: false,
        };
      } else {
        malloyType = {
          type: 'struct',
          name,
          dialect: this.dialectName,
          structSource: {type: 'nested'},
          structRelationship: {
            type: 'nested',
            fieldName: name,
            isArray: true,
          },
          fields: [{...innerType, name: 'value'} as FieldTypeDef],
        };
      }
    } else {
      malloyType = this.dialect.sqlTypeToMalloyType(dtype) ?? {
        type: 'unsupported',
        rawType: dtype,
      };
    }

    return malloyType;
  }

  private async populateStructDef(
    columns: QueryData,
    structDef: StructDef
  ): Promise<void> {
    for (const column of columns) {
      structDef.fields.push({
        name: column['col_name'] as string,
        ...this.malloyTypeFromDatabricksType(
          column['col_name'] as string,
          column['data_type'] as string
        ),
      });
    }
  }

  private async getTableSchema(
    tableKey: string,
    tablePath: string
  ): Promise<StructDef> {
    const structDef: StructDef = {
      type: 'struct',
      dialect: this.dialectName,
      name: tableKey,
      structSource: {type: 'table', tablePath},
      structRelationship: {
        type: 'basetable',
        connectionName: this.name,
      },
      fields: [],
    };
    const schema = await this.executor.describeTable(tablePath);
    await this.populateStructDef(schema, structDef);
    return structDef;
  }

  public async fetchSchemaForTables(
    missing: Record<string, string>,
    {refreshTimestamp}: FetchSchemaOptions
  ): Promise<{
    schemas: Record<string, StructDef>;
    errors: Record<string, string>;
  }> {
    const schemas: NamedStructDefs = {};
    const errors: {[name: string]: string} = {};

    for (const tableKey in missing) {
      let inCache = this.schemaCache.get(tableKey);
      if (
        !inCache ||
        (refreshTimestamp && refreshTimestamp > inCache.timestamp)
      ) {
        const tablePath = missing[tableKey];
        const timestamp = refreshTimestamp || Date.now();
        try {
          inCache = {
            schema: await this.getTableSchema(tableKey, tablePath),
            timestamp,
          };
          this.schemaCache.set(tableKey, inCache);
        } catch (error) {
          inCache = {error: error.message, timestamp};
        }
      }
      if (inCache.schema !== undefined) {
        schemas[tableKey] = inCache.schema;
      } else {
        errors[tableKey] = inCache.error || 'Unknown schema fetch error';
      }
    }
    return {schemas, errors};
  }

  private async getSQLBlockSchema(sqlRef: SQLBlock): Promise<StructDef> {
    const structDef: StructDef = {
      type: 'struct',
      dialect: this.dialectName,
      name: sqlRef.name,
      structSource: {
        type: 'sql',
        method: 'subquery',
        sqlBlock: sqlRef,
      },
      structRelationship: {
        type: 'basetable',
        connectionName: this.name,
      },
      fields: [],
    };

    const schema = await this.executor.describeStatement(sqlRef.selectStr);
    await this.populateStructDef(schema, structDef);
    return structDef;
  }

  public async fetchSchemaForSQLBlock(
    sqlRef: SQLBlock,
    {refreshTimestamp}: FetchSchemaOptions
  ): Promise<
    | {structDef: StructDef; error?: undefined}
    | {error: string; structDef?: undefined}
  > {
    const key = sqlRef.name;
    let inCache = this.sqlSchemaCache.get(key);
    if (
      !inCache ||
      (refreshTimestamp && refreshTimestamp > inCache.timestamp)
    ) {
      const timestamp = refreshTimestamp ?? Date.now();
      try {
        inCache = {
          structDef: await this.getSQLBlockSchema(sqlRef),
          timestamp,
        };
      } catch (error) {
        inCache = {error: error.message, timestamp};
      }
      this.sqlSchemaCache.set(key, inCache);
    }
    return inCache;
  }
}
