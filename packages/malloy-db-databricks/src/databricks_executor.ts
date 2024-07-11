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

import {QueryData} from '@malloydata/malloy';
import {DBSQLClient} from '@databricks/sql';
import invariant from 'tiny-invariant';
import * as dotenv from 'dotenv';
import IDBSQLSession from '@databricks/sql/dist/contracts/IDBSQLSession';
import {randomUUID} from 'crypto';

export interface DatabricksConnOptions {
  server_hostname: string;
  http_path: string;
  auth_token: string;
  catalog?: string;
  schema?: string;
}

export class DatabricksExecutor {
  private dbClient: DBSQLClient;

  constructor(private options: DatabricksConnOptions) {
    this.dbClient = new DBSQLClient();
  }
  public static createOptionsFromEnv(): DatabricksConnOptions {
    dotenv.config();

    const server_hostname = process.env['DATABRICKS_SERVER_HOSTNAME'];
    const http_path = process.env['DATABRICKS_HTTP_PATH'];
    const auth_token = process.env['DATABRICKS_TOKEN'];
    const catalog = process.env['DATABRICKS_CATALOG'];
    const schema = process.env['DATABRICKS_SCHEMA'];

    invariant(server_hostname, 'DATABRICKS_SERVER_HOSTNAME is not set');
    invariant(http_path, 'DATABRICKS_HTTP_PATH is not set');
    invariant(auth_token, 'DATABRICKS_TOKEN is not set');
    return {
      server_hostname,
      http_path,
      auth_token,
      catalog,
      schema,
    };
  }

  public static createFromEnv(): DatabricksExecutor {
    return new DatabricksExecutor(DatabricksExecutor.createOptionsFromEnv());
  }

  public async close(): Promise<void> {
    await this.dbClient.close();
  }

  private async _setSessionOptions(session: IDBSQLSession): Promise<void> {
    const setProperty = async (op: string) => {
      const operation = await session.executeStatement(op);
      await operation.finished();
    };
    await setProperty('SET ansi_mode = true;');
    await setProperty("SET time zone 'UTC';");
  }

  private async _execute(sqlText: string): Promise<QueryData> {
    let result: QueryData | undefined;

    await this.dbClient
      .connect({
        token: this.options.auth_token,
        host: this.options.server_hostname,
        path: this.options.http_path,
      })
      .then(async client => {
        const session = await client.openSession({
          initialCatalog: this.options.catalog,
          initialSchema: this.options.schema,
        });

        await this._setSessionOptions(session);
        const queryOperation = await session.executeStatement(sqlText, {
          maxRows: 10_000,
        });
        await queryOperation.finished();

        result = (await queryOperation.fetchAll()) as QueryData;

        await queryOperation.close();

        await session.close();
        await client.close();
      })
      .catch(error => {
        throw error;
      });

    invariant(result, 'result is undefined');
    return result;
  }

  public async executeStatement(sqlText: string): Promise<QueryData> {
    return await this._execute(sqlText);
  }

  public async describeTable(table: string): Promise<QueryData> {
    return await this._execute(`DESC ${table}`);
  }

  public async describeStatement(sqlText: string): Promise<QueryData> {
    let result: QueryData | undefined;

    const tmp_view = `desc_view_${randomUUID()
      .replace(/-/g, '')
      .substring(0, 8)}`;

    await this.dbClient
      .connect({
        token: this.options.auth_token,
        host: this.options.server_hostname,
        path: this.options.http_path,
      })
      .then(async client => {
        const session = await client.openSession({
          initialCatalog: this.options.catalog,
          initialSchema: this.options.schema,
        });

        const makeViewOperation = await session.executeStatement(
          `CREATE OR REPLACE TEMP VIEW ${tmp_view} AS (${sqlText});`
        );
        await makeViewOperation.finished();
        await makeViewOperation.close();

        const descOperation = await session.executeStatement(
          `DESC ${tmp_view};`,
          {
            maxRows: 10_000,
          }
        );
        result = (await descOperation.fetchAll()) as QueryData;
        await descOperation.close();

        await session.close();
        await client.close();
      })
      .catch(error => {
        throw error;
      });

    invariant(result, 'result is undefined');
    return result;
  }
}

// const executor = DatabricksExecutor.createFromEnv();

// executor.executeStatement('DESCRIBE owid_covid_data').then(data => {
//   console.table(data);
// });

// executor
//   .executeStatement('SELECT * from `owid_covid_data` limit 2')
//   .then(data => {
//     console.table(data);
//   });

// // executor.executeStatement('SELECT 1/0 as tst').then(data => {
// //   console.table(data);
// // });

// // executor.describeTable('owid_covid_data').then(data => {
// //   console.table(data);
// // });

// executor
//   .describeStatement(
//     "select struct(A, B) as C from (SELECT struct(a, b) A, array(1,2) B from (select 1 as a, '2' as b))"
//   )
//   .then(data => {
//     console.table(data);
//   });
