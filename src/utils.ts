import snowflake from 'snowflake-sdk';
import { Config } from './types';
import { Client } from 'pg';

export class Queue<T> {
  private items: T[] = [];

  enqueue(item: T): void {
    this.items.push(item);
  }

  dequeue(): T | undefined {
    return this.items.shift();
  }

  isEmpty(): boolean {
    return this.items.length === 0;
  }

  size(): number {
    return this.items.length;
  }

  peek(): T | undefined {
    return this.items[0];
  }
}

export async function connectToSnowflake(config: Config) {
  const snowflakeConn = snowflake.createConnection({
    account: config.snowflake.account,
    username: config.snowflake.username,
    password: config.snowflake.password,
    database: config.snowflake.database,
    schema: config.snowflake.schema,
    warehouse: config.snowflake.warehouse,
    role: config.snowflake.role
  });
  
  await new Promise<void>((resolve, reject) => {
    snowflakeConn!.connect((err: any) => err ? reject(err) : resolve());
  });
  console.log('Connected to Snowflake');
  return snowflakeConn;
}

export async function connectToPostgresClient(config: Config) {
  const client = new Client({
    host: config.postgres.host,
    port: config.postgres.port,
    database: config.postgres.database,
    user: config.postgres.user,
    password: config.postgres.password
  });
  await client.connect();
  console.log('Connected to PostgreSQL');
  return client;
}

// Helper function to execute Snowflake queries
export async function executeSnowflakeQuery(conn: any, query: string, retries = 3, delay = 1000): Promise<any> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await new Promise((resolve, reject) => {
        conn.execute({
          sqlText: query,
          complete: (err: any, stmt: any, rows: any) => {
            if (err) {
              reject(err);
            } else {
              resolve(rows || []);
            }
          }
        });
      });
    } catch (error: any) {
      // Check if it's a transaction commit error
      if (error.code === '000640' && error.sqlState === '57014') {
        if (attempt < retries) {
          console.log(`Retrying query attempt ${attempt}/${retries} after ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
      }
      throw error;
    }
  }
}

// Format values for Snowflake SQL
export function formatValueForSnowflake(value: any): string {
  if (value === null) {
    return 'NULL';
  }
  
  if (typeof value === 'string') {
    return `'${value.replace(/'/g, "''")}'`;
  }
  
  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE';
  }
  
  if (typeof value === 'number') {
    return value.toString();
  }
  
  if (value instanceof Date) {
    return `'${value.toISOString()}'`;
  }
  
  if (typeof value === 'object') {
    // For objects, stringify as JSON
    return `PARSE_JSON('${JSON.stringify(value).replace(/'/g, "''")}')`;
  }
  
  return String(value);
}

// PostgreSQL type OIDs that represent JSON types
const JSON_TYPE_OIDS = new Set([
  114,  // json
  3802, // jsonb
  199,  // json array
  3807  // jsonb array
]);

// Helper function to check if a column is a JSON type
export function isJsonColumn(relation: any, columnName: string): boolean {
  if (!relation || !relation.columns) return false;
  
  const column = relation.columns.find((col: any) => col.name === columnName);
  if (!column) return false;
  
  return JSON_TYPE_OIDS.has(column.typeId);
}