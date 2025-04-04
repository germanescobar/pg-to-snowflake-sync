import snowflake from 'snowflake-sdk';
import { Config } from './types';
import { Client } from 'pg';

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
export function executeSnowflakeQuery(connection: snowflake.Connection, sql: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err: any, stmt: any, rows: any[] | undefined) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows || []);
        }
      }
    });
  });
}

// Format values for Snowflake SQL
export function formatValueForSnowflake(value: any): string {
  if (value === null) {
    return 'NULL';
  } else if (typeof value === 'string') {
    return `'${value.replace(/'/g, "''")}'`;
  } else if (typeof value === 'object' && value instanceof Date) {
    return `'${value.toISOString()}'`;
  } else if (typeof value === 'object') {
    try {
      // For complex JSON objects, use a more robust approach with PARSE_JSON
      // This is safer than CAST or TO_VARIANT for complex nested structures
      const jsonString = JSON.stringify(value)
        .replace(/'/g, "''"); // Escape single quotes for SQL string literals
      
      // Use PARSE_JSON which is designed to handle complex JSON
      return `PARSE_JSON('${jsonString}')`;
    } catch (error) {
      console.error('Error stringifying JSON object:', error);
      return 'NULL';
    }
  }
  return String(value);
}