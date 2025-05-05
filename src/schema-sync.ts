import { Client } from 'pg';
import { connectToPostgresClient, executeSnowflakeQuery } from './utils';
import { Config, SchemaCache, SchemaUpdateFunction, SnowflakeColumn, SnowflakeTable } from './types';
import snowflake from 'snowflake-sdk';

export async function createUpdateSchemaFn(snowflakeConn: snowflake.Connection, config: Config): Promise<SchemaUpdateFunction> {
  console.log('Loading current Snowflake schema');
  
  // Load the schema from Snowflake
  const schemaCache: SchemaCache = await loadSchemaFromSnowflake(snowflakeConn, config);
  
  // Return a function that can update schema when needed
  return async (relation: any): Promise<void> => {
    if (!relation) {
      console.error('Relation information is missing');
      return;
    }
    
    const tableName = relation.name.toLowerCase();
    
    // Check if table is in excluded tables list
    if (config.sync.excludedTables.includes(tableName)) {
      console.log(`Table ${tableName} is in excluded tables list, skipping schema sync`);
      return;
    }
    
    console.log(`Checking schema for table ${tableName}`);
    
    try {
      // Check if table exists in our cache
      let tableInfo = schemaCache.get(tableName);
      
      if (!tableInfo) {
        // Table doesn't exist, create it
        await createTableFromRelation(snowflakeConn, relation);
        
        // Update our cache with the new table
        const columnMap = new Map<string, SnowflakeColumn>();
        relation.columns.forEach((column: any) => {
          columnMap.set(column.name.toLowerCase(), {
            name: column.name.toLowerCase(),
            dataType: mapPgTypeToSnowflake(column.type),
            isNullable: !!column.nullable
          });
        });
        
        schemaCache.set(tableName, {
          columns: columnMap
        });
        
        console.log(`Created and cached schema for table ${tableName}`);
      } else {
        // Table exists, check for column changes
        await updateTableFromRelation(snowflakeConn, relation, tableInfo, schemaCache);
      }
    } catch (error) {
      console.error(`Error updating schema for table ${tableName}:`, error);
    }
  };
}

async function loadSchemaFromSnowflake(snowflakeConn: snowflake.Connection, config: Config): Promise<SchemaCache> {
  const schemaCache: SchemaCache = new Map();

  // Get all tables in the current schema
  const tablesQuery = `
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '${config.snowflake.schema.toUpperCase()}'
  `;
  
  try {
    const tables = await executeSnowflakeQuery(snowflakeConn, tablesQuery);
    
    // Load column information for all tables
    for (const table of tables) {
      const tableName = table.TABLE_NAME.toLowerCase();
      
      const columnsQuery = `
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '${config.snowflake.schema.toUpperCase()}'
        AND table_name = '${tableName.toUpperCase()}'
      `;
      
      const columns = await executeSnowflakeQuery(snowflakeConn, columnsQuery);
      
      // Create column map for this table
      const columnMap = new Map<string, SnowflakeColumn>();
      
      columns.forEach((col: any) => {
        columnMap.set(col.COLUMN_NAME.toLowerCase(), {
          name: col.COLUMN_NAME.toLowerCase(),
          dataType: col.DATA_TYPE,
          isNullable: col.IS_NULLABLE === 'YES'
        });
      });
      
      // Add table to schema cache
      schemaCache.set(tableName, { columns: columnMap });
    }
    
    console.log(`Loaded schema information for ${schemaCache.size} tables from Snowflake`);
    return schemaCache;
  } catch (error) {
    console.error('Error loading Snowflake schema:', error);
    throw error;
  }
}

// Create a table from relation information
async function createTableFromRelation(snowflakeConn: any, relation: any): Promise<void> {
  const tableName = relation.name.toUpperCase();
  console.log(`Creating table ${tableName} from relation information`);
  
  // Convert relation columns to Snowflake column definitions
  const columnDefs = relation.columns.map((column: any) => {
    const snowflakeType = mapPgTypeToSnowflake(column.type);
    return `"${column.name.toUpperCase()}" ${snowflakeType} ${column.nullable ? 'NULL' : 'NOT NULL'}`;
  });
  
  // Add primary key if available
  let primaryKeyClause = '';
  if (relation.keyColumns && relation.keyColumns.length > 0) {
    const pkColumns = relation.keyColumns.map((pk: string) => `"${pk.toUpperCase()}"`).join(', ');
    primaryKeyClause = `, PRIMARY KEY (${pkColumns})`;
  }
  
  const createTableSQL = `
    CREATE TABLE "${tableName}" (
      ${columnDefs.join(',\n      ')}
      ${primaryKeyClause}
    )
  `;
  
  await executeSnowflakeQuery(snowflakeConn, createTableSQL);
  console.log(`Created table ${tableName} in Snowflake from relation information`);
}

// Update a table from relation information
async function updateTableFromRelation(
  snowflakeConn: any, 
  relation: any, 
  tableInfo: SnowflakeTable,
  schemaCache: SchemaCache
): Promise<void> {
  const tableName = relation.name.toLowerCase();
  console.log(`Checking for schema updates for table ${tableName}`);
  
  // Find columns to add
  const columnChanges = [];
  
  for (const pgCol of relation.columns) {
    const colName = pgCol.name.toLowerCase();
    const sfCol = tableInfo.columns.get(colName);
    
    if (!sfCol) {
      // Column doesn't exist in Snowflake, add it
      columnChanges.push({
        type: 'add',
        name: colName,
        dataType: mapPgTypeToSnowflake(pgCol.type),
        nullable: !!pgCol.nullable
      });
      
      // Update our cache
      tableInfo.columns.set(colName, {
        name: colName,
        dataType: mapPgTypeToSnowflake(pgCol.type),
        isNullable: !!pgCol.nullable
      });
    }
    // Note: we're not checking for column type changes as that would require more complex migration
  }
  
  // Apply column changes if any
  if (columnChanges.length > 0) {
    for (const change of columnChanges) {
      if (change.type === 'add') {
        const alterSQL = `
          ALTER TABLE "${tableName.toUpperCase()}" 
          ADD COLUMN "${change.name.toUpperCase()}" ${change.dataType} ${change.nullable ? 'NULL' : 'NOT NULL'}
        `;
        
        await executeSnowflakeQuery(snowflakeConn, alterSQL);
        console.log(`Added column ${change.name} to table ${tableName} in Snowflake`);
      }
    }
    
    // Update the schema cache with our changes
    schemaCache.set(tableName, tableInfo);
  } else {
    console.log(`No schema changes needed for table ${tableName}`);
  }
}

// Map PostgreSQL WAL types to Snowflake data types
function mapPgTypeToSnowflake(pgType: number): string {
  // This is a simplified mapping based on common OIDs
  // You can expand this mapping based on your specific needs
  switch (pgType) {
    case 23: // int4
      return 'NUMBER(38,0)';
    case 20: // int8
      return 'NUMBER(38,0)';
    case 21: // int2
      return 'NUMBER(38,0)';
    case 1043: // varchar
    case 25: // text
      return 'VARCHAR';
    case 1700: // numeric
      return 'NUMBER(38,9)';
    case 701: // float8
    case 700: // float4
      return 'FLOAT';
    case 1082: // date
      return 'DATE';
    case 1083: // time
      return 'TIME';
    case 1114: // timestamp
      return 'TIMESTAMP_NTZ';
    case 1184: // timestamptz
      return 'TIMESTAMP_TZ';
    case 16: // bool
      return 'BOOLEAN';
    case 114: // json
    case 3802: // jsonb
      return 'VARIANT';
    case 2950: // uuid
      return 'VARCHAR';
    default:
      console.log(`Unmapped PostgreSQL type: ${pgType}, defaulting to VARCHAR`);
      return 'VARCHAR';
  }
}

export async function syncSchema(snowflakeConn: snowflake.Connection, excludedTables: string[], config: Config) {
  let pgClient: Client | null = null;

  try {
    // Connect to PostgreSQL for schema inspection
    pgClient = await connectToPostgresClient(config);
    const schemas = await getPostgresSchema(pgClient, excludedTables);
    console.log(`Retrieved schema information for ${schemas.size} tables`);
    
    await syncSnowflakeSchema(snowflakeConn, schemas);
    console.log('Schema synchronization completed');

  } finally {
    // Close the regular client as we'll use the replication client now
    if (pgClient) {
      await pgClient.end();
    }
  }
}

// Get schema information for all tables in PostgreSQL
export async function getPostgresSchema(client: Client, excludedTables: string[]): Promise<Map<string, any>> {
  const schemas = new Map();
  
  try {
    // Get all table names
    const tableResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
    `);
    
    for (const row of tableResult.rows) {
      const tableName = row.table_name;
      
      // Skip excluded tables
      if (excludedTables.includes(tableName)) {
        console.log(`Skipping excluded table: ${tableName}`);
        continue;
      }
      
      // Get column information
      const columnResult = await client.query(`
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = $1
        ORDER BY ordinal_position
      `, [tableName]);
      
      // Get primary key information
      const pkResult = await client.query(`
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = $1::regclass
        AND i.indisprimary
      `, [`public.${tableName}`]);
      
      const primaryKeys = pkResult.rows.map(row => row.attname);
      
      schemas.set(tableName, {
        columns: columnResult.rows,
        primaryKeys
      });
    }
    
    return schemas;
  } catch (error) {
    console.error('Error getting PostgreSQL schema:', error);
    throw error;
  }
}

// Create or update tables in Snowflake based on PostgreSQL schema
export async function syncSnowflakeSchema(
  snowflakeConn: any,
  schemas: Map<string, any>
): Promise<void> {
  for (const [tableName, schema] of schemas.entries()) {
    console.log(`Syncing table schema for ${tableName}`);
    try {
      // Check if table exists in Snowflake
      const tableExistsQuery = `
        SELECT COUNT(1) as count 
        FROM information_schema.tables 
        WHERE table_name = '${tableName.toUpperCase()}'
      `;
      
      const tableExists = await executeSnowflakeQuery(snowflakeConn, tableExistsQuery);
      const tableExistsCount = tableExists[0]?.COUNT || 0;
      
      if (tableExistsCount === 0) {
        // Create new table
        await createSnowflakeTable(snowflakeConn, tableName, schema);
      } else {
        // Update existing table
        await updateSnowflakeTable(snowflakeConn, tableName, schema);
      }
    } catch (error) {
      console.error(`Error syncing schema for table ${tableName}:`, error);
      throw error;
    }
  }
}

// Create a new table in Snowflake
async function createSnowflakeTable(
  snowflakeConn: any,
  tableName: string,
  schema: any
): Promise<void> {
  const columnDefs = schema.columns.map((column: any) => {
    const snowflakeType = mapPostgresToSnowflakeType(column.data_type);
    return `"${column.column_name.toUpperCase()}" ${snowflakeType} ${column.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'}`;
  });
  
  // Add primary key if defined
  let primaryKeyClause = '';
  if (schema.primaryKeys && schema.primaryKeys.length > 0) {
    const pkColumns = schema.primaryKeys.map((pk: any) => `"${pk.toUpperCase()}"`).join(', ');
    primaryKeyClause = `, PRIMARY KEY (${pkColumns})`;
  }
  
  const createTableSQL = `
    CREATE TABLE "${tableName.toUpperCase()}" (
      ${columnDefs.join(',\n      ')}
      ${primaryKeyClause}
    )
  `;
  
  await executeSnowflakeQuery(snowflakeConn, createTableSQL);
  console.log(`Created table ${tableName} in Snowflake`);
}

// Update an existing table in Snowflake (handle schema changes)
async function updateSnowflakeTable(
  snowflakeConn: any,
  tableName: string,
  pgSchema: any
): Promise<void> {
  // Get current Snowflake table schema
  const columnQuery = `
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = '${tableName.toUpperCase()}'
  `;
  
  const existingColumns = await executeSnowflakeQuery(snowflakeConn, columnQuery);
  
  // Map of existing columns for easy lookup
  const sfColumns = new Map();
  existingColumns.forEach((col: any) => {
    sfColumns.set(col.COLUMN_NAME.toLowerCase(), {
      dataType: col.DATA_TYPE,
      isNullable: col.IS_NULLABLE
    });
  });
  
  // Find columns to add
  for (const pgCol of pgSchema.columns) {
    const colName = pgCol.column_name.toLowerCase();
    
    if (!sfColumns.has(colName)) {
      // New column - add it
      const snowflakeType = mapPostgresToSnowflakeType(pgCol.data_type);
      const alterSQL = `
        ALTER TABLE "${tableName.toUpperCase()}" 
        ADD COLUMN "${colName.toUpperCase()}" ${snowflakeType} ${pgCol.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'}
      `;
      
      await executeSnowflakeQuery(snowflakeConn, alterSQL);
      console.log(`Added column ${colName} to table ${tableName} in Snowflake`);
    }
    // Note: We're not handling type changes or nullability changes here
    // That would require more complex migration logic
  }
}

// Map PostgreSQL data types to Snowflake data types
function mapPostgresToSnowflakeType(pgType: string): string {
  const typeMap: {[key: string]: string} = {
    'integer': 'NUMBER(38,0)',
    'bigint': 'NUMBER(38,0)',
    'smallint': 'NUMBER(38,0)',
    'numeric': 'NUMBER(38,9)',
    'decimal': 'NUMBER(38,9)',
    'real': 'FLOAT',
    'double precision': 'FLOAT',
    'character varying': 'VARCHAR',
    'varchar': 'VARCHAR',
    'text': 'TEXT',
    'char': 'CHAR',
    'boolean': 'BOOLEAN',
    'date': 'DATE',
    'time': 'TIME',
    'timestamp': 'TIMESTAMP_NTZ',
    'timestamp without time zone': 'TIMESTAMP_NTZ',
    'timestamp with time zone': 'TIMESTAMP_TZ',
    'json': 'VARIANT',
    'jsonb': 'VARIANT',
    'uuid': 'VARCHAR',
  };
  
  return typeMap[pgType.toLowerCase()] || 'VARCHAR'; // Default to VARCHAR for unknown types
}
