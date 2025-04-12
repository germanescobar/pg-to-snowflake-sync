import snowflake from 'snowflake-sdk';
import { connectToPostgresClient, connectToSnowflake, executeSnowflakeQuery, formatValueForSnowflake } from './utils';
import { Queue } from './utils';
import { Config } from './types';
import { createSchemaSync } from './schema-sync';
import { LogicalReplicationService, PgoutputPlugin } from 'pg-logical-replication';
import { Client } from 'pg';

export async function runIncrementalSync(config: Config) {
  let snowflakeConn: snowflake.Connection | null = null;
  
  try {
    console.log('Starting incremental sync process...');
    const start = Date.now();

    // Connect to Snowflake
    snowflakeConn = await connectToSnowflake(config);

    // Sync data from PostgreSQL to Snowflake
    await syncData(snowflakeConn, config);

    const end = Date.now();
    console.log(`Incremental sync completed successfully in ${((end - start) / 1000).toFixed(2)} seconds`);
  } catch (error) {
    console.error('Error in replication process:', error);
  } finally {
    if (snowflakeConn) {
      snowflakeConn.destroy((err: Error | undefined) => {
        if (err) console.error('Error destroying Snowflake connection:', err);
      });
    }
  }
}

// Flag to control the queue processor
let isProcessing = true;

async function syncData(snowflakeConn: snowflake.Connection, config: Config) {
  // Queue of changes to process
  const changeQueue = new Queue<{lsn: string, log: any}>();

  try {
    // Create the function that updates the Snowflake schema
    const updateSchema = await createSchemaSync(snowflakeConn, config);

    // Start the worker that will process changes from the queue
    const worker = startWorker(changeQueue, snowflakeConn, updateSchema);

    // start the replication service and enqueue the changes
    await sync(config, changeQueue);

    // Wait for the worker to finish
    await worker;
  } catch (error) {
    console.error('Error in syncData:', error);
    throw error;
  }
}

async function sync(config: Config, changeQueue: Queue<{lsn: string, log: any}>) {
  // Capture the target LSN before we start syncing
  const targetLSN = await getCurrentLSN(config);
  const targetLSNNumeric = lsnToLong(targetLSN);

  console.log(`Syncing up to LSN: ${targetLSN}`);

  let service: LogicalReplicationService | null = null;

  try {
    // Create the replication service
    service = createReplicationService(config);

    // Using pgoutput plugin (native to PostgreSQL, recommended)
    const plugin = new PgoutputPlugin({
      protoVersion: 1,
      publicationNames: [config.postgres.publicationName]
    });

    // Promise to track completion of the sync
    const syncCompletionPromise = new Promise<void>((resolve, reject) => {
      // Track if we've reached our target LSN
      let reachedTargetLSN = false;
      
      // Set up event handlers
      service!.on('data', async (lsn: string, log: any) => {
        console.log('LSN:', lsn);
        
        const currentLSNNumeric = lsnToLong(lsn);
        
        // Add the change to the queue
        changeQueue.enqueue({ lsn, log });
        
        // Check if we've reached or exceeded our target LSN
        if (currentLSNNumeric >= targetLSNNumeric && !reachedTargetLSN) {
          reachedTargetLSN = true;
          console.log(`Reached target LSN: ${lsn}`);
          await service!.stop();
          resolve();
        }
      });
      
      service!.on('error', (error: Error) => {
        console.error('Replication error:', error);
        reject(error);
      });
    });

    // Start subscribing to changes - don't await as this will block the main thread indefinitely
    service.subscribe(plugin, config.postgres.replicationSlot);
    
    // Wait for the sync to complete
    console.log('Waiting for sync to complete')
    await syncCompletionPromise;
    console.log('Sync complete, all operations finished')
  } finally {
    isProcessing = false;
    
    console.log('Cleaning up resources')
    if (service) {
      await service.stop()
    }
  }
}

function createReplicationService(config: Config) {
  return new LogicalReplicationService({
    host: config.postgres.host,
    port: config.postgres.port,
    database: config.postgres.database,
    user: config.postgres.user,
    password: config.postgres.password,
  },
  {
    acknowledge: {
      auto: true,
      timeoutSeconds: 10
    }
  });
}

// Worker that processes changes from the queue
async function startWorker(
  queue: Queue<{lsn: string, log: any}>,
  snowflakeConn: any,
  updateSchema: (relation: any) => Promise<void>
): Promise<void> {
  while (isProcessing) {
    const item = queue.dequeue();
    
    if (item) {
      try {
        await processChanges(snowflakeConn, item.log, item.lsn, updateSchema);
      } catch (error) {
        console.error('Error processing queued change:', error);
      }
    } else {
      // If queue is empty, wait a bit before checking again
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
}

// Get the current LSN from PostgreSQL
async function getCurrentLSN(config: Config): Promise<string> {
  let client: Client | null = null;
  try {
    client = await connectToPostgresClient(config);
    const result = await client.query("SELECT pg_current_wal_lsn() as current_lsn");
    const currentLSN = result.rows[0].current_lsn;
    console.log(`Current LSN: ${currentLSN}`);
    return currentLSN;
  } finally {
    if (client) {
      await client.end();
    }
  }
}

// Convert LSN string to numeric format for comparison
function lsnToLong(lsn: string): bigint {
  const [high, low] = lsn.split('/').map(n => BigInt(parseInt(n, 16)));
  return (high << BigInt(32)) + low;
}

// Process changes and sync to Snowflake
async function processChanges(
  snowflakeConn: any, 
  log: any, 
  lsn: string, 
  updateSchema: (relation: any) => Promise<void>
): Promise<void> {
  if (!log) return;
  
  try {
    const { tag } = log;
    
    // Handle different operation types
    switch (tag) {
      case 'insert':
        console.log('Insert log', log);
        await handleInsert(snowflakeConn, log.relation, log.new);
        break;
      case 'update':
        console.log('Update log', log);
        for (const column of log.relation.columns) {
          console.log('Column', column);
        }
        await handleUpdate(snowflakeConn, log.relation, log.new);
        break;
      case 'delete':
        console.log('Delete log', log);
        await handleDelete(snowflakeConn, log.relation, log.key);
        break;
      case 'begin':
        console.log('Transaction begin');
        break;
      case 'commit':
        console.log('Transaction committed');
        break;
      case 'relation':
        console.log('Relation information received', log);
        // Use the updateSchema function to handle schema changes
        await updateSchema(log);
        break;
      default:
        console.log(`Unhandled change type: ${tag}`);
    }
  } catch (error) {
    console.error('Error processing change:', error);
    throw error; // Re-throw so the pendingOperations promise can catch it
  }
}

// Handle INSERT operations
export async function handleInsert(snowflakeConn: any, relation: any, newRow: any): Promise<void> {
  if (!relation) {
    console.error('Relation information is missing for INSERT operation');
    return;
  }
  
  const tableName = relation.name.toUpperCase();
  
  console.log('Inserting into table', tableName);
  console.log('New row data', newRow);
  
  // Check if we have valid data
  if (!newRow) {
    console.error('New row data is missing for INSERT operation');
    return;
  }
  
  // Extract column names and values
  const columnNames = Object.keys(newRow).map(col => `"${col.toUpperCase()}"`);
  const values = Object.values(newRow).map(val => formatValueForSnowflake(val));
  
  if (columnNames.length === 0 || values.length === 0) {
    console.error('Could not extract column names or values for INSERT operation');
    return;
  }
  
  try {
    // Start transaction
    await executeSnowflakeQuery(snowflakeConn, 'BEGIN');
    
    const insertSQL = `
      INSERT INTO "${tableName}" (${columnNames.join(', ')})
      SELECT ${values.join(', ')}
    `;
    
    await executeSnowflakeQuery(snowflakeConn, insertSQL);
    
    // Commit transaction
    await executeSnowflakeQuery(snowflakeConn, 'COMMIT');
    console.log(`Inserted into table ${tableName}`);
  } catch (error) {
    // Rollback on error
    await executeSnowflakeQuery(snowflakeConn, 'ROLLBACK');
    console.error(`Error inserting into ${tableName}:`, error);
    throw error; // Re-throw to handle it in the caller
  }
}

// Handle UPDATE operations
export async function handleUpdate(snowflakeConn: any, relation: any, newRow: any): Promise<void> {
  if (!relation) {
    console.error('Relation information is missing for UPDATE operation');
    return;
  }
  
  const tableName = relation.name.toUpperCase();
  
  console.log('Updating row in table', tableName);
  console.log('New row data', newRow);
  
  // Check if we have valid data
  if (!newRow) {
    console.error('New row data is missing for UPDATE operation');
    return;
  }
  
  // Extract primary key columns from relation
  if (!relation.keyColumns || relation.keyColumns.length === 0) {
    console.error('No primary key columns defined in relation');
    return;
  }
  
  try {
    // Start transaction
    await executeSnowflakeQuery(snowflakeConn, 'BEGIN');
    
    // Build WHERE clause using primary key columns
    const whereClauseParts = [];
    for (const keyColumn of relation.keyColumns) {
      if (keyColumn in newRow) {
        whereClauseParts.push(`"${keyColumn.toUpperCase()}" = ${formatValueForSnowflake(newRow[keyColumn])}`);
      } else {
        console.error(`Primary key column ${keyColumn} not found in new row data`);
        await executeSnowflakeQuery(snowflakeConn, 'ROLLBACK');
        return;
      }
    }
    
    const whereClause = whereClauseParts.join(' AND ');

    // Build SET clause for all non-key columns
    const setClauses = [];
    for (const [columnName, value] of Object.entries(newRow)) {
      // Skip primary key columns in the SET clause
      if (!relation.keyColumns.includes(columnName)) {
        setClauses.push(`"${columnName.toUpperCase()}" = ${formatValueForSnowflake(value)}`);
      }
    }
    
    const setClause = setClauses.join(', ');
    
    if (!setClause) {
      console.error('No columns to update');
      await executeSnowflakeQuery(snowflakeConn, 'ROLLBACK');
      return;
    }
    
    const updateSQL = `
      UPDATE "${tableName}"
      SET ${setClause}
      WHERE ${whereClause}
    `;
    
    await executeSnowflakeQuery(snowflakeConn, updateSQL);
    
    // Commit transaction
    await executeSnowflakeQuery(snowflakeConn, 'COMMIT');
    console.log(`Updated row in table ${tableName}`);
  } catch (error) {
    // Rollback on error
    await executeSnowflakeQuery(snowflakeConn, 'ROLLBACK');
    console.error(`Error updating ${tableName}:`, error);
    throw error; // Re-throw to handle it in the caller
  }
}

// Handle DELETE operations
export async function handleDelete(snowflakeConn: any, relation: any, key: any): Promise<void> {
  if (!relation) {
    console.error('Relation information is missing for DELETE operation');
    return;
  }
  
  const tableName = relation.name.toUpperCase();
  
  console.log('Deleting row from table', tableName);
  console.log('Delete key', key);
  
  // Check if we have valid key information
  if (!key) {
    console.error('Key information is missing for DELETE operation');
    return;
  }
  
  try {
    // Start transaction
    await executeSnowflakeQuery(snowflakeConn, 'BEGIN');
    
    // Build where clause based on key values
    const whereClause = Object.entries(key)
      .map(([columnName, value]) => `"${columnName.toUpperCase()}" = ${formatValueForSnowflake(value)}`)
      .join(' AND ');
    
    if (!whereClause) {
      console.error('Could not build WHERE clause for DELETE operation');
      await executeSnowflakeQuery(snowflakeConn, 'ROLLBACK');
      return;
    }
    
    const deleteSQL = `
      DELETE FROM "${tableName}"
      WHERE ${whereClause}
    `;
    
    await executeSnowflakeQuery(snowflakeConn, deleteSQL);
    
    // Commit transaction
    await executeSnowflakeQuery(snowflakeConn, 'COMMIT');
    console.log(`Deleted row from table ${tableName}`);
  } catch (error) {
    // Rollback on error
    await executeSnowflakeQuery(snowflakeConn, 'ROLLBACK');
    console.error(`Error deleting from ${tableName}:`, error);
    throw error; // Re-throw to handle it in the caller
  }
} 