import { LogicalReplicationService, PgoutputPlugin } from 'pg-logical-replication';
import * as dotenv from 'dotenv';
import { Client } from 'pg';
import snowflake from 'snowflake-sdk';
import { getPostgresSchema, syncSnowflakeSchema } from './schema-sync';
import { syncData } from './data-sync';
import { connectToPostgresClient } from './utils';

dotenv.config();

const config = {
  postgres: {
    host: process.env.PG_HOST || '',
    port: parseInt(process.env.PG_PORT || '5432'),
    database: process.env.PG_DATABASE || '',
    user: process.env.PG_USER || '',
    password: process.env.PG_PASSWORD || '',
    replicationSlot: process.env.PG_REPLICATION_SLOT || 'snowflake_slot',
    publicationName: process.env.PG_PUBLICATION_NAME || 'snowflake_pub',
  },
  snowflake: {
    account: process.env.SF_ACCOUNT || '',
    username: process.env.SF_USERNAME || '',
    password: process.env.SF_PASSWORD || '',
    database: process.env.SF_DATABASE || '',
    schema: process.env.SF_SCHEMA || 'PUBLIC',
    warehouse: process.env.SF_WAREHOUSE || '',
    role: process.env.SF_ROLE || '',
  },
  sync: {
    excludedTables: (process.env.EXCLUDED_TABLES || '').split(',').filter(Boolean),
    noChangesTimeoutMs: parseInt(process.env.NO_CHANGES_TIMEOUT_MS || '30000'), // Default 30 seconds timeout
  }
}

// Get the current LSN from PostgreSQL
async function getCurrentLSN(): Promise<string> {
  const client = await connectToPostgresClient(config);
  try {
    const result = await client.query("SELECT pg_current_wal_lsn() as current_lsn");
    const currentLSN = result.rows[0].current_lsn;
    console.log(`Current LSN: ${currentLSN}`);
    return currentLSN;
  } finally {
    await client.end();
  }
}

// Get the LSN of the replication slot
async function getSlotLSN(): Promise<string | null> {
  const client = await connectToPostgresClient(config);
  try {
    const result = await client.query(
      "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = $1",
      [config.postgres.replicationSlot]
    );
    
    if (result.rows.length === 0) {
      console.log(`Replication slot ${config.postgres.replicationSlot} not found`);
      return null;
    }
    
    const slotLSN = result.rows[0].restart_lsn;
    console.log(`Slot LSN: ${slotLSN}`);
    return slotLSN;
  } finally {
    await client.end();
  }
}

// Convert LSN string to numeric format for comparison
function lsnToLong(lsn: string): bigint {
  const [high, low] = lsn.split('/').map(n => BigInt(parseInt(n, 16)));
  return (high << BigInt(32)) + low;
}

// Check if there are any changes to process
async function hasChangesToProcess(): Promise<boolean> {
  try {
    const currentLSN = await getCurrentLSN();
    const slotLSN = await getSlotLSN();
    
    if (!slotLSN) {
      console.log('Replication slot not found or has no LSN');
      return false;
    }
    
    const currentLSNNumeric = lsnToLong(currentLSN);
    const slotLSNNumeric = lsnToLong(slotLSN);
    
    const hasChanges = currentLSNNumeric > slotLSNNumeric;
    
    if (hasChanges) {
      console.log(`Changes detected: current LSN ${currentLSN} is ahead of slot LSN ${slotLSN}`);
    } else {
      console.log(`No changes detected: current LSN ${currentLSN} is not ahead of slot LSN ${slotLSN}`);
    }
    
    return hasChanges;
  } catch (error) {
    console.error('Error checking for changes:', error);
    // Default to assuming there are changes if we can't check
    return true;
  }
}

async function runSyncProcess() {
  let snowflakeConn: snowflake.Connection | null = null;
  let service: LogicalReplicationService | null = null;
  
  try {
    // Check if there are changes to process
    const hasChanges = await hasChangesToProcess();
    if (!hasChanges) {
      console.log('No changes to process. Exiting.');
      return;
    }
    
    // Capture the target LSN before we start syncing
    const targetLSN = await getCurrentLSN();
    const targetLSNNumeric = lsnToLong(targetLSN);
    
    console.log(`Starting sync process up to LSN: ${targetLSN}`);
    
    // Connect to Snowflake
    snowflakeConn = await connectToSnowflake();
    
    // Sync schema from PostgreSQL to Snowflake
    await syncSchema(snowflakeConn, config.sync.excludedTables);

    // Create the replication service
    service = new LogicalReplicationService(
      {
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
      }
    );

    // Using pgoutput plugin (native to PostgreSQL, recommended)
    const plugin = new PgoutputPlugin({
      protoVersion: 1,
      publicationNames: [config.postgres.publicationName]
    });

    // Track if we've reached our target LSN
    let reachedTargetLSN = false;
    let receivedAnyData = false;
    
    // Promise to track completion of the sync
    const syncCompletionPromise = new Promise<void>((resolve, reject) => {
      // Set up a timeout in case we don't get any changes
      const noChangesTimeout = setTimeout(() => {
        if (!receivedAnyData) {
          console.log(`No changes received after ${config.sync.noChangesTimeoutMs}ms, completing sync`);
          resolve();
        }
      }, config.sync.noChangesTimeoutMs);
      
      // Set up event handlers
      service!.on('data', async (lsn: string, log: any) => {
        // Clear the timeout since we received data
        if (!receivedAnyData) {
          clearTimeout(noChangesTimeout);
          receivedAnyData = true;
        }
        
        console.log('LSN:', lsn);
        
        const currentLSNNumeric = lsnToLong(lsn);
        
        // Process and sync changes to Snowflake
        await syncData(snowflakeConn!, log, lsn);
        
        // Check if we've reached or exceeded our target LSN
        if (currentLSNNumeric >= targetLSNNumeric && !reachedTargetLSN) {
          reachedTargetLSN = true;
          console.log(`Reached target LSN: ${lsn}, stopping sync process`);
          resolve();
        }
      });
      
      service!.on('error', (error: Error) => {
        clearTimeout(noChangesTimeout);
        console.error('Replication error:', error);
        reject(error);
      });
    });

    // Start subscribing to changes
    await service.subscribe(plugin, config.postgres.replicationSlot);
    
    // Wait for the sync to complete
    await syncCompletionPromise;
    
    console.log('Sync process completed');
    
  } catch (error) {
    console.error('Error in replication process:', error);
  } finally {
    // Clean up resources
    if (service) {
      try {
        // Use the stop() method to terminate replication
        await service.stop();
      } catch (err) {
        console.error('Error stopping replication service:', err);
      }
    }
    
    if (snowflakeConn) {
      snowflakeConn.destroy((err: Error | undefined) => {
        if (err) console.error('Error destroying Snowflake connection:', err);
      });
    }
  }
}

async function connectToSnowflake() {
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

async function syncSchema(snowflakeConn: snowflake.Connection, excludedTables: string[]) {
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

// Run the sync process once
async function run() {
  console.log('Starting PostgreSQL to Snowflake sync process');
  await runSyncProcess();
  console.log('Sync service completed');
}

run()
  .then(() => console.log("Sync process finished"))
  .catch(error => console.log(error)); 