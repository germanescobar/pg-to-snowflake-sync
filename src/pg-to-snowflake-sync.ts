import { syncData } from './data-sync';
import snowflake from 'snowflake-sdk';
import { connectToSnowflake } from './utils';
import { loadConfig } from './config';

export async function runSyncProcess() {
  let snowflakeConn: snowflake.Connection | null = null;
  
  try {
    console.log('Starting sync process ...');

    const config = loadConfig();

    // Connect to Snowflake
    snowflakeConn = await connectToSnowflake(config);
    
    // Sync schema from PostgreSQL to Snowflake
    // await syncSchema(snowflakeConn, config.sync.excludedTables, config);

    // Sync data from PostgreSQL to Snowflake
    await syncData(snowflakeConn, config);
    
    console.log('Sync process finished');
  } catch (error) {
    console.error('Error in replication process:', error);
  } finally {
    // Add delay before destroying connection to ensure all operations complete
    if (snowflakeConn) {
      try {
        console.log('Closing Snowflake connection...');
        // Wait a short time before destroying the connection to ensure any final operations complete
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        // Destroy connection with a Promise so we can properly wait for it
        await new Promise<void>((resolve, reject) => {
          snowflakeConn!.destroy((err: Error | undefined) => {
            if (err) {
              console.error('Error destroying Snowflake connection:', err);
              reject(err);
            } else {
              console.log('Snowflake connection closed successfully');
              resolve();
            }
          });
        });
      } catch (err) {
        console.error('Failed to gracefully close Snowflake connection:', err);
      }
    }
  }
}