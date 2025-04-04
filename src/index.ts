import * as dotenv from 'dotenv';
import { runSyncProcess } from './pg-to-snowflake-sync';
import { resyncData } from './full-sync';
import { loadConfig } from './config';

dotenv.config();

async function main() {
  try {
    const args = process.argv.slice(2);
    const config = loadConfig();
    
    if (args.includes('--full-sync') || args.includes('-f')) {
      console.log('Starting full resync process...');
      await resyncData(config);
    } else {
      console.log('Starting incremental sync process...');
      await runSyncProcess();
    }
  } catch (error) {
    console.error('Error in sync process:', error);
    process.exit(1);
  }
}

main();