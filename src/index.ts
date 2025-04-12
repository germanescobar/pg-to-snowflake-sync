import * as dotenv from 'dotenv';
import { runIncrementalSync } from './incremental-sync';
import { runFullSync } from './full-sync';
import { loadConfig } from './config';

dotenv.config();

async function main() {
  try {
    const args = process.argv.slice(2);
    const config = loadConfig();
    
    if (args.includes('--full-sync') || args.includes('-f')) {
      await runFullSync(config);
    } else {
      await runIncrementalSync(config);
    }
  } catch (error) {
    console.error('Error in sync process:', error);
    process.exit(1);
  }
}

main();