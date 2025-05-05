import * as dotenv from 'dotenv';
import { runIncrementalSync } from './incremental-sync';
import { runFullSync } from './full-sync';
import { loadConfig } from './config';

dotenv.config();

async function main() {
  try {
    const args = process.argv.slice(2);
    const config = loadConfig(args);
    
    if (args.includes('--full-sync') || args.includes('-f')) {
      await runFullSync(config);
    } else {
      if (args.includes('--daemon') || args.includes('-d')) {
        config.sync.continuous = true;
      }
      await runIncrementalSync(config);
    }
  } catch (error) {
    console.error('Error in sync process:', error);
    process.exit(1);
  }
}

main();